import * as fileSystem from "@distilled.cloud/daytona/file_system";
import * as port from "@distilled.cloud/daytona/port";
import * as process from "@distilled.cloud/daytona/process";
import * as sandboxes from "@distilled.cloud/daytona/sandbox";
import * as Toolbox from "@distilled.cloud/daytona/Toolbox";
import * as Bundle from "alchemy/Bundle/Bundle";
import { findCwdForBundle, resolveMainPath } from "alchemy/Bundle/TempRoot";
import { Platform, type Main, type PlatformProps } from "alchemy/Platform";
import * as Provider from "alchemy/Provider";
import type { Resource, ResourceBinding } from "alchemy/Resource";
import {
  createHostRuntimeContext,
  type HostRuntimeContext,
  type ServerHost,
} from "alchemy/Runtime/Process";
import { Stack } from "alchemy/Stack";
import { sha256Object } from "alchemy/Util/sha256";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import type * as rolldown from "rolldown";
import { physicalName } from "./internal/Names.ts";
import { resolved, type Ref } from "./internal/Ref.ts";
import { inSandbox } from "./internal/Toolbox.ts";
import type { Providers } from "./Providers.ts";
import { syncRunState } from "./Sandbox.ts";
import type { Sandbox } from "./Sandbox.ts";

/** The sandbox a {@link Service} runs in: a `Daytona.Sandbox`. */
export interface ServiceSandboxRef {
  readonly sandboxId: string;
  readonly toolboxProxyUrl: string;
}

export interface ServiceProps extends PlatformProps {
  /**
   * Sandbox the program runs in. Its snapshot must have Node.js 20 or
   * later (Daytona's default snapshot has Node 24). Changing the sandbox
   * replaces the Service.
   */
  sandbox: Ref<ServiceSandboxRef>;
  /** Module entrypoint, bundled with rolldown and uploaded to the sandbox. */
  main: string;
  /**
   * Port the program's HTTP server listens on. Written to `PORT` and used
   * for {@link Service} `url`.
   *
   * @default 3000
   */
  port?: number;
  /**
   * Named export to load from `main`.
   *
   * @default "default"
   */
  handler?: string;
  /**
   * Extra environment variables for the process. Merged after
   * binding-injected `env`.
   */
  env?: Record<string, any>;
  /** Bundler configuration for `main`. */
  build?: Bundle.BundleConfig;
}

/** What a binding attaches to a {@link Service}: environment variables. */
export interface ServiceBinding {
  readonly env?: Record<string, any>;
}

export type Service = Resource<
  "Daytona.Service",
  ServiceProps,
  {
    /** Sandbox the program runs in. */
    sandboxId: string;
    /** Port the program listens on. */
    port: number;
    /**
     * Preview URL of `port`. It is open when the sandbox is `public`;
     * otherwise send a preview token (`getPortPreviewUrl`) in
     * `X-Daytona-Preview-Token`.
     */
    url: string;
    /** Name of the deployment: its directory and session in the sandbox. */
    unitName: string;
    /** Session the process runs in. */
    sessionId: string;
    /** The session command running the process. */
    commandId: string;
    /** Content hash of the bundled program. */
    code: { hash: string };
    /** Hash of the process environment. */
    envHash: string;
  },
  ServiceBinding,
  Providers
>;

export type ServiceServices = ServerHost;

export type ServiceShape = Main<ServiceServices>;

export type ServiceRuntimeContext = HostRuntimeContext;

/**
 * An Effect program running in a Daytona sandbox, served on the
 * sandbox's preview URL.
 *
 * Alchemy bundles `main` with rolldown, uploads it into the
 * {@link Sandbox}, and runs it with Node in a session. A redeploy
 * uploads the new bundle and restarts the process; the sandbox and its
 * URL stay the same. Several Services can share one sandbox on
 * different ports.
 *
 * The program reaches the internet as its sandbox does. Below Daytona's
 * Tier 3, sandboxes reach only an allowlist of essential services
 * (package registries, GitHub, AI APIs, Daytona's API), so bindings that
 * manage sandboxes work but toolbox calls are refused.
 *
 * The process lives as long as the sandbox runs: set the sandbox's
 * `autoStopInterval` to `0` to keep it serving. If the sandbox stopped,
 * the next deploy starts it and the program again.
 *
 * ### Hosting a Service
 * **Example:** HTTP server in a sandbox
 * ```typescript
 * const Box = Daytona.Sandbox("Box", { public: true, autoStopInterval: 0 });
 *
 * export default class Api extends Daytona.Service<Api>()(
 *   "Api",
 *   { sandbox: Box, main: import.meta.url, port: 3000 },
 *   Effect.gen(function* () {
 *     return {
 *       fetch: Effect.succeed(HttpServerResponse.json({ ok: true })),
 *     };
 *   }),
 * ) {}
 * ```
 *
 * ### Reaching a Private Service
 * **Example:** Call it with a preview token
 * ```typescript
 * // The sandbox isn't public: send its preview token.
 * const { token } = yield* sandboxes.getPortPreviewUrl({ sandboxIdOrName: api.sandboxId, port: 3000 });
 * const res = yield* client.get(api.url, { headers: { "X-Daytona-Preview-Token": token } });
 * ```
 *
 * @resource
 * @product Sandbox
 */
export const Service: Platform<Service, ServiceServices, ServiceShape, ServiceRuntimeContext> =
  Platform("Daytona.Service", {
    createRuntimeContext: createHostRuntimeContext("Daytona.Service"),
    // `{ sandbox: Box }` at module scope is an Effect. Yield it here so the
    // Sandbox is registered and `news.sandbox` resolves to its attributes.
    transformProps: (_id, props) =>
      Effect.gen(function* () {
        if (globalThis.__ALCHEMY_RUNTIME__) return props;
        const sandbox = Effect.isEffect(props.sandbox)
          ? yield* props.sandbox as Effect.Effect<ServiceSandboxRef, never, Providers>
          : props.sandbox;
        return { ...props, sandbox };
      }),
  });

/** The program didn't come up: it exited, or never listened on its port. */
export class ServiceStartFailed extends Data.TaggedError("Daytona.ServiceStartFailed")<{
  readonly message: string;
  readonly stdout: string;
  readonly stderr: string;
}> {}

const DEFAULT_PORT = 3000;
const ROOT = "/home/daytona/.alchemy/services";

/** The generated entry: a shim importing only the bootstrap and `main`. */
const makeBootstrap =
  (handler: string) =>
  (importPath: string): string =>
    `
import { bootstrap } from "@alchemy.run/daytona/Bootstrap";
import { ${handler} as entrypoint } from ${JSON.stringify(importPath)};

await bootstrap(entrypoint);
`;

const shellQuote = (value: unknown) => {
  const text = typeof value === "string" ? value : JSON.stringify(value ?? null);
  return `'${text.replaceAll("'", `'\\''`)}'`;
};

const renderEnv = (env: Record<string, unknown>) =>
  Object.entries(env)
    .filter(([, value]) => value !== undefined)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${shellQuote(value)}`)
    .join("\n") + "\n";

const bindingEnv = (bindings: ReadonlyArray<ResourceBinding<ServiceBinding>>) =>
  bindings
    .filter(
      (binding: ResourceBinding<ServiceBinding> & { action?: string }) =>
        binding.action !== "delete",
    )
    .reduce<Record<string, any>>((acc, binding) => ({ ...acc, ...binding?.data?.env }), {});

export const ServiceProvider = () =>
  Provider.effect(
    Service,
    Effect.gen(function* () {
      const stack = yield* Stack;
      const virtualEntryPlugin = yield* Bundle.virtualEntryPlugin;
      const alchemyEnv = {
        ALCHEMY_STACK_NAME: stack.name,
        ALCHEMY_STAGE: stack.stage,
        ALCHEMY_PHASE: "runtime",
        HOST: "0.0.0.0",
      };

      const bundle = Effect.fn(function* (props: ServiceProps) {
        const realMain = yield* resolveMainPath(props.main);
        const cwd = yield* findCwdForBundle(realMain);
        const plugins: rolldown.RolldownPluginOption = props.isExternal
          ? undefined
          : virtualEntryPlugin(makeBootstrap(props.handler ?? "default"));
        const output = yield* Bundle.build(
          {
            ...props.build?.input,
            input: realMain,
            cwd,
            platform: "node",
            resolve: {
              conditionNames: [...Bundle.NODE_CONDITION_NAMES],
              ...props.build?.input?.resolve,
            },
            plugins: [props.build?.input?.plugins, plugins],
          },
          {
            ...props.build?.output,
            format: "esm",
            sourcemap: props.build?.output?.sourcemap ?? false,
            minify: props.build?.output?.minify ?? false,
            entryFileNames: "index.mjs",
          },
          props.build,
        );
        const files = output.files.map((file) => ({
          path: file.path,
          content:
            typeof file.content === "string"
              ? new TextEncoder().encode(file.content)
              : file.content,
        }));
        return { files, hash: (yield* sha256Object({ bundle: output.hash })).slice(0, 16) };
      });

      const sandboxOf = (props: ServiceProps) => resolved(props.sandbox);

      /** Toolbox calls against the Service's sandbox, with the deployer's key. */
      const inBox = (sandbox: ServiceSandboxRef) =>
        inSandbox(sandbox.sandboxId, sandbox.toolboxProxyUrl);

      const isRunning = (sandbox: ServiceSandboxRef, sessionId: string, commandId: string) =>
        process.getSessionCommand({ sessionId, commandId }).pipe(
          inBox(sandbox),
          Effect.map((command) => command.exitCode === undefined),
          // A stopped or deleted sandbox, or a gone session: not running.
          Effect.catch(() => Effect.succeed(false)),
        );

      const unitNameOf = (id: string, existing?: string) =>
        existing !== undefined
          ? Effect.succeed(existing)
          : physicalName(id, 48).pipe(
              Effect.map((name) => name.toLowerCase().replaceAll(/[^a-z0-9-]/g, "-")),
            );

      const stopUnit = (sandbox: ServiceSandboxRef, sessionId: string) =>
        process.deleteSession({ sessionId }).pipe(
          inBox(sandbox),
          Effect.catchTag("SessionNotFound", () => Effect.void),
        );

      return Service.Provider.of({
        stables: ["sandboxId", "unitName", "sessionId"],
        // Deployments live inside a sandbox; deleting the sandbox removes them.
        nuke: { dependsOn: ["Daytona.Sandbox"] },
        list: () => Effect.succeed([]),
        diff: Effect.fn(function* ({ news, output }) {
          if (output === undefined) return undefined;
          // `news` may be partly unresolved: the sandbox's attributes are
          // while it is itself planned for an update (a stopped sandbox is).
          const props = news as ServiceProps;
          const sandboxId = (props.sandbox as Partial<ServiceSandboxRef> | undefined)?.sandboxId;
          if (typeof sandboxId === "string" && sandboxId !== output.sandboxId) {
            return { action: "replace" } as const;
          }
          // The bundle depends only on `main`, `handler` and `build`, which are plain.
          const { hash } = yield* bundle(props);
          if (hash !== output.code.hash) return { action: "update" } as const;
          // A sandbox restart ends the process; redeploy to bring it back.
          const live = { sandboxId: output.sandboxId, toolboxProxyUrl: "" };
          if (!(yield* isRunning(live, output.sessionId, output.commandId))) {
            return { action: "update" } as const;
          }
          return undefined;
        }),
        read: Effect.fn(function* ({ output }) {
          if (output === undefined) return undefined;
          const live = yield* sandboxes
            .getSandbox({ sandboxIdOrName: output.sandboxId })
            .pipe(Effect.catchTag("SandboxNotFound", () => Effect.undefined));
          return live === undefined ? undefined : output;
        }),
        reconcile: Effect.fn(function* ({ id, news, output, bindings, session }) {
          const sandbox = sandboxOf(news);
          const servicePort = news.port ?? DEFAULT_PORT;
          const unitName = yield* unitNameOf(id, output?.unitName);
          const sessionId = `alchemy-${unitName}`;
          const env = {
            ...bindingEnv(bindings as ResourceBinding<ServiceBinding>[]),
            ...alchemyEnv,
            PORT: String(servicePort),
            ...news.env,
          };
          const envFile = renderEnv(env);
          const envHash = (yield* sha256Object({ env: envFile })).slice(0, 16);

          // Observe: the sandbox must run for its toolbox to answer.
          const box = yield* sandboxes.getSandbox({ sandboxIdOrName: sandbox.sandboxId });
          yield* syncRunState(box, "started");

          const { files, hash } = yield* bundle(news);
          const running =
            output !== undefined &&
            output.sessionId === sessionId &&
            output.code.hash === hash &&
            output.envHash === envHash &&
            output.port === servicePort &&
            (yield* isRunning(sandbox, output.sessionId, output.commandId));

          let commandId = output?.commandId ?? "";
          if (!running) {
            const dir = `${ROOT}/${unitName}/${hash}`;
            yield* session.note(`Uploading ${id} to sandbox ${sandbox.sandboxId}...`);
            yield* Toolbox.uploadFiles({
              files: [
                // Rolldown's entry chunk is `index.mjs`; other chunks keep their names.
                ...files.map((file, i) => ({
                  path: `${dir}/${i === 0 ? "index.mjs" : file.path}`,
                  content: file.content,
                })),
                { path: `${dir}/.env`, content: envFile },
              ],
            }).pipe(inBox(sandbox));
            yield* fileSystem
              .setFilePermissions({ path: `${dir}/.env`, mode: "600" })
              .pipe(inBox(sandbox));

            // Replace the running process: one session per deployment.
            yield* stopUnit(sandbox, sessionId);
            yield* process.createSession({ sessionId }).pipe(inBox(sandbox));
            const started = yield* process
              .sessionExecuteCommand({
                sessionId,
                command: `cd ${dir} && set -a && . ./.env && set +a && exec node index.mjs`,
                runAsync: true,
              })
              .pipe(inBox(sandbox));
            commandId = started.cmdId;

            // Ready once the port listens; fail with the logs if the process exits.
            yield* session.note(`Starting ${id} on port ${servicePort}...`);
            const ready = yield* Effect.gen(function* () {
              const command = yield* process
                .getSessionCommand({ sessionId, commandId })
                .pipe(inBox(sandbox));
              if (command.exitCode !== undefined) return "exited" as const;
              const { ports } = yield* port.getPorts({}).pipe(inBox(sandbox));
              return (ports ?? []).includes(servicePort)
                ? ("listening" as const)
                : ("starting" as const);
            }).pipe(
              Effect.repeat({
                schedule: Schedule.spaced("1 second"),
                until: (state) => state !== "starting",
                times: 60,
              }),
            );
            if (ready !== "listening") {
              const logs = yield* process
                .getSessionCommandLogs({ sessionId, commandId })
                .pipe(inBox(sandbox));
              return yield* new ServiceStartFailed({
                message:
                  ready === "exited"
                    ? `${id} exited before listening on port ${servicePort}`
                    : `${id} did not listen on port ${servicePort} within 60 seconds`,
                stdout: logs.stdout,
                stderr: logs.stderr,
              });
            }
            // Keep only the running deployment.
            yield* process
              .executeCommand({
                command: `find ${ROOT}/${unitName} -mindepth 1 -maxdepth 1 ! -name ${hash} -exec rm -rf {} +`,
              })
              .pipe(inBox(sandbox));
          }

          const preview = yield* sandboxes.getPortPreviewUrl({
            sandboxIdOrName: sandbox.sandboxId,
            port: servicePort,
          });
          return {
            sandboxId: sandbox.sandboxId,
            port: servicePort,
            url: preview.url,
            unitName,
            sessionId,
            commandId,
            code: { hash },
            envHash,
          };
        }),
        delete: Effect.fn(function* ({ output }) {
          const sandbox = yield* sandboxes
            .getSandbox({ sandboxIdOrName: output.sandboxId })
            .pipe(Effect.catchTag("SandboxNotFound", () => Effect.undefined));
          // Gone with its sandbox, or the sandbox isn't running: nothing runs.
          if (sandbox === undefined || sandbox.state !== "started") return;
          const ref = {
            sandboxId: output.sandboxId,
            toolboxProxyUrl: sandbox.toolboxProxyUrl ?? "",
          };
          yield* stopUnit(ref, output.sessionId);
          yield* process
            .executeCommand({ command: `rm -rf ${ROOT}/${output.unitName}` })
            .pipe(inBox(ref));
        }),
      });
    }),
  );
