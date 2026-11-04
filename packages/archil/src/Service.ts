import * as archil from "@distilled.cloud/archil";
import * as Bundle from "alchemy/Bundle/Bundle";
import { isResolved } from "alchemy/Diff";
import { createPhysicalName } from "alchemy/PhysicalName";
import { Platform, type Main, type PlatformProps } from "alchemy/Platform";
import * as Provider from "alchemy/Provider";
import { Resource, type ResourceBinding } from "alchemy/Resource";
import {
  createHostRuntimeContext,
  type HostRuntimeContext,
  type ServerHost,
} from "alchemy/Runtime/Process";
import { Stack } from "alchemy/Stack";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import { getSandbox } from "./internal/archil.ts";
import { bundleProgram, deployService, removeService } from "./internal/hosted.ts";
import type { Providers } from "./Providers.ts";
import type { Sandbox } from "./Sandbox.ts";

/** A resource-valued prop: the resource, or the Effect that declares it. */
type Ref<T> = T | Effect.Effect<T, never, Providers>;

/** What bindings attach to a Service: environment for its process. */
export interface ServiceBinding {
  env?: Record<string, any>;
}

export interface ServiceProps extends PlatformProps {
  /**
   * The sandbox the service runs in: an `Archil.Sandbox`, or the Effect
   * that declares one. A sandbox can run several services. Changing it
   * replaces the service.
   */
  sandbox: Ref<Sandbox>;
  /**
   * The program. In the Effect form, `import.meta.url` (the file is both the
   * bundle entry and the program). In the async form, a path to a module
   * whose default export has `fetch(request, env)`.
   */
  main: string;
  /**
   * TCP port the program listens on, passed to it as `PORT`. Alchemy
   * exposes it publicly and returns its URL as `url`.
   * @default 3000
   */
  port?: number;
  /** Named export to load from `main`. @default "default" */
  handler?: string;
  /**
   * Environment for the process: strings, `Redacted` secrets, and resource
   * attributes. Merged after the environment bindings add.
   */
  env?: Record<string, any>;
  /** Bundler options for `main` (rolldown `input` / `output`). */
  build?: Bundle.BundleConfig;
}

export interface ServiceAttributes {
  /** The sandbox the service runs in. */
  sandboxId: string;
  /** Archil's name for the service inside the sandbox. */
  name: string;
  /** The port the process listens on. */
  port: number;
  /** Public hostname of the port. */
  hostname: string;
  /** `https://` URL of the service. */
  url: string;
  /** Content hash of the deployed program. */
  code: { hash: string };
}

export type Service = Resource<
  "Archil.Service",
  ServiceProps,
  ServiceAttributes,
  ServiceBinding,
  Providers
>;

export type ServiceShape = Main<ServerHost>;

/**
 * A program that runs as a service in an Archil sandbox: Alchemy bundles
 * it, uploads it into the sandbox, and registers it with Archil's service
 * manager, which restarts it when it crashes and starts it whenever the
 * sandbox boots. Its port gets a public `https://` URL. A request to a
 * stopped sandbox starts it again, typically in a few seconds, so an idle
 * service costs no compute.
 *
 * Alchemy owns the service and its files under `/opt/alchemy/<name>`; the
 * sandbox (its image, disks, network policy and timeouts) is the
 * `Archil.Sandbox` you pass in, and several services can share one. The
 * program runs on the sandbox image's Node.js (`node:24-bookworm` by
 * default).
 *
 * :::caution
 * An exposed port is public: anyone with the URL can call it. Check
 * requests yourself, or hand clients an `Archil.SandboxPortToken`.
 * :::
 *
 * :::caution
 * Writes to the sandbox's root filesystem persist across stops, but a
 * replaced sandbox starts empty. Keep data on a mounted `Archil.Disk`.
 * :::
 *
 * ### Effect Services
 * **Example:** An HTTP service that reads a disk
 * ```typescript
 * // src/api.ts
 * import * as Archil from "@alchemy.run/archil";
 * import * as Effect from "effect/Effect";
 * import * as HttpServerResponse from "effect/http/HttpServerResponse";
 *
 * export const Box = Archil.Sandbox("Box", {});
 * export const Dataset = Archil.Disk("Dataset", {});
 *
 * export default class Api extends Archil.Service<Api>()(
 *   "Api",
 *   { sandbox: Box, main: import.meta.url, port: 3000 },
 *   Effect.gen(function* () {
 *     const files = yield* Archil.ReadDisk(Dataset);
 *     return {
 *       fetch: Effect.gen(function* () {
 *         const readme = yield* files.getText("README.md");
 *         return HttpServerResponse.text(readme);
 *       }),
 *     };
 *   }).pipe(Effect.provide(Archil.ReadDiskHttp)),
 * ) {}
 * ```
 *
 * ### Async Services
 * Point `main` at a module whose default export has `fetch(request, env)`.
 * The bundle contains your code only, with no Effect runtime.
 *
 * **Example:** A plain handler with typed `env`
 * ```typescript
 * // alchemy.run.ts
 * export const Box = Archil.Sandbox("Box", {});
 * export const Hello = Archil.Service("Hello", {
 *   sandbox: Box,
 *   main: "./src/hello.ts",
 *   env: { GREETING: "hello" },
 * });
 * export type HelloEnv = Archil.InferEnv<typeof Hello>;
 * ```
 *
 * ```typescript
 * // src/hello.ts
 * import type { HelloEnv } from "../alchemy.run.ts";
 *
 * export default {
 *   async fetch(request: Request, env: HelloEnv) {
 *     return new Response(`${env.GREETING} from ${new URL(request.url).pathname}`);
 *   },
 * };
 * ```
 *
 * ### Configuration
 * **Example:** A bigger sandbox that never pauses while idle
 * ```typescript
 * export const Box = Archil.Sandbox("Box", {
 *   vcpuCount: 2,
 *   memSizeMiB: 4096,
 *   idleTtlSeconds: 0,
 * });
 * ```
 *
 * **Example:** Two services in one sandbox
 * ```typescript
 * export const Web = Archil.Service("Web", { sandbox: Box, main: "./src/web.ts", port: 3000 });
 * export const Admin = Archil.Service("Admin", { sandbox: Box, main: "./src/admin.ts", port: 3001 });
 * ```
 *
 * ### Disk Bindings
 * **Example:** Write uploads to a disk from a service
 * ```typescript
 * const uploads = yield* Archil.WriteDisk(Uploads);
 * // runtime
 * yield* uploads.upload(`uploads/${id}`, request.stream);
 * ```
 *
 * ### Sandbox Bindings
 * **Example:** Run commands in a sandbox per request
 * ```typescript
 * const agents = yield* Archil.CreateSandbox(AgentImage);
 * // runtime
 * const agent = yield* agents.create({ name: `job-${id}` });
 * const result = yield* agent.exec("node --version");
 * ```
 *
 * @resource
 * @product Service
 */
export const Service: Platform<Service, ServerHost, ServiceShape, HostRuntimeContext> = Platform(
  "Archil.Service",
  {
    createRuntimeContext: createHostRuntimeContext("Archil.Service"),
    // `{ sandbox: Box }` at module scope is an Effect: yield it here so the
    // sandbox is registered and `news.sandbox` resolves to its attributes.
    transformProps: (_id, props) =>
      Effect.gen(function* () {
        if (globalThis.__ALCHEMY_RUNTIME__) return props;
        const sandbox = Effect.isEffect(props.sandbox)
          ? yield* props.sandbox as Effect.Effect<Sandbox, never, Providers>
          : props.sandbox;
        return { ...props, sandbox };
      }),
  },
);

export class ServiceSandboxUnresolved extends Data.TaggedError("Archil.ServiceSandboxUnresolved")<{
  readonly message: string;
}> {}

const DEFAULT_PORT = 3000;

const sandboxIdOf = (value: unknown): string | undefined =>
  typeof value === "object" && value !== null && "sandboxId" in value
    ? typeof value.sandboxId === "string"
      ? value.sandboxId
      : undefined
    : undefined;

const serviceName = (id: string, existing?: string) =>
  existing !== undefined
    ? Effect.succeed(existing)
    : createPhysicalName({ id, maxLength: 63, lowercase: true }).pipe(
        Effect.map((name) => name.replaceAll(/[^a-z0-9-]/g, "-").replace(/^-+|-+$/g, "")),
      );

/** The public hostname of `port`, exposing it when it isn't yet. */
const exposePort = Effect.fn(function* (sid: string, port: number) {
  const { data } = yield* archil.listSandboxPorts({ sid });
  const existing = (data.ports ?? []).find((p) => p.port === port);
  if (existing) return existing.hostname;
  const { data: exposed } = yield* archil.exposeSandboxPort({ sid, port });
  return exposed.hostname;
});

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
      return Service.Provider.of({
        stables: ["sandboxId", "name"],
        // Services live inside sandboxes, which nuke deletes.
        nuke: { dependsOn: ["Archil.Sandbox"] },
        list: () => Effect.succeed([]),
        diff: Effect.fn(function* ({ news, output }) {
          if (!isResolved(news) || output === undefined) return undefined;
          const sid = sandboxIdOf(news.sandbox);
          if (sid !== undefined && sid !== output.sandboxId) {
            return { action: "replace" } as const;
          }
          const { hash } = yield* bundleProgram(news, virtualEntryPlugin);
          if (hash !== output.code.hash || (news.port ?? DEFAULT_PORT) !== output.port) {
            return { action: "update" } as const;
          }
          return undefined;
        }),
        read: Effect.fn(function* ({ output }) {
          if (output === undefined) return undefined;
          const sandbox = yield* getSandbox(output.sandboxId);
          return sandbox === undefined ? undefined : output;
        }),
        reconcile: Effect.fn(function* ({ id, news, output, bindings }) {
          const sid = sandboxIdOf(news.sandbox);
          if (sid === undefined) {
            return yield* new ServiceSandboxUnresolved({
              message: `Archil.Service ${id} needs a resolved Archil.Sandbox`,
            });
          }
          const name = yield* serviceName(id, output?.name);
          const port = news.port ?? DEFAULT_PORT;
          const bindingEnv = (bindings as ResourceBinding<ServiceBinding>[])
            .filter(
              (b: ResourceBinding<ServiceBinding> & { action?: string }) => b.action !== "delete",
            )
            .reduce<Record<string, any>>((acc, b) => ({ ...acc, ...b.data?.env }), {});
          const program = yield* bundleProgram(news, virtualEntryPlugin);
          // A moved port leaves the old one exposed; take it down.
          if (output !== undefined && output.port !== port && output.sandboxId === sid) {
            yield* archil
              .deleteSandboxPort({ sid, port: output.port })
              .pipe(Effect.catchTag("SandboxNotFound", () => Effect.void));
          }
          yield* deployService({
            sid,
            name,
            port,
            files: program.files,
            env: { ...bindingEnv, ...alchemyEnv, ...news.env },
          });
          const hostname = yield* exposePort(sid, port);
          return {
            sandboxId: sid,
            name,
            port,
            hostname,
            url: `https://${hostname}`,
            code: { hash: program.hash },
          };
        }),
        delete: Effect.fn(function* ({ output }) {
          yield* removeService(output.sandboxId, output.name);
          yield* archil
            .deleteSandboxPort({ sid: output.sandboxId, port: output.port })
            .pipe(Effect.catchTag("SandboxNotFound", () => Effect.void));
        }),
      });
    }),
  );
