import * as computerUse from "@distilled.cloud/daytona/computer_use";
import type { Credentials } from "@distilled.cloud/daytona/Credentials";
import * as fileSystem from "@distilled.cloud/daytona/file_system";
import * as git from "@distilled.cloud/daytona/git";
import * as info from "@distilled.cloud/daytona/info";
import * as interpreter from "@distilled.cloud/daytona/interpreter";
import * as lsp from "@distilled.cloud/daytona/lsp";
import * as port from "@distilled.cloud/daytona/port";
import * as process from "@distilled.cloud/daytona/process";
import * as sandboxes from "@distilled.cloud/daytona/sandbox";
import * as server from "@distilled.cloud/daytona/server";
import * as system from "@distilled.cloud/daytona/system";
import * as telemetry from "@distilled.cloud/daytona/telemetry";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Effect from "effect/Effect";
import type * as HttpClient from "effect/http/HttpClient";
import { pick, scopeModule, type ScopedModule } from "./internal/Scoped.ts";
import { inSandbox } from "./internal/Toolbox.ts";

/** Operations of the sandbox service that act on one sandbox. */
const SANDBOX_OPERATIONS = [
  "getSandbox",
  "deleteSandbox",
  "startSandbox",
  "stopSandbox",
  "pauseSandbox",
  "archiveSandbox",
  "recoverSandbox",
  "resizeSandbox",
  "replaceLabels",
  "setAutostopInterval",
  "setAutoPauseInterval",
  "setAutoArchiveInterval",
  "setAutoDeleteInterval",
  "setTtl",
  "updateNetworkSettings",
  "updatePublicStatus",
  "updateSandboxSecrets",
  "createSandboxSnapshot",
  "forkSandbox",
  "getSandboxForks",
  "getSandboxParent",
  "getSandboxAncestors",
  "getPortPreviewUrl",
  "getSignedPortPreviewUrl",
  "expireSignedPortPreviewUrl",
  "getBuildLogsUrl",
  "createSshAccess",
  "revokeSshAccess",
  "getSandboxSigningKey",
  "rotateSigningKey",
  "getSandboxLogs",
  "getSandboxTraces",
  "getSandboxTraceSpans",
  "getSandboxMetrics",
] as const;

type SandboxOperations = Pick<typeof sandboxes, (typeof SANDBOX_OPERATIONS)[number]>;

/** What `exec` returns: the exit code and combined output. */
export interface ExecResult {
  readonly exitCode: number;
  readonly result: string;
}

/** `exec` options. */
export interface ExecOptions {
  /** Working directory. */
  readonly cwd?: string;
  /** Extra environment variables. */
  readonly env?: Record<string, string>;
  /** Seconds before the command is killed. */
  readonly timeout?: number;
}

/**
 * One Daytona sandbox, as a runtime client. Every Daytona operation that
 * acts on a sandbox is a method here, with Daytona's own parameters and
 * typed errors and the sandbox filled in: lifecycle on the handle itself
 * (`startSandbox`, `forkSandbox`, `createSandboxSnapshot`,
 * `getSignedPortPreviewUrl`, …) and the in-sandbox toolbox under
 * `process`, `fs`, `git`, `lsp`, `interpreter`, `computerUse`, `ports`,
 * `info`, `system` and `server`. Telemetry from Daytona's analytics API is
 * under `telemetry`.
 *
 * Every method needs `RuntimeContext`: it runs in a deployed runtime, with
 * the API key its binding minted.
 */
export interface DaytonaSandbox extends ScopedModule<SandboxOperations> {
  /** Sandbox ID. */
  readonly sandboxId: string;
  /** Sandbox name. */
  readonly name: string;
  /** The sandbox's toolbox proxy. */
  readonly toolboxProxyUrl: string;
  /** Processes, sessions, PTYs and code runs. */
  readonly process: ScopedModule<typeof process>;
  /** Files: read, write, list, search, replace, permissions. */
  readonly fs: ScopedModule<typeof fileSystem>;
  /** Git: clone, branch, commit, push, pull, status. */
  readonly git: ScopedModule<typeof git>;
  /** Language servers: start, completions, symbols. */
  readonly lsp: ScopedModule<typeof lsp>;
  /** Stateful code interpreter contexts. */
  readonly interpreter: ScopedModule<typeof interpreter>;
  /** Desktop control: screenshots, mouse, keyboard, recordings, accessibility. */
  readonly computerUse: ScopedModule<typeof computerUse>;
  /** Listening ports. */
  readonly ports: ScopedModule<typeof port>;
  /** Home and working directory, daemon version. */
  readonly info: ScopedModule<typeof info>;
  /** System metrics. */
  readonly system: ScopedModule<typeof system>;
  /** Daemon environment. */
  readonly server: ScopedModule<typeof server>;
  /** Logs, traces and metrics the sandbox exported. */
  readonly telemetry: ScopedModule<typeof telemetry>;
  /**
   * Run a shell command and wait for it. Pipes, quotes and `&&` work as in
   * a shell; a non-zero exit is returned, not failed.
   */
  exec(
    command: string,
    options?: ExecOptions,
  ): Effect.Effect<ExecResult, process.ExecuteCommandError, RuntimeContext>;
}

/**
 * The handle {@link ConnectSandbox} returns for a sandbox declared at
 * deploy. Its methods are the same as {@link DaytonaSandbox}'s; its
 * identity resolves at runtime, so `sandboxId`, `name` and
 * `toolboxProxyUrl` are Effects.
 */
export interface ConnectedSandbox extends Omit<
  DaytonaSandbox,
  "sandboxId" | "name" | "toolboxProxyUrl"
> {
  /** Sandbox ID. */
  readonly sandboxId: Effect.Effect<string, never, RuntimeContext>;
  /** Sandbox name. */
  readonly name: Effect.Effect<string, never, RuntimeContext>;
  /** The sandbox's toolbox proxy. */
  readonly toolboxProxyUrl: Effect.Effect<string, never, RuntimeContext>;
}

/** What a binding needs to build a handle. */
export interface SandboxIdentity {
  readonly id: string;
  readonly name: string;
  readonly toolboxProxyUrl: string;
}

type Runner = <A, E>(
  effect: Effect.Effect<A, E, Credentials | HttpClient.HttpClient>,
) => Effect.Effect<A, E, RuntimeContext>;

/**
 * Build the handle for one sandbox. `run` supplies the binding's
 * credentials; toolbox calls are additionally scoped to the sandbox.
 */
export const makeSandboxClient = (
  sandbox: SandboxIdentity,
  organizationId: string,
  run: Runner,
): DaytonaSandbox => {
  const fixed = { sandboxIdOrName: sandbox.id, sandboxId: sandbox.id, organizationId };
  const toolbox: Runner = (effect) => run(inSandbox(sandbox.id, sandbox.toolboxProxyUrl)(effect));
  const exec = Effect.fn(function* (command: string, options?: ExecOptions) {
    const encoded = yield* Effect.sync(() => Buffer.from(command).toString("base64"));
    const response = yield* toolbox(
      process.executeCommand({
        command: `sh -c "echo '${encoded}' | base64 -d | sh"`,
        cwd: options?.cwd,
        envs: options?.env,
        timeout: options?.timeout,
      }),
    );
    return { exitCode: response.exitCode ?? 0, result: response.result };
  });
  return {
    ...scopeModule(pick(sandboxes, SANDBOX_OPERATIONS), fixed, run),
    sandboxId: sandbox.id,
    name: sandbox.name,
    toolboxProxyUrl: sandbox.toolboxProxyUrl,
    process: scopeModule(process, {}, toolbox),
    fs: scopeModule(fileSystem, {}, toolbox),
    git: scopeModule(git, {}, toolbox),
    lsp: scopeModule(lsp, {}, toolbox),
    interpreter: scopeModule(interpreter, {}, toolbox),
    computerUse: scopeModule(computerUse, {}, toolbox),
    ports: scopeModule(port, {}, toolbox),
    info: scopeModule(info, {}, toolbox),
    system: scopeModule(system, {}, toolbox),
    server: scopeModule(server, {}, toolbox),
    telemetry: scopeModule(telemetry, fixed, run),
    exec,
  };
};
