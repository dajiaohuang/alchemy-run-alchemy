import type * as E2B from "@distilled.cloud/e2b";
import type * as filesystem from "@distilled.cloud/e2b/filesystem";
import type * as process from "@distilled.cloud/e2b/process";
import type * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import type * as templates from "@distilled.cloud/e2b/templates";
import type * as volumes from "@distilled.cloud/e2b/volumes";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import type * as HttpClientError from "effect/http/HttpClientError";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import type * as HttpClientResponse from "effect/http/HttpClientResponse";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { E2BUnexpectedResponse, SandboxBaseMissing } from "./Errors.ts";

/** Errors an E2B call can fail with. */
export type E2BError =
  | E2B.E2BOpError
  | templates.TemplateInUse
  | volumes.VolumesNotEnabled
  | process.ConnectStreamError
  | SandboxBaseMissing
  | E2BUnexpectedResponse;

/** A process inside a sandbox, by pid or by the tag it was started with. */
export type ProcessSelector = process.ConnectProcessRequest["process"];

/** An event from a running process: `Start`, output, `End`, `KeepAlive`. */
export type ProcessEvent = process.ProcessEvent;

/** An event from a watched directory. */
export type WatchEvent = filesystem.WatchDirEvent;

/** A file or directory inside a sandbox. */
export type EntryInfo = filesystem.EntryInfo;

/** A running process inside a sandbox. */
export type ProcessInfo = process.ProcessInfo;

export interface RunOptions {
  /** Working directory. @default the sandbox user's home */
  readonly cwd?: string;
  /** Environment variables for the command. */
  readonly envs?: Readonly<Record<string, string>>;
  /** A tag to select the process by while it runs. */
  readonly tag?: string;
  /** Keep stdin open for `commands.sendStdin`. */
  readonly stdin?: boolean;
}

/** What a finished command printed, and its exit code. Non-zero exit codes are returned, not failed. */
export interface CommandResult {
  readonly pid: number;
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
  /** envd's error for a process that could not run (e.g. the executable is missing). */
  readonly error: string | undefined;
}

/** Processes inside the sandbox, through envd. */
export interface Commands {
  /** Runs a shell command line (`bash -l -c`) to completion. */
  run(
    command: string,
    options?: RunOptions,
  ): Effect.Effect<CommandResult, E2BError, RuntimeContext>;
  /** Runs an argument vector directly (no shell) to completion. */
  exec(
    argv: readonly [string, ...string[]],
    options?: RunOptions,
  ): Effect.Effect<CommandResult, E2BError, RuntimeContext>;
  /**
   * Starts a shell command line and streams its events. Interrupting the
   * stream detaches; the process keeps running until it exits or is killed.
   */
  start(
    command: string,
    options?: RunOptions,
  ): Stream.Stream<ProcessEvent, E2BError, RuntimeContext>;
  /** Re-attaches to a running process and streams its events from now on. */
  connect(process: ProcessSelector): Stream.Stream<ProcessEvent, E2BError, RuntimeContext>;
  /** Running processes. */
  list(): Effect.Effect<ReadonlyArray<ProcessInfo>, E2BError, RuntimeContext>;
  /** Writes to a process's stdin (it must have been started with `stdin: true`). */
  sendStdin(
    process: ProcessSelector,
    data: string | Uint8Array,
  ): Effect.Effect<void, E2BError, RuntimeContext>;
  /** Closes a process's stdin. */
  closeStdin(process: ProcessSelector): Effect.Effect<void, E2BError, RuntimeContext>;
  /** Signals a process. @default "SIGKILL" */
  kill(
    process: ProcessSelector,
    signal?: "SIGTERM" | "SIGKILL",
  ): Effect.Effect<void, E2BError, RuntimeContext>;
}

/** Pseudo-terminals inside the sandbox. */
export interface Pty {
  /**
   * Starts a login shell (or `command`) in a pseudo-terminal and streams its
   * events; output arrives as `Pty` events.
   */
  create(options: {
    readonly cols: number;
    readonly rows: number;
    readonly command?: string;
    readonly cwd?: string;
    readonly envs?: Readonly<Record<string, string>>;
    readonly tag?: string;
  }): Stream.Stream<ProcessEvent, E2BError, RuntimeContext>;
  /** Types into a pseudo-terminal. */
  sendInput(
    process: ProcessSelector,
    data: string | Uint8Array,
  ): Effect.Effect<void, E2BError, RuntimeContext>;
  /** Resizes a pseudo-terminal. */
  resize(
    process: ProcessSelector,
    size: { readonly cols: number; readonly rows: number },
  ): Effect.Effect<void, E2BError, RuntimeContext>;
}

/** The sandbox's filesystem, through envd. Relative paths resolve in the user's home. */
export interface Files {
  read(path: string): Effect.Effect<Uint8Array, E2BError, RuntimeContext>;
  readText(path: string): Effect.Effect<string, E2BError, RuntimeContext>;
  /** Writes a file, creating parent directories. */
  write(
    path: string,
    data: string | Uint8Array,
  ): Effect.Effect<EntryInfo, E2BError, RuntimeContext>;
  list(
    path: string,
    options?: { readonly depth?: number },
  ): Effect.Effect<ReadonlyArray<EntryInfo>, E2BError, RuntimeContext>;
  stat(path: string): Effect.Effect<EntryInfo, E2BError, RuntimeContext>;
  exists(path: string): Effect.Effect<boolean, E2BError, RuntimeContext>;
  makeDir(path: string): Effect.Effect<EntryInfo, E2BError, RuntimeContext>;
  move(source: string, destination: string): Effect.Effect<EntryInfo, E2BError, RuntimeContext>;
  remove(path: string): Effect.Effect<void, E2BError, RuntimeContext>;
  /** Concatenates files into `destination`. */
  compose(
    sources: ReadonlyArray<string>,
    destination: string,
  ): Effect.Effect<void, E2BError, RuntimeContext>;
  /** Streams changes under a directory until interrupted. */
  watch(
    path: string,
    options?: { readonly recursive?: boolean; readonly includeEntry?: boolean },
  ): Stream.Stream<WatchEvent, E2BError, RuntimeContext>;
}

/** One fork's outcome: the new sandbox, or why it could not start. */
export type ForkResult =
  | { readonly _tag: "Forked"; readonly sandbox: E2BSandbox }
  | { readonly _tag: "ForkFailed"; readonly code: number | undefined; readonly message: string };

/** Network rules for a running sandbox (`updateSandboxNetwork`), in E2B's shape. */
export type SandboxNetworkUpdate = Omit<sandboxes.UpdateSandboxNetworkRequest, "sandboxID">;

/**
 * A running (or paused) E2B sandbox: E2B's own operations on it, with no
 * client-side policy. Holds only ids and tokens, so it can be rebuilt from a
 * sandbox id with `sandboxes.connect(id)` in any request.
 */
export interface E2BSandbox {
  readonly sandboxId: string;
  readonly templateId: string;
  /** The domain the sandbox's ports are served under (`<port>-<sandboxId>.<domain>`). */
  readonly domain: string;
  /** What E2B returned for the sandbox when it was created or connected. */
  readonly info: sandboxes.Sandbox;
  readonly commands: Commands;
  readonly files: Files;
  readonly pty: Pty;
  /** The host serving a port inside the sandbox. */
  getHost(port: number): string;
  /** `https://<port>-<sandboxId>.<domain>`. */
  url(port: number): string;
  /**
   * Sends a request to a port inside the sandbox, with the sandbox's traffic
   * token when its URLs are not public.
   */
  fetch(
    port: number,
    request: HttpClientRequest.HttpClientRequest,
  ): Effect.Effect<
    HttpClientResponse.HttpClientResponse,
    HttpClientError.HttpClientError,
    RuntimeContext | Scope.Scope
  >;
  /** Live details: state, resources, network, metadata. */
  describe(): Effect.Effect<sandboxes.SandboxDetail, E2BError, RuntimeContext>;
  /** Pauses the sandbox (memory and disk are kept; `connect` resumes it). */
  pause(): Effect.Effect<void, E2BError, RuntimeContext>;
  /** Kills the sandbox. */
  kill(): Effect.Effect<void, E2BError, RuntimeContext>;
  /** Sets the time left before the sandbox is killed (or paused, with `autoPause`). */
  setTimeout(timeout: Duration.Input): Effect.Effect<void, E2BError, RuntimeContext>;
  /** Extends the sandbox's lifetime by `duration`. */
  refresh(duration?: Duration.Input): Effect.Effect<void, E2BError, RuntimeContext>;
  /** Replaces the sandbox's egress rules (E2B's `updateSandboxNetwork`). */
  updateNetwork(network: SandboxNetworkUpdate): Effect.Effect<void, E2BError, RuntimeContext>;
  /**
   * Snapshots the sandbox into a named template (memory and disk by
   * default). Sandboxes created `from` it start where this one is now.
   */
  snapshot(options?: {
    readonly name?: string;
    readonly memory?: boolean;
  }): Effect.Effect<sandboxes.SnapshotInfo, E2BError, RuntimeContext>;
  /**
   * Checkpoints the sandbox (it keeps running) and starts `count` copies of
   * it. Each fork succeeds or fails on its own.
   */
  fork(options?: {
    readonly count?: number;
    readonly timeout?: Duration.Input;
  }): Effect.Effect<ReadonlyArray<ForkResult>, E2BError, RuntimeContext>;
  /** CPU, memory and disk metrics. */
  metrics(): Effect.Effect<ReadonlyArray<sandboxes.SandboxMetric>, E2BError, RuntimeContext>;
  /** The sandbox's logs. */
  logs(
    options?: Omit<sandboxes.ListSandboxLogsRequest, "sandboxID">,
  ): Effect.Effect<sandboxes.SandboxLogsV2Response, E2BError, RuntimeContext>;
}
