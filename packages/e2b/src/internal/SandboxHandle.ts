/**
 * Builds a {@link Sandbox} handle over E2B's control plane (lifecycle) and
 * envd (processes and files), from what `createSandbox` / `connectSandbox`
 * returned. envd calls address the sandbox with its own access token; the
 * team key is used only for control-plane calls.
 */
import { sandboxConnectionFor } from "@distilled.cloud/e2b";
import * as envd from "@distilled.cloud/e2b/envd";
import * as filesystem from "@distilled.cloud/e2b/filesystem";
import * as process from "@distilled.cloud/e2b/process";
import * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import type {
  CommandResult,
  E2BError,
  ForkResult,
  ProcessEvent,
  RunOptions,
  Sandbox,
} from "../Sandbox.ts";
import type { E2BAuth } from "./Bindings.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const toBase64 = (data: string | Uint8Array) => {
  const bytes = typeof data === "string" ? encoder.encode(data) : data;
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

const seconds = (duration: Duration.Input) =>
  Math.max(1, Math.round(Duration.toMillis(Duration.fromInputUnsafe(duration)) / 1000));

/** Collects a process's events into its output and exit code. */
const collect = (events: Stream.Stream<ProcessEvent, E2BError, never>) =>
  events.pipe(
    Stream.runFold(
      () => ({
        pid: 0,
        exitCode: -1,
        stdout: "",
        stderr: "",
        error: undefined as string | undefined,
      }),
      (acc, event) => {
        switch (event._tag) {
          case "Start":
            return { ...acc, pid: event.pid };
          case "Stdout":
          case "Pty":
            return { ...acc, stdout: acc.stdout + decoder.decode(event.data) };
          case "Stderr":
            return { ...acc, stderr: acc.stderr + decoder.decode(event.data) };
          case "End":
            return { ...acc, exitCode: event.exitCode, error: event.error };
          default:
            return acc;
        }
      },
    ),
    Effect.map((result): CommandResult => result),
  );

const shell = (command: string, options: RunOptions | undefined) => ({
  process: {
    cmd: "/bin/bash",
    args: ["-l", "-c", command],
    ...(options?.envs ? { envs: options.envs } : {}),
    ...(options?.cwd ? { cwd: options.cwd } : {}),
  },
  ...(options?.tag ? { tag: options.tag } : {}),
  ...(options?.stdin ? { stdin: true } : {}),
});

const signalOf = (signal: "SIGTERM" | "SIGKILL" | undefined) =>
  signal === "SIGTERM" ? ("SIGNAL_SIGTERM" as const) : ("SIGNAL_SIGKILL" as const);

/**
 * `allowOut` lists the only destinations a sandbox may reach: E2B allows
 * everything else unless `denyOut` blocks it, so an `allowOut` without a
 * `denyOut` denies all other traffic.
 */
export const denyUnlisted = <
  N extends { allowOut?: ReadonlyArray<string>; denyOut?: ReadonlyArray<string> },
>(
  network: N,
): N =>
  network.allowOut !== undefined && network.denyOut === undefined
    ? { ...network, denyOut: ["0.0.0.0/0"] }
    : network;

export const makeSandboxHandle = (
  auth: E2BAuth,
  info: sandboxes.Sandbox,
  domain: string,
): Sandbox => {
  const sandboxId = info.sandboxID;
  const sandboxDomain = info.domain ?? domain;
  const connection = Layer.mergeAll(
    sandboxConnectionFor({ ...info, domain: sandboxDomain }),
    FetchHttpClient.layer,
  );
  /** An envd call against this sandbox. */
  const inSandbox = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(Effect.provide(connection)) as Effect.Effect<A, E, never>;
  const streamInSandbox = <A, E, R>(stream: Stream.Stream<A, E, R>) =>
    stream.pipe(Stream.provide(connection)) as Stream.Stream<A, E, never>;
  const trafficToken =
    info.trafficAccessToken === undefined || info.trafficAccessToken === null
      ? undefined
      : Redacted.isRedacted(info.trafficAccessToken)
        ? info.trafficAccessToken
        : Redacted.make(info.trafficAccessToken);

  const handle: Sandbox = {
    sandboxId,
    templateId: info.templateID,
    domain: sandboxDomain,
    info,
    commands: {
      run: (command, options) =>
        collect(streamInSandbox(process.startProcess(shell(command, options)))),
      exec: (argv, options) =>
        collect(
          streamInSandbox(
            process.startProcess({
              process: {
                cmd: argv[0],
                args: argv.slice(1),
                ...(options?.envs ? { envs: options.envs } : {}),
                ...(options?.cwd ? { cwd: options.cwd } : {}),
              },
              ...(options?.tag ? { tag: options.tag } : {}),
              ...(options?.stdin ? { stdin: true } : {}),
            }),
          ),
        ),
      start: (command, options) => streamInSandbox(process.startProcess(shell(command, options))),
      connect: (selector) => streamInSandbox(process.connectProcess({ process: selector })),
      list: () =>
        inSandbox(process.listProcesses({})).pipe(Effect.map((res) => res.processes ?? [])),
      sendStdin: (selector, data) =>
        inSandbox(
          process.sendProcessInput({ process: selector, input: { stdin: toBase64(data) } }),
        ).pipe(Effect.asVoid),
      closeStdin: (selector) =>
        inSandbox(process.closeProcessStdin({ process: selector })).pipe(Effect.asVoid),
      kill: (selector, signal) =>
        inSandbox(process.sendProcessSignal({ process: selector, signal: signalOf(signal) })).pipe(
          Effect.asVoid,
        ),
    },
    pty: {
      create: (options) =>
        streamInSandbox(
          process.startProcess({
            process: {
              cmd: "/bin/bash",
              args: options.command ? ["-l", "-c", options.command] : ["-i", "-l"],
              envs: { TERM: "xterm-256color", ...options.envs },
              ...(options.cwd ? { cwd: options.cwd } : {}),
            },
            pty: { size: { cols: options.cols, rows: options.rows } },
            ...(options.tag ? { tag: options.tag } : {}),
          }),
        ),
      sendInput: (selector, data) =>
        inSandbox(
          process.sendProcessInput({ process: selector, input: { pty: toBase64(data) } }),
        ).pipe(Effect.asVoid),
      resize: (selector, size) =>
        inSandbox(process.updateProcess({ process: selector, pty: { size } })).pipe(Effect.asVoid),
    },
    files: {
      read: (path) => inSandbox(envd.readFile({ path })),
      readText: (path) =>
        inSandbox(envd.readFile({ path })).pipe(Effect.map((bytes) => decoder.decode(bytes))),
      write: (path, data) =>
        inSandbox(
          envd.writeFiles({
            path,
            file: new Blob([typeof data === "string" ? data : new Uint8Array(data)]),
          }),
        ).pipe(
          Effect.map((entries) => {
            const entry = entries[0];
            return { name: entry?.name, path: entry?.path ?? path } as filesystem.EntryInfo;
          }),
        ),
      list: (path, options) =>
        inSandbox(filesystem.listDir({ path, depth: options?.depth ?? 1 })).pipe(
          Effect.map((res) => res.entries ?? []),
        ),
      stat: (path) =>
        inSandbox(filesystem.statPath({ path })).pipe(Effect.map((res) => res.entry ?? { path })),
      exists: (path) =>
        inSandbox(filesystem.statPath({ path })).pipe(
          Effect.as(true),
          Effect.catchTag("NotFound", () => Effect.succeed(false)),
        ),
      makeDir: (path) =>
        inSandbox(filesystem.makeDir({ path })).pipe(Effect.map((res) => res.entry ?? { path })),
      move: (source, destination) =>
        inSandbox(filesystem.movePath({ source, destination })).pipe(
          Effect.map((res) => res.entry ?? { path: destination }),
        ),
      remove: (path) => inSandbox(filesystem.removePath({ path })).pipe(Effect.asVoid),
      compose: (sources, destination) =>
        inSandbox(envd.composeFiles({ source_paths: [...sources], destination })).pipe(
          Effect.asVoid,
        ),
      watch: (path, options) =>
        streamInSandbox(
          filesystem.watchDir({
            path,
            recursive: options?.recursive ?? false,
            includeEntry: options?.includeEntry ?? false,
          }),
        ),
    },
    getHost: (port) => `${port}-${sandboxId}.${sandboxDomain}`,
    url: (port) => `https://${port}-${sandboxId}.${sandboxDomain}`,
    fetch: (port, request) =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        // Raw HTTP: the user's own server inside the sandbox, at the URL
        // E2B serves its ports under.
        return yield* client.execute(
          request.pipe(
            HttpClientRequest.prependUrl(`https://${port}-${sandboxId}.${sandboxDomain}`),
            trafficToken === undefined
              ? (r) => r
              : HttpClientRequest.setHeader(
                  "e2b-traffic-access-token",
                  Redacted.value(trafficToken),
                ),
          ),
        );
      }).pipe(Effect.provide(FetchHttpClient.layer)),
    describe: () => auth.authorize(sandboxes.getSandbox({ sandboxID: sandboxId })),
    pause: () =>
      auth.authorize(sandboxes.pauseSandbox({ sandboxID: sandboxId })).pipe(Effect.asVoid),
    kill: () =>
      auth.authorize(sandboxes.deleteSandbox({ sandboxID: sandboxId })).pipe(
        Effect.asVoid,
        Effect.catchTag("NotFound", () => Effect.void),
      ),
    setTimeout: (timeout) =>
      auth
        .authorize(sandboxes.setSandboxTimeout({ sandboxID: sandboxId, timeout: seconds(timeout) }))
        .pipe(Effect.asVoid),
    refresh: (duration) =>
      auth
        .authorize(
          sandboxes.refreshSandbox({
            sandboxID: sandboxId,
            ...(duration === undefined ? {} : { duration: seconds(duration) }),
          }),
        )
        .pipe(Effect.asVoid),
    updateNetwork: (network) =>
      auth
        .authorize(
          sandboxes.updateSandboxNetwork({ ...denyUnlisted(network), sandboxID: sandboxId }),
        )
        .pipe(Effect.asVoid),
    snapshot: (options) =>
      auth.authorize(sandboxes.createSandboxSnapshot({ sandboxID: sandboxId, ...options })),
    fork: (options) =>
      auth
        .authorize(
          sandboxes.forkSandbox({
            sandboxID: sandboxId,
            ...(options?.count === undefined ? {} : { count: options.count }),
            ...(options?.timeout === undefined ? {} : { timeout: seconds(options.timeout) }),
          }),
        )
        .pipe(
          Effect.map((results) =>
            results.map((result): ForkResult =>
              result.sandbox !== undefined
                ? { _tag: "Forked", sandbox: makeSandboxHandle(auth, result.sandbox, domain) }
                : {
                    _tag: "ForkFailed",
                    code: result.error?.code,
                    message: result.error?.message ?? "fork failed",
                  },
            ),
          ),
        ),
    metrics: () => auth.authorize(sandboxes.listSandboxMetrics({ sandboxID: sandboxId })),
    logs: (options) =>
      auth.authorize(sandboxes.listSandboxLogs({ ...options, sandboxID: sandboxId })),
  };
  return handle;
};
