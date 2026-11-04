/**
 * Bundling and deploying `Archil.Service` programs: a Node process run as
 * an Archil sandbox service (`archil-sandbox services create`), which
 * restarts when it crashes and starts whenever the sandbox boots.
 */
import * as archil from "@distilled.cloud/archil";
import * as Process from "@distilled.cloud/archil/Process";
import * as Bundle from "alchemy/Bundle/Bundle";
import { findCwdForBundle, resolveMainPath } from "alchemy/Bundle/TempRoot";
import { sha256, sha256Object } from "alchemy/Util/sha256";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import type * as rolldown from "rolldown";
import { waitForSandbox } from "./archil.ts";

/** A service failed to deploy or never started serving. */
export class ServiceDeployFailed extends Data.TaggedError("Archil.ServiceDeployFailed")<{
  readonly service: string;
  readonly message: string;
}> {}

export interface HostedProgramProps {
  readonly main: string;
  readonly handler?: string;
  readonly isExternal?: boolean;
  readonly build?: Bundle.BundleConfig;
}

export interface BundledProgram {
  readonly files: ReadonlyArray<{ readonly path: string; readonly content: Uint8Array }>;
  readonly hash: string;
}

/** Entry for the Effect form: the package bootstrap plus the user's `main`. */
const effectEntry = (handler: string) => (importPath: string) =>
  `
import { bootstrap } from "@alchemy.run/archil/Bootstrap";
import { ${handler} as entrypoint } from ${JSON.stringify(importPath)};

await bootstrap(entrypoint);
`;

/**
 * Entry for the async form: serve the module's `fetch(request, env)` over
 * `node:http`, with `process.env` as `env`. Nothing from alchemy or Effect
 * is bundled.
 */
const asyncEntry = (handler: string) => (importPath: string) =>
  `
import * as mod from ${JSON.stringify(importPath)};
import http from "node:http";
import { Readable } from "node:stream";

const app = mod[${JSON.stringify(handler)}] ?? mod.default;
const port = Number(process.env.PORT ?? 3000);

http
  .createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", \`http://\${req.headers.host ?? "localhost"}\`);
      const headers = new Headers();
      for (const [key, value] of Object.entries(req.headers)) {
        for (const v of Array.isArray(value) ? value : value === undefined ? [] : [value]) {
          headers.append(key, v);
        }
      }
      const hasBody = req.method !== "GET" && req.method !== "HEAD";
      const request = new Request(url, {
        method: req.method,
        headers,
        body: hasBody ? Readable.toWeb(req) : undefined,
        duplex: hasBody ? "half" : undefined,
      });
      const response = await app.fetch(request, process.env);
      res.writeHead(response.status, Object.fromEntries(response.headers));
      if (response.body) Readable.fromWeb(response.body).pipe(res);
      else res.end();
    } catch (error) {
      console.error(error);
      if (!res.headersSent) res.writeHead(500);
      res.end("Internal Server Error");
    }
  })
  .listen(port, "0.0.0.0");
`;

/** Bundle `main` into a Node program whose entry is `index.mjs`. */
export const bundleProgram = Effect.fn(function* (
  props: HostedProgramProps,
  virtualEntryPlugin: (content: (importPath: string) => string) => rolldown.Plugin,
) {
  const handler = props.handler ?? "default";
  const realMain = yield* resolveMainPath(props.main);
  const cwd = yield* findCwdForBundle(realMain);
  const entry = props.isExternal ? asyncEntry(handler) : effectEntry(handler);
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
      plugins: [props.build?.input?.plugins, virtualEntryPlugin(entry)],
    },
    {
      ...props.build?.output,
      format: "esm",
      sourcemap: props.build?.output?.sourcemap ?? false,
      minify: props.build?.output?.minify ?? false,
      entryFileNames: "index.mjs",
      strictExecutionOrder: true,
      keepNames: true,
    },
    props.build,
  );
  const files = output.files.map((file) => ({
    path: file.path,
    content:
      typeof file.content === "string" ? new TextEncoder().encode(file.content) : file.content,
  }));
  return { files, hash: output.hash } satisfies BundledProgram;
});

/** Hash extra files (e.g. static assets) packed with the program. */
export const hashFiles = Effect.fn(function* (
  files: ReadonlyArray<{ readonly path: string; readonly content: Uint8Array }>,
) {
  const entries: Record<string, string> = {};
  for (const file of files) entries[file.path] = yield* sha256(file.content);
  return yield* sha256Object(entries);
});

/** Read a directory tree into files under `prefix`. */
export const readTree = Effect.fn(function* (source: string, prefix: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const names = yield* fs.readDirectory(source, { recursive: true });
  const files: Array<{ path: string; content: Uint8Array }> = [];
  for (const name of names) {
    const full = path.join(source, name);
    if ((yield* fs.stat(full)).type !== "File") continue;
    files.push({
      path: `${prefix}/${name.replaceAll("\\", "/")}`.replace(/^\/+/, ""),
      content: yield* fs.readFile(full),
    });
  }
  return files;
});

/** Single-quote a value for `sh`. */
const shellQuote = (value: string) => `'${value.replaceAll("'", `'"'"'`)}'`;

const envValue = (value: unknown): string =>
  Redacted.isRedacted(value)
    ? String(Redacted.value(value))
    : typeof value === "string"
      ? value
      : JSON.stringify(value ?? null);

/** Boot a sandbox that is paused or stopped, and wait until it runs. */
export const ensureRunning = Effect.fn(function* (sid: string) {
  const sandbox = yield* waitForSandbox(sid);
  if (sandbox.status === "paused") {
    yield* archil
      .resumeSandbox({ sid })
      .pipe(Effect.catchTag("SandboxStateConflict", () => Effect.void));
  } else if (sandbox.status === "stopped" || sandbox.status === "exited") {
    yield* archil
      .startSandbox({ sid })
      .pipe(Effect.catchTag("SandboxStateConflict", () => Effect.void));
  }
  const ready = yield* waitForSandbox(sid);
  if (ready.status !== "running") {
    return yield* new ServiceDeployFailed({
      service: sid,
      message: `Sandbox ${ready.name} is ${ready.status}, not running`,
    });
  }
});

/** Run a shell command in the sandbox and fail with its output on a non-zero exit. */
const sh = Effect.fn(function* (sid: string, service: string, command: string) {
  const result = yield* Process.exec(sid, command);
  if (result.exitCode !== 0) {
    return yield* new ServiceDeployFailed({
      service,
      message: `\`${command.split("\n")[0]}\` exited ${result.exitCode ?? result.status}: ${(result.stderr || result.stdout).trim()}`,
    });
  }
  return result.stdout;
});

/** The directory a service's program lives in. */
export const serviceDir = (name: string) => `/opt/alchemy/${name}`;

/**
 * Upload the program, (re)define the service, and wait until it answers on
 * `port` inside the sandbox.
 */
export const deployService = Effect.fn(function* (input: {
  readonly sid: string;
  readonly name: string;
  readonly port: number;
  readonly files: ReadonlyArray<{ readonly path: string; readonly content: Uint8Array }>;
  readonly env: Record<string, unknown>;
}) {
  const dir = serviceDir(input.name);
  const staging = `${dir}.next`;
  yield* ensureRunning(input.sid);
  yield* sh(
    input.sid,
    input.name,
    `rm -rf ${shellQuote(staging)} && mkdir -p ${shellQuote(staging)}`,
  );
  yield* Effect.forEach(
    input.files,
    (file) => Process.uploadFile(input.sid, `${staging}/${file.path}`, file.content),
    { concurrency: 4, discard: true },
  );
  const envFlags = Object.entries({ ...input.env, PORT: String(input.port) })
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `--env ${shellQuote(`${key}=${envValue(value)}`)}`)
    .join(" ");
  yield* sh(
    input.sid,
    input.name,
    [
      `archil-sandbox services delete ${shellQuote(input.name)} >/dev/null 2>&1 || true`,
      `rm -rf ${shellQuote(dir)} && mv ${shellQuote(staging)} ${shellQuote(dir)}`,
      `archil-sandbox services create ${shellQuote(input.name)} --tcp-port ${input.port} --dir ${shellQuote(dir)} ${envFlags} -- node index.mjs >/dev/null`,
    ].join(" && "),
  );
  // Wait until the process answers HTTP on its port.
  const probe = `curl -s -o /dev/null -m 2 -w '%{http_code}' http://127.0.0.1:${input.port}/ || true`;
  const code = yield* Process.exec(input.sid, probe).pipe(
    Effect.map((r) => r.stdout.trim()),
    Effect.repeat({
      schedule: Schedule.spaced("1 second"),
      until: (status) => status !== "" && status !== "000",
      times: 30,
    }),
  );
  if (code === "" || code === "000") {
    const logs = yield* Process.exec(
      input.sid,
      `archil-sandbox services logs ${shellQuote(input.name)} 2>&1 | tail -n 40`,
    ).pipe(
      Effect.map((r) => r.stdout),
      Effect.catch(() => Effect.succeed("")),
    );
    return yield* new ServiceDeployFailed({
      service: input.name,
      message: `Service ${input.name} never answered on port ${input.port}.\n${logs}`,
    });
  }
});

/** Stop the service and remove its program. Removing a missing one succeeds. */
export const removeService = Effect.fn(function* (sid: string, name: string) {
  const sandbox = yield* waitForSandbox(sid).pipe(
    Effect.catchTag("SandboxNotFound", () => Effect.undefined),
  );
  if (sandbox === undefined || sandbox.status === "deleted") return;
  yield* ensureRunning(sid);
  yield* Process.exec(
    sid,
    `archil-sandbox services delete ${shellQuote(name)} >/dev/null 2>&1; rm -rf ${shellQuote(serviceDir(name))}`,
  );
});
