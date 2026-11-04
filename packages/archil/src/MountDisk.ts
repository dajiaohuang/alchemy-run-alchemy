import { DEFAULT_REGION } from "@distilled.cloud/archil/Credentials";
import * as Binding from "alchemy/Binding";
import type { MountBinding, MountCapability } from "alchemy/Mount";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { ChildProcess } from "effect/process";
import { ChildProcessSpawner } from "effect/process/ChildProcessSpawner";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type { Disk } from "./Disk.ts";
import { DiskUser } from "./DiskUser.ts";

/**
 * How a mount shares the disk with other clients.
 *
 * - `exclusive` — this client owns writes to the whole disk (local-disk
 *   speed); other clients can still mount `read-only`.
 * - `shared` — many clients; the disk is read-only until a path is checked
 *   out with `archil checkout <path>`.
 * - `conditional` — many writers; writes go to the server unless a path is
 *   checked out (remote-filesystem speed).
 * - `read-only` — no writes.
 */
export type MountMode = "exclusive" | "shared" | "conditional" | "read-only";

export interface MountOptions {
  /**
   * Absolute directory to mount at. Created when missing.
   * @default "/mnt/archil"
   */
  readonly path?: string;
  /** @default "exclusive" */
  readonly mode?: MountMode;
  /** Mount a branch of the disk instead of its root. */
  readonly branch?: string;
  /** Mount only this subdirectory of the disk. */
  readonly subdirectory?: string;
  /**
   * Take ownership of the root from a client that crashed without
   * unmounting. Outstanding writes of that client are lost.
   */
  readonly force?: boolean;
}

/** A run-time mount of a disk named at call time (`MountDisk()`). */
export interface MountAnyOptions extends MountOptions {
  /** Disk id (`dsk-…`) or `<organization>/<name>`. */
  readonly disk: string;
  /** A disk token for that disk, e.g. from `DiskHandle.createToken`. */
  readonly token: string | Redacted.Redacted<string>;
  /** Region the disk lives in. @default the region bound at deploy */
  readonly region?: string;
}

/** `archil mount` or `archil unmount` failed. */
export class MountFailed extends Data.TaggedError("Archil.MountFailed")<{
  readonly disk: string;
  readonly path: string;
  readonly exitCode: number;
  readonly stderr: string;
}> {
  override get message() {
    return `archil mount ${this.disk} at ${this.path} exited ${this.exitCode}: ${this.stderr.trim()}`;
  }
}

/** The host can't run the Archil client (no process spawner, e.g. a Worker). */
export class MountNotSupported extends Data.TaggedError("Archil.MountNotSupported")<{
  readonly message: string;
}> {}

/** The path is already mounted in this process with different options. */
export class MountConflict extends Data.TaggedError("Archil.MountConflict")<{
  readonly path: string;
  readonly message: string;
}> {}

export type MountError = MountFailed | MountNotSupported | MountConflict;

/** Mounts one deploy-time disk, with a disk user minted for this host. */
export interface MountDiskClient {
  /**
   * Mount the disk for the life of the current scope (typically one
   * request) and return the mount path. Concurrent scopes that mount the
   * same path with the same options share one mount; the last one to close
   * flushes and unmounts, releasing any delegations the mount held.
   */
  mount(options?: MountOptions): Effect.Effect<string, MountError, RuntimeContext | Scope.Scope>;
}

/** Mounts any disk the program names at run time. */
export interface MountAnyDiskClient {
  /**
   * Mount a disk with its token for the life of the current scope and
   * return the mount path.
   */
  mount(options: MountAnyOptions): Effect.Effect<string, MountError, RuntimeContext | Scope.Scope>;
}

/**
 * The Archil FUSE client, installed into a host's image. `archil mount`
 * needs `/dev/fuse` and `CAP_SYS_ADMIN`.
 */
export const archilClientCapability: MountCapability = {
  id: "archil",
  install: [
    "RUN (command -v apt-get >/dev/null && apt-get update && apt-get install -y --no-install-recommends ca-certificates curl fuse3 && rm -rf /var/lib/apt/lists/*) || (command -v apk >/dev/null && apk add --no-cache ca-certificates curl fuse3)",
    "RUN curl -fsSL https://archil.com/install | sh",
  ],
  requires: { fuse: true },
};

/**
 * Bind the Archil client capability onto the current host. A mount is a
 * host-only attachment (an image layer and a kernel feature), so this is
 * the one place Archil binds directly onto its host.
 */
const bindClientCapability = Effect.gen(function* () {
  if (globalThis.__ALCHEMY_RUNTIME__) return;
  const host = yield* Binding.Host;
  if (!host) return;
  // `Binding.Host` is typed as a bare `ResourceLike` (no `bind`): a host's
  // binding contract is only known per host type, and `MountBinding` is the
  // slice every mount-capable host accepts.
  yield* (
    host as unknown as { bind: (sid: string, data: MountBinding) => Effect.Effect<void> }
  ).bind("Archil.MountDisk", { mounts: [archilClientCapability] });
});

const modeFlags = (mode: MountMode | undefined) =>
  mode === "shared"
    ? ["--shared"]
    : mode === "conditional"
      ? ["--conditional"]
      : mode === "read-only"
        ? ["--read-only"]
        : [];

const run = (
  spawner: ChildProcessSpawner["Service"],
  command: string,
  args: ReadonlyArray<string>,
  env: Record<string, string | Redacted.Redacted<string>> = {},
) =>
  Effect.gen(function* () {
    const handle = yield* ChildProcess.make(command, args, {
      env,
      extendEnv: true,
      shell: false,
      // A client stuck on the network may ignore SIGTERM; don't wait on it.
      forceKillAfter: "3 seconds",
    });
    let stderr = "";
    const reader = yield* handle.stderr.pipe(
      Stream.decodeText,
      Stream.runForEach((chunk) => Effect.sync(() => void (stderr += chunk))),
      Effect.forkScoped,
    );
    // A client that never finishes attaching (no FUSE, no route to Archil)
    // is killed, and reported with what it printed so far.
    const exitCode = yield* handle.exitCode.pipe(
      Effect.timeoutOption("60 seconds"),
      Effect.flatMap((code) =>
        Option.isSome(code)
          ? Effect.succeed(Number(code.value))
          : handle.kill().pipe(
              Effect.ignore,
              Effect.as(124),
              Effect.tap(() =>
                Effect.sync(
                  () =>
                    void (stderr +=
                      "\n(timed out after 60s: check that the host can reach Archil; a Cloudflare Container needs `start({ enableInternet: true })`)"),
                ),
              ),
            ),
      ),
    );
    // Let the stderr reader drain what's left.
    yield* Fiber.join(reader).pipe(Effect.timeoutOption("2 seconds"), Effect.ignore);
    return { exitCode, stderr };
  }).pipe(
    Effect.scoped,
    Effect.provideService(ChildProcessSpawner, spawner),
    // The binary is missing or can't start: report it like a failed command.
    Effect.catch((cause) => Effect.succeed({ exitCode: 127, stderr: String(cause) })),
  );

/** The mounts this process holds, by path. */
interface Mounts {
  readonly lock: Semaphore.Semaphore;
  readonly byPath: Map<string, { readonly key: string; users: number }>;
}

const makeMounts = Effect.gen(function* () {
  return {
    lock: yield* Semaphore.make(1),
    byPath: new Map(),
  } satisfies Mounts;
});

// `archil unmount` returns before the client's daemon exits, and a mount at
// the same path fails until it has.
const isStillUnmounting = (e: MountError) =>
  e._tag === "Archil.MountFailed" && e.stderr.includes("older Archil process is still running");

/** Mount a disk for the life of the ambient scope. */
const mountFor = (
  spawner: Option.Option<ChildProcessSpawner["Service"]>,
  mounts: Mounts,
  input: {
    readonly disk: string;
    readonly token: Redacted.Redacted<string> | undefined;
    readonly region: string;
    readonly options: MountOptions;
  },
) =>
  Effect.gen(function* () {
    if (Option.isNone(spawner)) {
      return yield* new MountNotSupported({
        message:
          "Archil.MountDisk needs a host that can run processes (a Container, Fly Machine, ECS task, or VM).",
      });
    }
    const path = input.options.path ?? "/mnt/archil";
    const target = input.options.subdirectory
      ? `${input.disk}:/${input.options.subdirectory.replace(/^\//, "")}`
      : input.disk;
    const args = [
      "mount",
      target,
      path,
      "--region",
      input.region,
      ...modeFlags(input.options.mode),
      ...(input.options.branch ? ["--branch", input.options.branch] : []),
      ...(input.options.force ? ["--force"] : []),
    ];
    const key = args.join(" ");
    const env: Record<string, string | Redacted.Redacted<string>> = input.token
      ? { ARCHIL_MOUNT_TOKEN: input.token }
      : {};
    const mount = Effect.gen(function* () {
      const result = yield* run(spawner.value, "archil", args, env);
      if (result.exitCode !== 0) {
        return yield* new MountFailed({ disk: input.disk, path, ...result });
      }
    }).pipe(
      Effect.retry({
        while: isStillUnmounting,
        schedule: Schedule.spaced("1 second"),
        times: 15,
      }),
    );
    yield* Effect.acquireRelease(
      Effect.gen(function* () {
        const held = mounts.byPath.get(path);
        if (held !== undefined) {
          if (held.key !== key) {
            return yield* new MountConflict({
              path,
              message: `${path} is already mounted with \`archil ${held.key}\``,
            });
          }
          held.users++;
          return;
        }
        yield* run(spawner.value, "mkdir", ["-p", path]);
        yield* mount;
        mounts.byPath.set(path, { key, users: 1 });
      }).pipe(mounts.lock.withPermits(1)),
      () =>
        Effect.gen(function* () {
          const held = mounts.byPath.get(path);
          if (held === undefined || --held.users > 0) return;
          mounts.byPath.delete(path);
          yield* run(spawner.value, "archil", ["unmount", path], env);
        }).pipe(mounts.lock.withPermits(1)),
    );
    return path;
  });

export interface MountDiskOptions {
  /**
   * Region of the disks the program mounts with `MountDisk()`. Ignored for
   * `MountDisk(disk)`, which uses the disk's own region.
   * @default "aws-us-east-1"
   */
  region?: string;
  /**
   * Give the host a read-only disk user, so its mounts reject writes
   * (`EROFS`) whatever `mode` they ask for. Only for `MountDisk(disk)`.
   * @default false
   */
  readOnly?: boolean;
}

/**
 * Mount Archil disks on your own machines: a Cloudflare Container, an ECS
 * task, a Fly Machine, or any VM that runs your program. The binding
 * installs the Archil client into the host's image and checks at plan time
 * that the host provides FUSE. Mounts happen at run time, per request, and
 * last as long as the scope that made them.
 *
 * `MountDisk(disk)` mounts one deploy-time disk: it creates a disk user for
 * this host, so Archil itself refuses this host access to any other disk,
 * and the host never holds an API key. `MountDisk()` mounts whatever disk
 * the program names, with a disk token it receives at run time (e.g. from
 * a control Worker's `DiskHandle.createToken`).
 *
 * The client reaches Archil over the network, so start a Cloudflare
 * Container with `container.start({ enableInternet: true })`.
 *
 * ### Mounting a Shared Disk
 * **Example:** Read a dataset in a Cloudflare Container
 * ```typescript
 * export default class Indexer extends Cloudflare.Container<Indexer>()(
 *   "Indexer",
 *   { main: import.meta.url },
 *   Effect.gen(function* () {
 *     const dataset = yield* Archil.MountDisk(Dataset);
 *     const fs = yield* FileSystem.FileSystem;
 *     return {
 *       fetch: Effect.scoped(
 *         Effect.gen(function* () {
 *           const path = yield* dataset.mount({ mode: "read-only" });
 *           const entries = yield* fs.readDirectory(path);
 *           return yield* HttpServerResponse.json(entries);
 *         }),
 *       ),
 *     };
 *   }).pipe(Effect.provide(Archil.MountDiskLive)),
 * ) {}
 * ```
 *
 * **Example:** A host that can only read the disk
 * ```typescript
 * const dataset = yield* Archil.MountDisk(Dataset, { readOnly: true });
 * const path = yield* dataset.mount({ mode: "read-only" });
 * ```
 *
 * ### Mounting Disks Named at Run Time
 * **Example:** Mount the disk a job names, with its token
 * ```typescript
 * const archil = yield* Archil.MountDisk();
 * const path = yield* archil.mount({ disk: job.diskId, token: job.token, path: "/work" });
 * ```
 *
 * @binding
 * @product Disk
 */
export interface MountDisk extends Binding.Service<
  MountDisk,
  "Archil.MountDisk",
  {
    (disk: Disk, options?: Pick<MountDiskOptions, "readOnly">): Effect.Effect<MountDiskClient>;
    (disk?: undefined, options?: MountDiskOptions): Effect.Effect<MountAnyDiskClient>;
  } & ((
    disk?: Disk,
    options?: MountDiskOptions,
  ) => Effect.Effect<MountDiskClient & MountAnyDiskClient>)
> {}

export const MountDisk = Binding.Service<MountDisk>("Archil.MountDisk");

/** {@link MountDisk} over the Archil FUSE client. */
export const MountDiskLive = Layer.effect(
  MountDisk,
  Effect.gen(function* () {
    const Users = yield* DiskUser;
    const spawner = yield* Effect.serviceOption(ChildProcessSpawner);
    const mounts = yield* makeMounts;
    return Effect.fn(function* (disk?: Disk, options?: MountDiskOptions) {
      yield* bindClientCapability;
      if (disk === undefined) {
        const client: MountAnyDiskClient = {
          mount: (input) =>
            mountFor(spawner, mounts, {
              disk: input.disk,
              token: typeof input.token === "string" ? Redacted.make(input.token) : input.token,
              region: input.region ?? options?.region ?? DEFAULT_REGION,
              options: input,
            }),
        };
        return client as MountDiskClient & MountAnyDiskClient;
      }
      const host = yield* Binding.Host;
      const user = yield* Users(`${host?.LogicalId ?? "Script"}${disk.LogicalId}DiskUser`, {
        disk: disk.diskId,
        readOnly: options?.readOnly,
      });
      const token = yield* user.token;
      const diskId = yield* disk.diskId;
      const region = yield* disk.region;
      const client: MountDiskClient = {
        mount: (mountOptions) =>
          Effect.gen(function* () {
            return yield* mountFor(spawner, mounts, {
              disk: yield* diskId,
              token: yield* token,
              region: yield* region,
              options: mountOptions ?? {},
            });
          }),
      };
      return client as MountDiskClient & MountAnyDiskClient;
    });
  }),
);
