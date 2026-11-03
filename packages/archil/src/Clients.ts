/**
 * The run-time handles Archil bindings return. A disk or sandbox handle is
 * the same shape whether it came from a deploy-time resource
 * (`ExecDisk(Dataset)`, `ConnectSandbox(Preview)`) or was created at run
 * time (`CreateDisk`, `CreateSandbox`), and takes Archil's own parameters.
 */
import * as archil from "@distilled.cloud/archil";
import * as Objects from "@distilled.cloud/archil/Objects";
import * as Process from "@distilled.cloud/archil/Process";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import * as StreamM from "effect/Stream";
import type { Authorize } from "./ArchilBindings.ts";
import type { BranchAttributes } from "./Branch.ts";
import { toBranchAttributes } from "./Branch.ts";
import type { DiskAttributes } from "./Disk.ts";
import type { DiskUserAttributes } from "./DiskUser.ts";
import { addDiskUser, removeDiskUser, type DiskUserUnexpectedResponse } from "./DiskUser.ts";
import { deleteSandbox, observeSandbox, type SandboxAttributes } from "./Sandbox.ts";
import { toEgressPolicy, type SandboxEgress } from "./SandboxOptions.ts";
import type { SandboxPortTokenAttributes } from "./SandboxPortToken.ts";
import { createPortToken, revokePortToken } from "./SandboxPortToken.ts";

type R = RuntimeContext;

/** What a serverless exec printed, and its exit code. */
export interface ExecResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
  /** Where the time went, as Archil reports it. */
  readonly timing: archil.ExecTiming;
}

/** Search options for `grep`. */
export interface GrepOptions {
  /** Directory on the disk to search. @default "/" */
  readonly directory?: string;
  /** Regular expression. */
  readonly pattern: string;
  /** Search subdirectories. @default false */
  readonly recursive?: boolean;
  /** Stop after this many seconds. @default 30 */
  readonly maxDurationSeconds?: number;
  /** Workers to fan out to. @default 50 */
  readonly concurrency?: number;
  /** Stop after this many matches. @default 1000 */
  readonly maxResults?: number;
}

/** Serverless commands and search against one disk. */
export interface ExecDiskClient {
  /**
   * Run a shell command in a fresh microVM with the disk mounted at
   * `/mnt/archil`. Blocks until it exits (up to five minutes).
   */
  exec(command: string): Effect.Effect<ExecResult, archil.ExecDiskError, R>;
  /** Constant-time parallel grep across the disk's files. */
  grep(options: GrepOptions): Effect.Effect<archil.GrepDiskResult, archil.GrepDiskError, R>;
}

/** Read the disk's files as objects. */
export interface ReadDiskClient {
  /** A file's bytes, optionally a byte range (`bytes=0-1023`). */
  get(
    key: string,
    options?: { readonly range?: string },
  ): Effect.Effect<Uint8Array, Objects.ObjectsFailure | ObjectsTransportError, R>;
  /** A file as UTF-8 text. */
  getText(key: string): Effect.Effect<string, Objects.ObjectsFailure | ObjectsTransportError, R>;
  /** A file's metadata, or `undefined` when it doesn't exist. */
  head(
    key: string,
  ): Effect.Effect<
    Objects.ObjectMetadata | undefined,
    Objects.ObjectsFailure | ObjectsTransportError,
    R
  >;
  /** Every file under a prefix, lazily paginated. */
  list(
    prefix?: string,
    options?: { readonly recursive?: boolean },
  ): Stream.Stream<Objects.ObjectSummary, Objects.ObjectsFailure | ObjectsTransportError, R>;
  /**
   * A signed URL anyone can download one file from, valid for
   * `expiresInSeconds` (1 to 604800; default one day).
   */
  share(
    key: string,
    expiresInSeconds?: number,
  ): Effect.Effect<Redacted.Redacted<string>, archil.CreateShareUrlError, R>;
}

/** Write the disk's files as objects. */
export interface WriteDiskClient {
  /** Write a file (≤ 5 GiB). */
  put(
    key: string,
    body: Uint8Array | string,
    options?: { readonly contentType?: string } & Objects.PosixAttributes,
  ): Effect.Effect<{ etag: string | undefined }, Objects.ObjectsFailure | ObjectsTransportError, R>;
  /** Append to a file, creating it when missing (≤ 1 MiB per call). */
  append(
    key: string,
    body: Uint8Array | string,
  ): Effect.Effect<{ etag: string | undefined }, Objects.ObjectsFailure | ObjectsTransportError, R>;
  /** Delete a file. Deleting a missing file succeeds. */
  delete(key: string): Effect.Effect<void, Objects.ObjectsFailure | ObjectsTransportError, R>;
  /** Delete many files; per-file failures are returned. */
  deleteMany(keys: ReadonlyArray<string>): Effect.Effect<
    {
      deleted: string[];
      errors: Array<{ readonly key: string; readonly code?: string; readonly message?: string }>;
    },
    Objects.ObjectsFailure | ObjectsTransportError,
    R
  >;
  /** Copy a file within the disk. */
  copy(
    sourceKey: string,
    key: string,
  ): Effect.Effect<{ etag: string | undefined }, Objects.ObjectsFailure | ObjectsTransportError, R>;
  /** Write delegations held on the disk, e.g. by a crashed mount. */
  delegations(): Effect.Effect<archil.DelegationEntry[], archil.ListDelegationsError, R>;
  /** Reclaim write access from a client that is gone. */
  revokeDelegation(
    delegation: Pick<archil.DelegationEntry, "clientId" | "inodeId">,
  ): Effect.Effect<void, archil.RevokeDelegationError, R>;
}

/** Both {@link ReadDiskClient} and {@link WriteDiskClient}. */
export interface ReadWriteDiskClient extends ReadDiskClient, WriteDiskClient {}

/** Errors from the HTTP transport underneath the objects API. */
export type ObjectsTransportError = Effect.Error<ReturnType<typeof Objects.headObject>>;

/** Branches of one disk. */
export interface BranchDiskClient {
  /**
   * Create a writable branch from a committed checkpoint, or return the
   * branch with that name.
   */
  branch(options: {
    readonly name: string;
    readonly fromCheckpoint: string;
    readonly fromBranch?: string;
  }): Effect.Effect<BranchAttributes, archil.CreateBranchError | archil.GetBranchError, R>;
  /** A branch, or `undefined` when it doesn't exist. */
  getBranch(name: string): Effect.Effect<BranchAttributes | undefined, archil.GetBranchError, R>;
  /** Every branch of the disk, including branches of branches. */
  listBranches(): Effect.Effect<BranchAttributes[], archil.ListBranchesError, R>;
}

/** One disk, with everything a run-time client can do to it. */
export interface DiskHandle extends ExecDiskClient, ReadWriteDiskClient, BranchDiskClient {
  readonly diskId: string;
  readonly name: string;
  /**
   * A disk token for mounting this disk only, e.g. to hand to a machine
   * that mounts with `MountDisk()`. Revoke it with `removeToken`.
   */
  createToken(
    nickname: string,
  ): Effect.Effect<DiskUserAttributes, archil.AddDiskUserError | DiskUserUnexpectedResponse, R>;
  /** Revoke a disk token. */
  removeToken(identifier: string): Effect.Effect<void, archil.RemoveDiskUserError, R>;
}

/** Options for a sandbox process. */
export type RunOptions = Process.RunOptions;

/** One sandbox, with everything a run-time client can do to it. */
/**
 * What a process call on a sandbox handle can fail with. A paused sandbox
 * is resumed first, so resuming can fail too.
 */
export type ProcessCallError =
  | Process.ProcessError
  | archil.CreateSandboxConnectionError
  | archil.GetSandboxError
  | archil.ResumeSandboxError;

export interface SandboxHandle {
  readonly sandboxId: string;
  readonly name: string;
  /** The sandbox's current attributes. */
  refresh(): Effect.Effect<SandboxAttributes, archil.GetSandboxError, R>;
  /** Run a command to completion and collect its output. */
  exec(
    command: string,
    options?: RunOptions,
  ): Effect.Effect<Process.ProcessResult, ProcessCallError, R>;
  /**
   * Start a command and attach to its output. Closing the scope detaches;
   * the process keeps running and can be re-attached by id.
   */
  run(
    command: string,
    options?: RunOptions,
  ): Effect.Effect<Process.Process, ProcessCallError, R | Scope.Scope>;
  /** Re-attach to a running process, resuming its output at `offset`. */
  attach(
    processId: string,
    offset?: number,
  ): Effect.Effect<Process.Process, ProcessCallError, R | Scope.Scope>;
  /** Kill a running process. */
  kill(processId: string): Effect.Effect<void, ProcessCallError, R>;
  /** Write a file in the sandbox. */
  writeFile(
    path: string,
    data: string | Uint8Array,
    options?: { readonly mode?: number },
  ): Effect.Effect<void, ProcessCallError, R>;
  /** Read a file from the sandbox. */
  readFile(path: string): Effect.Effect<Uint8Array, ProcessCallError, R>;
  /** Start a stopped sandbox. */
  start(): Effect.Effect<SandboxAttributes, archil.StartSandboxError, R>;
  /** Stop the sandbox (its disk survives; memory doesn't). */
  stop(): Effect.Effect<SandboxAttributes, archil.StopSandboxError, R>;
  /** Pause the sandbox, keeping its memory. */
  pause(): Effect.Effect<SandboxAttributes, archil.PauseSandboxError, R>;
  /** Resume a paused sandbox. */
  resume(): Effect.Effect<SandboxAttributes, archil.ResumeSandboxError, R>;
  /** Fork the sandbox (memory and disk) into a new one. */
  fork(options?: {
    readonly name?: string;
    readonly checkpoint?: string;
  }): Effect.Effect<SandboxHandle, archil.ForkSandboxError, R>;
  /**
   * Pause after `timeoutSeconds` from now, and/or after `idleTtlSeconds`
   * without a connection (`0` disables idle expiry).
   */
  setTimeout(options: {
    readonly timeoutSeconds?: number;
    readonly idleTtlSeconds?: number;
  }): Effect.Effect<SandboxAttributes, archil.SetSandboxTimeoutError, R>;
  /** Serve a port publicly. */
  exposePort(
    port: number,
  ): Effect.Effect<{ port: number; hostname: string }, archil.ExposeSandboxPortError, R>;
  /** Stop serving a port. */
  unexposePort(port: number): Effect.Effect<void, archil.DeleteSandboxPortError, R>;
  /** Ports the sandbox serves. */
  ports(): Effect.Effect<
    Array<{ port: number; hostname: string }>,
    archil.ListSandboxPortsError,
    R
  >;
  /** A token that opens one port for a client that shouldn't hold an API key. */
  createPortToken(
    port: number,
    ttl?: string,
  ): Effect.Effect<SandboxPortTokenAttributes, archil.CreateSandboxPortTokenError, R>;
  /** Revoke a port token. */
  revokePortToken(tokenId: string): Effect.Effect<void, archil.DeleteSandboxPortTokenError, R>;
  /** Replace the outbound network policy. Applies at once. */
  setEgress(egress: SandboxEgress): Effect.Effect<void, archil.UpdateSandboxNetworkError, R>;
  /**
   * Stop (if it's live) and delete the sandbox. Delete forks first: Archil
   * won't delete a sandbox that has forks. Deleting a missing sandbox
   * succeeds.
   */
  delete(): Effect.Effect<void, Effect.Error<ReturnType<typeof deleteSandbox>>, R>;
}

const toExecResult = (data: archil.ExecDiskResult): ExecResult => ({
  exitCode: data.exitCode,
  stdout: data.stdout,
  stderr: data.stderr,
  timing: data.timing,
});

const unwrapToken = (value: string | Redacted.Redacted<string>) =>
  typeof value === "string" ? Redacted.make(value) : value;

/** Serverless exec + grep on a disk whose id resolves at run time. */
export const makeExecDiskClient = (
  diskId: Effect.Effect<string, never, R>,
  authorize: Authorize,
): ExecDiskClient => ({
  exec: (command) =>
    authorize(
      Effect.gen(function* () {
        const id = yield* diskId;
        const { data } = yield* archil.execDisk({ id, command });
        return toExecResult(data);
      }),
    ),
  grep: (options) =>
    authorize(
      Effect.gen(function* () {
        const { data } = yield* archil.grepDisk({
          id: yield* diskId,
          directory: options.directory ?? "/",
          pattern: options.pattern,
          recursive: options.recursive,
          maxDurationSeconds: options.maxDurationSeconds,
          concurrency: options.concurrency,
          maxResults: options.maxResults,
        });
        return data;
      }),
    ),
});

/** Object reads on a disk (or branch) whose id resolves at run time. */
export const makeReadDiskClient = (
  ref: Effect.Effect<Objects.DiskRef, never, R>,
  authorize: Authorize,
): ReadDiskClient => ({
  get: (key, options) => authorize(Effect.flatMap(ref, (r) => Objects.getObject(r, key, options))),
  getText: (key) => authorize(Effect.flatMap(ref, (r) => Objects.getObjectText(r, key))),
  head: (key) => authorize(Effect.flatMap(ref, (r) => Objects.headObject(r, key))),
  list: (prefix, options) =>
    authorize.stream(
      StreamM.unwrap(
        Effect.map(ref, (r) => Objects.listObjects(r, prefix, { recursive: options?.recursive })),
      ),
    ),
  share: (key, expiresInSeconds) =>
    authorize(
      Effect.gen(function* () {
        const { disk } = yield* ref;
        const { data } = yield* archil.createShareUrl({
          id: disk,
          key,
          expiresIn: expiresInSeconds,
        });
        return unwrapToken(data.url);
      }),
    ),
});

/** Object writes on a disk (or branch) whose id resolves at run time. */
export const makeWriteDiskClient = (
  ref: Effect.Effect<Objects.DiskRef, never, R>,
  authorize: Authorize,
): WriteDiskClient => ({
  put: (key, body, options) =>
    authorize(Effect.flatMap(ref, (r) => Objects.putObject(r, key, body, options))),
  append: (key, body) => authorize(Effect.flatMap(ref, (r) => Objects.appendObject(r, key, body))),
  delete: (key) => authorize(Effect.flatMap(ref, (r) => Objects.deleteObject(r, key))),
  deleteMany: (keys) => authorize(Effect.flatMap(ref, (r) => Objects.deleteObjects(r, keys))),
  copy: (sourceKey, key) =>
    authorize(
      Effect.flatMap(ref, (r) => Objects.copyObject(r, sourceKey, key)).pipe(
        Effect.map(({ etag }) => ({ etag })),
      ),
    ),
  delegations: () =>
    authorize(
      Effect.gen(function* () {
        const { disk } = yield* ref;
        const { data } = yield* archil.listDelegations({ id: disk });
        return data.delegations;
      }),
    ),
  revokeDelegation: (delegation) =>
    authorize(
      Effect.gen(function* () {
        const { disk } = yield* ref;
        yield* archil.revokeDelegation({
          id: disk,
          clientId: delegation.clientId,
          inodeId: delegation.inodeId,
        });
      }),
    ),
});

/** Branches of a disk whose id resolves at run time. */
export const makeBranchDiskClient = (
  diskId: Effect.Effect<string, never, R>,
  authorize: Authorize,
): BranchDiskClient => ({
  branch: (options) =>
    authorize(
      Effect.gen(function* () {
        const id = yield* diskId;
        const existing = yield* archil.getBranch({ id, name: options.name }).pipe(
          Effect.map(({ data }) => toBranchAttributes(data)),
          Effect.catchTag("BranchNotFound", () => Effect.undefined),
        );
        if (existing) return existing;
        const { data } = yield* archil.createBranch({
          id,
          branch_name: options.name,
          from_checkpoint_name: options.fromCheckpoint,
          from_branch: options.fromBranch,
        });
        return toBranchAttributes(data);
      }),
    ),
  getBranch: (name) =>
    authorize(
      Effect.gen(function* () {
        const id = yield* diskId;
        return yield* archil.getBranch({ id, name }).pipe(
          Effect.map(({ data }) => toBranchAttributes(data)),
          Effect.catchTag("BranchNotFound", () => Effect.undefined),
        );
      }),
    ),
  listBranches: () =>
    authorize(
      Effect.gen(function* () {
        const id = yield* diskId;
        const { data } = yield* archil.listBranches({ id });
        return data.map(toBranchAttributes);
      }),
    ),
});

/** A full handle on a disk known by id and name. */
export const makeDiskHandle = (
  disk: Pick<DiskAttributes, "diskId" | "name">,
  authorize: Authorize,
): DiskHandle => {
  const diskId = Effect.succeed(disk.diskId);
  const ref = Effect.succeed({ disk: disk.diskId } satisfies Objects.DiskRef);
  return {
    diskId: disk.diskId,
    name: disk.name,
    ...makeExecDiskClient(diskId, authorize),
    ...makeReadDiskClient(ref, authorize),
    ...makeWriteDiskClient(ref, authorize),
    ...makeBranchDiskClient(diskId, authorize),
    createToken: (nickname) => authorize(addDiskUser(disk.diskId, { nickname })),
    removeToken: (identifier) =>
      authorize(removeDiskUser({ diskId: disk.diskId, type: "token", identifier })),
  };
};

/**
 * A full handle on a sandbox known by id and name. `forkName` maps a fork's
 * name (a binding stamps its prefix on it so destroy can sweep forks too).
 */
export const makeSandboxHandle = (
  sandbox: { readonly sandboxId: string; readonly name: string },
  authorize: Authorize,
  forkName: (name: string | undefined) => Effect.Effect<string | undefined> = Effect.succeed,
): SandboxHandle => {
  const sid = sandbox.sandboxId;
  // A paused sandbox (idle TTL) refuses process connections; resume it and
  // try once more.
  const awake = <A, E extends { readonly _tag: string }, R2>(effect: Effect.Effect<A, E, R2>) =>
    authorize(
      effect.pipe(
        Effect.catchIf(
          (e) => e._tag === "SandboxStateConflict",
          (e) =>
            archil
              .getSandbox({ sid })
              .pipe(
                Effect.flatMap(({ data }) =>
                  data.status === "paused"
                    ? archil.resumeSandbox({ sid, wait: true }).pipe(Effect.andThen(effect))
                    : Effect.fail(e),
                ),
              ),
        ),
      ),
    );
  const attrs = <E>(effect: Effect.Effect<{ data: archil.Sandbox }, E, any>) =>
    authorize(effect.pipe(Effect.flatMap(({ data }) => observeSandbox(data))));
  const handle: SandboxHandle = {
    sandboxId: sid,
    name: sandbox.name,
    refresh: () => attrs(archil.getSandbox({ sid })),
    exec: (command, options) => awake(Process.exec(sid, command, options)),
    run: (command, options) => awake(Process.run(sid, command, options)),
    attach: (processId, offset) => awake(Process.attach(sid, processId, offset)),
    kill: (processId) => awake(Process.kill(sid, processId)),
    writeFile: (path, data, options) => awake(Process.uploadFile(sid, path, data, options)),
    readFile: (path) => awake(Process.downloadFile(sid, path)),
    start: () => attrs(archil.startSandbox({ sid, wait: true })),
    stop: () => attrs(archil.stopSandbox({ sid })),
    pause: () => attrs(archil.pauseSandbox({ sid })),
    resume: () => attrs(archil.resumeSandbox({ sid })),
    fork: (options) =>
      authorize(
        forkName(options?.name).pipe(
          Effect.flatMap((name) =>
            archil.forkSandbox({ sid, wait: true, name, checkpoint: options?.checkpoint }),
          ),
        ),
      ).pipe(
        Effect.map(({ data }) =>
          makeSandboxHandle({ sandboxId: data.sandbox_id, name: data.name }, authorize, forkName),
        ),
      ),
    setTimeout: (options) =>
      attrs(
        archil.setSandboxTimeout({
          sid,
          timeout: options.timeoutSeconds,
          idle_ttl_seconds: options.idleTtlSeconds,
        }),
      ),
    exposePort: (port) =>
      authorize(archil.exposeSandboxPort({ sid, port })).pipe(
        Effect.map(({ data }) => ({ port: data.port, hostname: data.hostname })),
      ),
    unexposePort: (port) => authorize(archil.deleteSandboxPort({ sid, port })).pipe(Effect.asVoid),
    ports: () =>
      authorize(archil.listSandboxPorts({ sid })).pipe(
        Effect.map(({ data }) =>
          (data.ports ?? []).map((p) => ({ port: p.port, hostname: p.hostname })),
        ),
      ),
    createPortToken: (port, ttl) => authorize(createPortToken(sid, port, ttl)),
    revokePortToken: (tokenId) => authorize(revokePortToken(sid, tokenId)),
    setEgress: (egress) =>
      authorize(archil.updateSandboxNetwork({ sid, egress: toEgressPolicy(egress) })).pipe(
        Effect.asVoid,
      ),
    delete: () => authorize(deleteSandbox(sid)),
  };
  return handle;
};
