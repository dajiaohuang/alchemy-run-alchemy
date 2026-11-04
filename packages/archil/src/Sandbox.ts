import * as archil from "@distilled.cloud/archil";
import { Unowned } from "alchemy/AdoptPolicy";
import { deepEqual, isResolved } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Effect from "effect/Effect";
import type * as Redacted from "effect/Redacted";
import {
  deletableSandboxStates,
  findSandboxByName,
  getSandbox,
  isAlchemyName,
  listSandboxes,
  sandboxName,
  waitForSandbox,
  waitForSandboxGone,
  waitForSandboxStopped,
} from "./internal/archil.ts";
import type { Providers } from "./Providers.ts";
import {
  egressIdentity,
  splitArchilEgress,
  toEgressPolicy,
  toEnv,
  toSandboxMount,
  type SandboxEgress,
  type SandboxEndpoint,
  type SandboxMount,
} from "./SandboxOptions.ts";

export interface SandboxProps {
  /**
   * Sandbox name, unique within your account: lowercase letters, digits
   * and `-`, at most 63 characters. If omitted, a unique name is generated
   * from the stack, stage, and logical ID. Changing it replaces the sandbox.
   */
  name?: string;
  /**
   * Image id from an {@link Image} (e.g. `image.imageId`), for a private
   * image or a pinned digest. Exclusive with `baseImage`. Changing it
   * replaces the sandbox.
   */
  image?: string;
  /**
   * Public OCI image reference with a `linux/amd64` manifest. Exclusive
   * with `image`. Changing it replaces the sandbox.
   * @default "node:24-bookworm"
   */
  baseImage?: string;
  /** vCPUs, 1–32. Changing it replaces the sandbox. @default 1 */
  vcpuCount?: number;
  /** Memory in MiB, 256–65536. Changing it replaces the sandbox. @default 2048 */
  memSizeMiB?: number;
  /**
   * TCP ports to serve publicly. Updated in place. When set, it is the
   * whole list: ports not in it are unexposed, including ports an
   * `Archil.Service` exposed, so leave it unset when services run here.
   */
  ports?: number[];
  /**
   * Let services inside the sandbox publish their own ingress.
   * Changing it replaces the sandbox. @default false
   */
  enableServiceIngress?: boolean;
  /** Environment for the sandbox's processes. Changing it replaces the sandbox. */
  env?: Record<string, string | Redacted.Redacted<string>>;
  /** Outbound network policy. Updated in place. */
  network?: { egress?: SandboxEgress };
  /** Disks to mount. Changing them replaces the sandbox. */
  mounts?: SandboxMount[];
  /**
   * Seconds a powered-on session may run before the sandbox pauses
   * (at most 24 hours). Changing it replaces the sandbox.
   */
  maxTtlSeconds?: number;
  /**
   * Seconds without a process connection before the sandbox pauses.
   * `0` disables idle expiry. Updated in place.
   */
  idleTtlSeconds?: number;
  /** Concurrent processes allowed. Changing it replaces the sandbox. */
  maxConcurrentExecs?: number;
}

export interface SandboxAttributes {
  /** Sandbox id. */
  sandboxId: string;
  /** Sandbox name. */
  name: string;
  /** Lifecycle state when last observed, e.g. `running` or `paused`. */
  status: string;
  /** The image reference the sandbox boots. */
  baseImage: string;
  /** Digest of the booted image. */
  imageDigest: string | undefined;
  vcpuCount: number;
  memSizeMiB: number;
  /** Public hostnames of the exposed ports. */
  endpoints: SandboxEndpoint[];
  /** `https://` URL of the first exposed port, if any. */
  url: string | undefined;
}

export type Sandbox = Resource<"Archil.Sandbox", SandboxProps, SandboxAttributes, never, Providers>;

/**
 * A long-lived Archil sandbox: a Firecracker microVM with disks mounted,
 * public ports, and an outbound network policy that can inject credentials
 * at the boundary. It pauses when idle and resumes on the next connection,
 * keeping its memory.
 *
 * Declare a sandbox at deploy time for a dev box or a preview environment
 * that outlives deploys. Short-lived sandboxes for one user or task are
 * created at run time with {@link CreateSandbox}; act on a declared one with
 * {@link ConnectSandbox}. Run your own program in it with
 * {@link Service}.
 *
 * :::caution
 * Changing the image, size, `env`, `mounts` or timeouts replaces the
 * sandbox, and a new one starts with an empty root filesystem. Keep data on
 * a mounted disk. `ports` and `network` update in place.
 * :::
 * @see https://docs.archil.com/compute/sandboxes/introduction
 *
 * ### Creating a Sandbox
 * **Example:** A dev box with a disk mounted
 * ```typescript
 * export const DevBox = Effect.gen(function* () {
 *   const workspace = yield* Workspace;
 *   return yield* Archil.Sandbox("DevBox", {
 *     baseImage: "node:24-bookworm",
 *     vcpuCount: 2,
 *     memSizeMiB: 4096,
 *     mounts: [{ disk: workspace.diskId, path: "/workspace" }],
 *     ports: [3000],
 *     idleTtlSeconds: 600,
 *   });
 * });
 * ```
 *
 * **Example:** A bigger sandbox that never pauses while idle
 * ```typescript
 * export const Box = Archil.Sandbox("Box", {
 *   vcpuCount: 4,
 *   memSizeMiB: 8192,
 *   idleTtlSeconds: 0,
 * });
 * ```
 *
 * **Example:** Boot a private image
 * ```typescript
 * export const AgentImage = Archil.Image("AgentImage", {
 *   image: "ghcr.io/acme/agent:1",
 *   registry: { server: "ghcr.io", username: "acme", password: Config.Redacted("GHCR_TOKEN") },
 * });
 * export const Agent = Effect.gen(function* () {
 *   const image = yield* AgentImage;
 *   return yield* Archil.Sandbox("Agent", { image: image.imageId });
 * });
 * ```
 *
 * ### Running Services
 * **Example:** An Effect service at a public URL
 * ```typescript
 * export default class Api extends Archil.Service<Api>()(
 *   "Api",
 *   { sandbox: Box, main: import.meta.url, port: 3000 },
 *   Effect.gen(function* () {
 *     return { fetch: Effect.succeed(HttpServerResponse.text("hello")) };
 *   }),
 * ) {}
 * ```
 *
 * ### Ports
 * **Example:** Serve a port and hand out a token for it
 * ```typescript
 * export const Preview = Archil.Sandbox("Preview", { ports: [8080] });
 * export const PreviewToken = Effect.gen(function* () {
 *   const preview = yield* Preview;
 *   return yield* Archil.SandboxPortToken("PreviewToken", {
 *     sandbox: preview.sandboxId,
 *     port: 8080,
 *     ttl: "24h",
 *   });
 * });
 * ```
 *
 * ### Network Policy
 * **Example:** Deny by default, inject a token for one API
 * ```typescript
 * export const Agent = Archil.Sandbox("Agent", {
 *   network: {
 *     egress: {
 *       default: "deny",
 *       allow: [
 *         "registry.npmjs.org",
 *         { target: "api.github.com", headers: { Authorization: Alchemy.Secret("GITHUB_AUTH") } },
 *       ],
 *     },
 *   },
 * });
 * ```
 *
 * ### Using a Sandbox at Run Time
 * **Example:** Run a command and share a port from a Worker
 * ```typescript
 * const preview = yield* Archil.ConnectSandbox(Preview);
 * const result = yield* preview.exec("pnpm build");
 * const fork = yield* preview.fork({ name: "pr-42" });
 * ```
 *
 * @resource
 * @product Sandbox
 */
export const Sandbox = Resource<Sandbox>("Archil.Sandbox");

/**
 * Map a sandbox and its exposed ports to attributes. Archil lists port
 * hostnames separately; sandbox responses don't carry them.
 */
export const toSandboxAttributes = (
  sandbox: archil.Sandbox,
  ports: ReadonlyArray<{ port: number; hostname: string }>,
): SandboxAttributes => {
  const endpoints = ports.map((e) => ({ port: e.port, hostname: e.hostname }));
  return {
    sandboxId: sandbox.sandbox_id,
    name: sandbox.name,
    status: sandbox.status,
    baseImage: sandbox.base_image,
    imageDigest: sandbox.image_digest,
    vcpuCount: sandbox.vcpu_count,
    memSizeMiB: sandbox.mem_size_mib,
    endpoints,
    url: endpoints[0] ? `https://${endpoints[0].hostname}` : undefined,
  };
};

/** Read a sandbox's exposed ports and map it to attributes. */
export const observeSandbox = Effect.fn(function* (sandbox: archil.Sandbox) {
  const ports = yield* archil.listSandboxPorts({ sid: sandbox.sandbox_id }).pipe(
    Effect.map(({ data }) => data.ports ?? []),
    Effect.catchTag("SandboxNotFound", () => Effect.succeed([])),
  );
  return toSandboxAttributes(sandbox, ports);
});

/** The create request for sandbox props. */
export const toCreateSandboxRequest = (
  name: string | undefined,
  props: Omit<SandboxProps, "name">,
): archil.CreateSandboxRequest => ({
  wait: true,
  name,
  image_id: props.image,
  base_image: props.image ? undefined : props.baseImage,
  vcpu_count: props.vcpuCount,
  mem_size_mib: props.memSizeMiB,
  ports: props.ports,
  enable_service_ingress: props.enableServiceIngress,
  env: toEnv(props.env),
  network: props.network?.egress ? { egress: toEgressPolicy(props.network.egress) } : undefined,
  mounts: props.mounts?.map(toSandboxMount),
  max_ttl_seconds: props.maxTtlSeconds,
  idle_ttl_seconds: props.idleTtlSeconds,
  max_concurrent_execs: props.maxConcurrentExecs,
});

/**
 * Delete a sandbox and wait until it is gone. Archil only deletes stopped,
 * exited or failed sandboxes, so a live one is stopped first. Deleting a
 * missing sandbox succeeds.
 */
export const deleteSandbox = Effect.fn(function* (sid: string) {
  const observed = yield* getSandbox(sid);
  if (observed === undefined) return;
  if (!deletableSandboxStates.has(observed.status)) {
    // Let a booting or pausing sandbox settle, then stop it.
    yield* waitForSandbox(sid).pipe(Effect.catchTag("SandboxNotFound", () => Effect.undefined));
    yield* archil.stopSandbox({ sid }).pipe(
      Effect.asVoid,
      Effect.catchTag("SandboxNotFound", () => Effect.void),
    );
    yield* waitForSandboxStopped(sid);
  }
  yield* archil.deleteSandbox({ sid }).pipe(
    Effect.asVoid,
    Effect.catchTag("SandboxNotFound", () => Effect.void),
  );
  yield* waitForSandboxGone(sid);
});

/** Converge a sandbox's exposed ports. */
const syncPorts = Effect.fn(function* (sid: string, desired: number[] | undefined) {
  if (desired === undefined) return;
  const { data } = yield* archil.listSandboxPorts({ sid });
  const observed = new Set((data.ports ?? []).map((p) => p.port));
  const want = new Set(desired);
  for (const port of want) {
    if (!observed.has(port)) yield* archil.exposeSandboxPort({ sid, port });
  }
  for (const port of observed) {
    if (!want.has(port)) yield* archil.deleteSandboxPort({ sid, port });
  }
});

/** Converge a sandbox's egress policy. */
const syncNetwork = Effect.fn(function* (sid: string, egress: SandboxEgress | undefined) {
  if (egress === undefined) return;
  const desired = toEgressPolicy(egress);
  const { data } = yield* archil.getSandboxNetwork({ sid });
  const { managed, policy } = splitArchilEgress(data.egress);
  if (!deepEqual(policy, desired)) {
    yield* archil.updateSandboxNetwork({
      sid,
      egress: { ...desired, allow: [...(desired.allow ?? []), ...managed] },
    });
  }
});

/** Props that can't change on a running sandbox. */
const createOnly = (props: SandboxProps | undefined) => ({
  image: props?.image,
  baseImage: props?.baseImage,
  vcpuCount: props?.vcpuCount,
  memSizeMiB: props?.memSizeMiB,
  enableServiceIngress: props?.enableServiceIngress,
  env: toEnv(props?.env),
  mounts: props?.mounts,
  maxTtlSeconds: props?.maxTtlSeconds,
  maxConcurrentExecs: props?.maxConcurrentExecs,
});

export const SandboxProvider = () =>
  Provider.effect(
    Sandbox,
    Effect.gen(function* () {
      return {
        stables: ["sandboxId", "name"],
        nuke: { dependsOn: ["Archil.Disk"] },
        list: Effect.fn(function* () {
          return (yield* listSandboxes())
            .filter((s) => isAlchemyName(s.name) && s.status !== "deleted")
            .map((sandbox) => toSandboxAttributes(sandbox, []));
        }),
        diff: Effect.fn(function* ({ id, olds, news, output }) {
          if (!isResolved(news)) return undefined;
          const name = output?.name ?? (yield* sandboxName(id, olds?.name));
          if (news.name !== undefined && news.name !== name) {
            return { action: "replace" } as const;
          }
          if (olds !== undefined && !deepEqual(createOnly(news), createOnly(olds))) {
            // A disk mounted for exclusive writes can only be held by one
            // sandbox, so the old one has to let go before the new one boots.
            const held = new Set(
              (olds.mounts ?? []).filter((m) => !m.readOnly && !m.conditional).map((m) => m.disk),
            );
            const deleteFirst = (news.mounts ?? []).some((m) => held.has(m.disk));
            return { action: "replace", deleteFirst } as const;
          }
          if (
            !deepEqual(egressIdentity(news.network?.egress), egressIdentity(olds?.network?.egress))
          ) {
            return { action: "update" } as const;
          }
          return undefined;
        }),
        read: Effect.fn(function* ({ id, olds, output }) {
          const name = output?.name ?? (yield* sandboxName(id, olds?.name));
          const sandbox =
            (output ? yield* getSandbox(output.sandboxId) : undefined) ??
            (yield* findSandboxByName(name));
          if (!sandbox) return undefined;
          const attrs = yield* observeSandbox(sandbox);
          if (output === undefined && olds?.name !== undefined && !isAlchemyName(sandbox.name)) {
            return Unowned(attrs);
          }
          return attrs;
        }),
        reconcile: Effect.fn(function* ({ id, news, output, session }) {
          const name = output?.name ?? (yield* sandboxName(id, news.name));
          // Observe
          let sandbox =
            (output ? yield* getSandbox(output.sandboxId) : undefined) ??
            (yield* findSandboxByName(name));
          // Ensure
          if (!sandbox) {
            yield* session.note(`Creating sandbox ${name}...`);
            sandbox = yield* archil.createSandbox(toCreateSandboxRequest(name, news)).pipe(
              Effect.map(({ data }) => data),
              Effect.catchTag("SandboxAlreadyExists", () =>
                findSandboxByName(name).pipe(
                  Effect.flatMap((found) =>
                    found
                      ? Effect.succeed(found)
                      : Effect.fail(
                          new archil.SandboxAlreadyExists({
                            error: `Sandbox ${name} exists but is not visible to this key`,
                          }),
                        ),
                  ),
                ),
              ),
            );
          }
          const sid = sandbox.sandbox_id;
          sandbox = yield* waitForSandbox(sid);
          // Sync
          yield* syncNetwork(sid, news.network?.egress);
          yield* syncPorts(sid, news.ports);
          if (
            news.idleTtlSeconds !== undefined &&
            news.idleTtlSeconds !== sandbox.idle_ttl_seconds
          ) {
            yield* archil.setSandboxTimeout({ sid, idle_ttl_seconds: news.idleTtlSeconds });
          }
          // Return
          const final = yield* getSandbox(sid);
          return yield* observeSandbox(final ?? sandbox);
        }),
        delete: Effect.fn(function* ({ output }) {
          yield* deleteSandbox(output.sandboxId);
        }),
      };
    }),
  );
