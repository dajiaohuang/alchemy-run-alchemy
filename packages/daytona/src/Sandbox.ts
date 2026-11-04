import * as sandboxes from "@distilled.cloud/daytona/sandbox";
import { Unowned } from "alchemy/AdoptPolicy";
import { deepEqual, isResolved } from "alchemy/Diff";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import {
  ALCHEMY_LABEL_PREFIX,
  createInternalLabels,
  definedLabels,
  hasAlchemyLabels,
  userLabels,
} from "./internal/Labels.ts";
import { resolveName } from "./internal/Names.ts";
import { resolved, type Ref } from "./internal/Ref.ts";
import { toMinutes, waitForState, waitUntilGone } from "./internal/Wait.ts";
import type { Providers } from "./Providers.ts";

/** A snapshot, by name. Accepts a `Daytona.Snapshot` or `{ name }`. */
export type SandboxSnapshot = { readonly name: string };
/** A volume, by id. Accepts a `Daytona.Volume` or `{ volumeId }`. */
export type SandboxVolumeRef = { readonly volumeId: string };
/** A secret, by name. Accepts a `Daytona.Secret` or `{ name }`. */
export type SandboxSecretRef = { readonly name: string };

export interface SandboxVolumeMount {
  /** The volume to mount. */
  volume: Ref<SandboxVolumeRef>;
  /** Absolute path in the sandbox. */
  mountPath: string;
  /** Mount only this directory of the volume. */
  subpath?: string;
}

/** Another sandbox, by id. Accepts a `Daytona.Sandbox` or `{ sandboxId }`. */
export type SandboxLinkRef = { readonly sandboxId: string };

/** Whether the sandbox should be running, stopped (disk kept) or archived (moved to object storage). */
export type SandboxRunState = "started" | "stopped" | "archived";

export interface SandboxProps {
  /**
   * Sandbox name, unique in the organization. If omitted, a unique name is
   * generated from the stack, stage and logical ID. Changing it replaces
   * the sandbox.
   */
  name?: string;
  /**
   * Snapshot to boot from. Defaults to Daytona's default snapshot. The
   * snapshot sets the sandbox's CPU, memory, disk and GPUs: declare a
   * `Daytona.Snapshot` with the size you want. Changing it replaces the
   * sandbox.
   */
  snapshot?: Ref<SandboxSnapshot>;
  /** OS user commands run as. Changing it replaces the sandbox. */
  user?: string;
  /** Environment variables. Changing them replaces the sandbox. */
  env?: Record<string, string>;
  /**
   * Secrets exposed as environment variables, keyed by variable name. The
   * sandbox sees each secret's placeholder; Daytona swaps in the value only
   * on requests to the secret's `hosts`. Updated in place.
   */
  secrets?: Record<string, Ref<SandboxSecretRef>>;
  /** Labels. Updated in place. */
  labels?: Record<string, string>;
  /** Whether preview URLs work without a token. Updated in place. @default false */
  public?: boolean;
  /** Block all outbound traffic. Updated in place. @default false */
  networkBlockAll?: boolean;
  /** CIDR ranges outbound traffic may reach (at most 10). Updated in place. */
  networkAllowList?: string[];
  /** Domains outbound traffic may reach (at most 100). Updated in place. */
  domainAllowList?: string[];
  /** Route outbound traffic through this proxy. Changing it replaces the sandbox. */
  outboundProxyUrl?: string;
  /** Region. Changing it replaces the sandbox. */
  target?: string;
  /**
   * Run a GPU sandbox on spot capacity: cheaper, but Daytona can end it
   * without notice. Changing it replaces the sandbox.
   */
  spot?: boolean;
  /** Expose `/dev/kvm` (VM sandboxes). Changing it replaces the sandbox. */
  kvm?: boolean;
  /** Volumes to mount. Changing them replaces the sandbox. */
  volumes?: SandboxVolumeMount[];
  /** Stop after this long without activity. `0` never stops. Updated in place. @default "15 minutes" */
  autoStopInterval?: Duration.Input;
  /** Pause (memory kept) after this long without activity. Updated in place. */
  autoPauseInterval?: Duration.Input;
  /** Archive after being stopped this long. Updated in place. @default "7 days" */
  autoArchiveInterval?: Duration.Input;
  /** Delete after being stopped this long. Updated in place. */
  autoDeleteInterval?: Duration.Input;
  /**
   * Destroy the sandbox this long after it is created (or after the deploy
   * that last changed it), whatever its state. Changing it restarts the
   * clock from now. `0` disables it, unless the region caps sandbox lifespan.
   */
  ttl?: Duration.Input;
  /**
   * Give up if no runner takes the sandbox within this long. Daytona honors
   * it only when the organization has a default queue timeout. Only used
   * when the sandbox is created.
   */
  queueTimeout?: Duration.Input;
  /**
   * Schedule the sandbox on the same runner as this one, so the two share
   * a local network. Both must be ephemeral (`autoDeleteInterval: 0`), and
   * neither can have GPUs. Changing it replaces the sandbox.
   */
  linkedSandbox?: Ref<SandboxLinkRef>;
  /**
   * Send the sandbox's OpenTelemetry data here instead of Daytona's
   * collector. It then doesn't show in Daytona's dashboard or the
   * telemetry API. Changing it replaces the sandbox.
   */
  otelEndpointOverride?: string;
  /** Whether the sandbox should be running. Updated in place. @default "started" */
  state?: SandboxRunState;
}

export interface SandboxAttributes {
  /** Sandbox ID. */
  sandboxId: string;
  /** Sandbox name. */
  name: string;
  /** Organization ID. */
  organizationId: string;
  /** Snapshot it booted from. */
  snapshot: string | undefined;
  /** Current state. */
  state: sandboxes.SandboxState | undefined;
  /** Region. */
  target: string;
  /** vCPUs. */
  cpu: number;
  /** Memory in GiB. */
  memory: number;
  /** Disk in GiB. */
  disk: number;
  /** GPUs. */
  gpu: number;
  /** Sandbox class: `container`, `linux-vm` or `windows`, set by the snapshot. */
  sandboxClass: sandboxes.SandboxSandboxClass | undefined;
  /** When the TTL destroys the sandbox, if one is set. */
  autoDestroyAt: string | undefined;
  /** ID of the sandbox it shares a runner with. */
  linkedSandboxId: string | undefined;
  /** Labels you set (alchemy's ownership labels excluded). */
  labels: Record<string, string>;
  /** Whether previews are public. */
  public: boolean;
  /** Base URL of the sandbox's toolbox proxy. */
  toolboxProxyUrl: string;
}

export type Sandbox = Resource<
  "Daytona.Sandbox",
  SandboxProps,
  SandboxAttributes,
  never,
  Providers
>;

/**
 * A long-lived Daytona sandbox: an isolated machine with its own
 * filesystem, processes and network, for a dev box, a preview environment
 * or a persistent agent workspace. Its disk survives stops; Daytona stops
 * it when idle and archives it later, and deploy starts it again.
 *
 * For sandboxes created per request or per task, use
 * {@link CreateSandbox} at runtime instead; for one declared here, use
 * {@link ConnectSandbox} to run commands in it from a Worker.
 * @see https://www.daytona.io/docs/en/sandboxes/
 *
 * ### Creating a Sandbox
 * **Example:** A dev box from a snapshot
 * ```typescript
 * export const DevBox = Daytona.Sandbox("DevBox", {
 *   snapshot: Base,
 *   autoStopInterval: "30 minutes",
 * });
 * ```
 *
 * ### Restricting the Network
 * **Example:** Only GitHub and npm are reachable
 * ```typescript
 * export const Locked = Daytona.Sandbox("Locked", {
 *   snapshot: Base,
 *   domainAllowList: ["github.com", "registry.npmjs.org"],
 * });
 * ```
 *
 * ### Volumes and Secrets
 * **Example:** A shared cache volume and an npm token only sent to npm
 * ```typescript
 * export const Builder = Daytona.Sandbox("Builder", {
 *   snapshot: Base,
 *   volumes: [{ volume: Cache, mountPath: "/cache" }],
 *   secrets: { NPM_TOKEN: NpmToken },
 * });
 * ```
 *
 * ### GPUs
 * A GPU sandbox boots from a GPU snapshot, such as Daytona's `daytona-gpu`
 * (one H100) in its `earth` region, and must be ephemeral. The
 * organization needs GPU credits in its wallet.
 *
 * **Example:** An ephemeral H100 sandbox on spot capacity
 * ```typescript
 * export const Trainer = Daytona.Sandbox("Trainer", {
 *   snapshot: { name: "daytona-gpu" },
 *   target: "earth",
 *   autoDeleteInterval: 0,
 *   spot: true,
 * });
 * ```
 *
 * ### Limiting Its Lifetime
 * **Example:** Destroyed two hours after it is created
 * ```typescript
 * export const Scratch = Daytona.Sandbox("Scratch", { ttl: "2 hours" });
 * ```
 *
 * ### Sharing a Runner
 * **Example:** A database and an app on one local network
 * ```typescript
 * export const Db = Daytona.Sandbox("Db", { autoDeleteInterval: 0 });
 * export const App = Daytona.Sandbox("App", {
 *   autoDeleteInterval: 0,
 *   linkedSandbox: Db,
 * });
 * ```
 *
 * @resource
 * @product Sandbox
 */
export const Sandbox = Resource<Sandbox>("Daytona.Sandbox");

const resolveRef = <T>(ref: Ref<T>) => Effect.succeed(resolved(ref));

const csv = (list: string[] | undefined) => (list === undefined ? undefined : list.join(","));
const fromCsv = (value: string | undefined) =>
  value === undefined || value === ""
    ? []
    : value
        .split(",")
        .map((s) => s.trim())
        .sort();

/** What both `getSandbox` and the sandbox list return. */
export type SandboxInfo = Omit<sandboxes.Sandbox2, "env" | "kvm">;

export const toSandboxAttributes = (s: SandboxInfo): SandboxAttributes => ({
  sandboxId: s.id,
  name: s.name,
  organizationId: s.organizationId,
  snapshot: s.snapshot,
  state: s.state,
  target: s.target,
  cpu: s.cpu,
  memory: s.memory,
  disk: s.disk,
  gpu: s.gpu,
  sandboxClass: s.sandboxClass,
  autoDestroyAt: s.autoDestroyAt,
  linkedSandboxId: s.linkedSandboxId,
  labels: userLabels(s.labels),
  public: s.public,
  toolboxProxyUrl: s.toolboxProxyUrl,
});

const isGone = (s: SandboxInfo) => s.state === "destroyed" || s.state === "destroying";

export const getSandbox = (idOrName: string) =>
  sandboxes.getSandbox({ sandboxIdOrName: idOrName }).pipe(
    Effect.map((s) => (isGone(s) ? undefined : s)),
    Effect.catchTag("SandboxNotFound", () => Effect.undefined),
  );

const TRANSITIONS: Partial<Record<string, readonly string[]>> = {
  started: ["started"],
  stopped: ["stopped"],
  archived: ["archived"],
};

export const waitForSandbox = (idOrName: string, want: SandboxRunState | "paused" = "started") =>
  waitForState({
    kind: "sandbox",
    id: idOrName,
    observe: sandboxes.getSandbox({ sandboxIdOrName: idOrName }),
    want: TRANSITIONS[want] ?? [want],
    fail: ["error", "build_failed", "destroyed"],
    reason: (s) => s.errorReason,
    budget: "10 minutes",
  });

/** Move a sandbox to the run state it should be in. */
export const syncRunState = Effect.fn(function* (
  current: sandboxes.Sandbox2,
  want: SandboxRunState,
) {
  const id = current.id;
  let state = current.state;
  // Let an in-flight transition land first.
  if (state && !["started", "stopped", "archived", "paused", "error"].includes(state)) {
    state = (yield* waitForState({
      kind: "sandbox",
      id,
      observe: sandboxes.getSandbox({ sandboxIdOrName: id }),
      want: ["started", "stopped", "archived", "paused", "error"],
      budget: "10 minutes",
    })).state;
  }
  if (state === "error") {
    if (current.recoverable) {
      yield* sandboxes.recoverSandbox({ sandboxIdOrName: id });
      state = (yield* waitForSandbox(id, "started")).state;
    } else {
      return yield* waitForSandbox(id, "started");
    }
  }
  if (state === want) return yield* sandboxes.getSandbox({ sandboxIdOrName: id });
  if (want === "started") {
    yield* sandboxes.startSandbox({ sandboxIdOrName: id });
    return yield* waitForSandbox(id, "started");
  }
  if (want === "stopped") {
    if (state === "archived") {
      // An archived sandbox only leaves the archive by starting.
      yield* sandboxes.startSandbox({ sandboxIdOrName: id });
      yield* waitForSandbox(id, "started");
    }
    yield* sandboxes.stopSandbox({ sandboxIdOrName: id });
    return yield* waitForSandbox(id, "stopped");
  }
  if (state === "started" || state === "paused") {
    yield* sandboxes.stopSandbox({ sandboxIdOrName: id });
    yield* waitForSandbox(id, "stopped");
  }
  yield* sandboxes.archiveSandbox({ sandboxIdOrName: id });
  return yield* waitForSandbox(id, "archived");
});

export const deleteSandboxById = Effect.fn(function* (idOrName: string) {
  yield* sandboxes
    .deleteSandbox({ sandboxIdOrName: idOrName })
    .pipe(Effect.catchTag("SandboxNotFound", () => Effect.void));
  yield* waitUntilGone(
    "sandbox",
    idOrName,
    getSandbox(idOrName).pipe(Effect.map((s) => s !== undefined)),
    "5 minutes",
  );
});

const resolveCreate = Effect.fn(function* (
  name: string,
  news: SandboxProps,
  labels: Record<string, string>,
) {
  const snapshot = news.snapshot ? (yield* resolveRef(news.snapshot)).name : undefined;
  const volumes = news.volumes
    ? yield* Effect.forEach(news.volumes, (m) =>
        Effect.map(resolveRef(m.volume), (v) => ({
          volumeId: v.volumeId,
          mountPath: m.mountPath,
          subpath: m.subpath,
        })),
      )
    : undefined;
  const secrets = news.secrets
    ? yield* Effect.forEach(Object.entries(news.secrets), ([env, ref]) =>
        Effect.map(resolveRef(ref), (s) => ({ [env]: s.name })),
      )
    : undefined;
  const linkedSandbox = news.linkedSandbox
    ? (yield* resolveRef(news.linkedSandbox)).sandboxId
    : undefined;
  return {
    name,
    snapshot,
    linkedSandbox,
    otelEndpointOverride: news.otelEndpointOverride,
    ttlMinutes: toMinutes(news.ttl),
    queueTimeout: toMinutes(news.queueTimeout),
    user: news.user,
    env: news.env,
    labels,
    public: news.public,
    networkBlockAll: news.networkBlockAll,
    networkAllowList: csv(news.networkAllowList),
    domainAllowList: csv(news.domainAllowList),
    outboundProxyUrl: news.outboundProxyUrl,
    target: news.target,
    spot: news.spot,
    kvm: news.kvm,
    volumes,
    secrets,
    autoStopInterval: toMinutes(news.autoStopInterval),
    autoPauseInterval: toMinutes(news.autoPauseInterval),
    autoArchiveInterval: toMinutes(news.autoArchiveInterval),
    autoDeleteInterval: toMinutes(news.autoDeleteInterval),
  } satisfies sandboxes.CreateSandboxRequest;
});

export const SandboxProvider = () =>
  Provider.effect(
    Sandbox,
    Effect.succeed({
      stables: ["sandboxId", "organizationId", "toolboxProxyUrl"],
      list: Effect.fn(function* () {
        const all = yield* sandboxes.listSandboxes.items({}).pipe(Stream.runCollect);
        return Array.from(all)
          .filter(
            (s) =>
              !isGone(s) &&
              Object.keys(s.labels ?? {}).some((k) => k.startsWith(ALCHEMY_LABEL_PREFIX)),
          )
          .map(toSandboxAttributes);
      }),
      diff: Effect.fn(function* ({ id, olds, news, output }) {
        if (!isResolved(news) || output === undefined) return undefined;
        const name = yield* resolveName(
          id,
          news.name,
          news.name === undefined ? output.name : undefined,
        );
        const snapshot = news.snapshot ? (yield* resolveRef(news.snapshot)).name : undefined;
        const replace =
          name !== output.name ||
          (snapshot !== undefined && snapshot !== output.snapshot) ||
          news.user !== olds?.user ||
          !deepEqual(news.env ?? {}, olds?.env ?? {}) ||
          news.outboundProxyUrl !== olds?.outboundProxyUrl ||
          (news.target !== undefined && news.target !== output.target) ||
          news.spot !== olds?.spot ||
          news.kvm !== olds?.kvm ||
          news.otelEndpointOverride !== olds?.otelEndpointOverride ||
          (news.linkedSandbox !== undefined &&
            (yield* resolveRef(news.linkedSandbox)).sandboxId !== output.linkedSandboxId) ||
          !deepEqual(
            (news.volumes ?? []).map((v) => [v.mountPath, v.subpath]),
            (olds?.volumes ?? []).map((v) => [v.mountPath, v.subpath]),
          );
        if (replace) return { action: "replace", deleteFirst: name === output.name } as const;
      }),
      read: Effect.fn(function* ({ id, olds, output }) {
        const found = yield* getSandbox(
          output?.sandboxId ?? (yield* resolveName(id, olds?.name, output?.name)),
        );
        if (found === undefined) return undefined;
        const attrs = toSandboxAttributes(found);
        return (yield* hasAlchemyLabels(id, found.labels)) ? attrs : Unowned(attrs);
      }),
      reconcile: Effect.fn(function* ({ id, news, olds, output, session }) {
        const name = yield* resolveName(id, news.name, output?.name);
        const labels = { ...(news.labels ?? {}), ...(yield* createInternalLabels(id)) };
        const request = yield* resolveCreate(name, news, labels);

        // Observe
        let current = yield* getSandbox(output?.sandboxId ?? name);
        const created = current === undefined;

        // Ensure
        if (current === undefined) {
          yield* session.note(`Creating sandbox ${name}...`);
          current = yield* sandboxes
            .createSandbox(request)
            .pipe(
              Effect.catchTag("SandboxAlreadyExists", () =>
                sandboxes.getSandbox({ sandboxIdOrName: name }),
              ),
            );
          current = yield* waitForSandbox(current.id, "started");
        }
        const sandboxId = current.id;

        // Sync labels against the observed set.
        if (!deepEqual(definedLabels(current.labels), labels)) {
          yield* sandboxes.replaceLabels({ sandboxIdOrName: sandboxId, labels });
        }
        // Sync network.
        const network = {
          networkBlockAll: news.networkBlockAll ?? false,
          networkAllowList: fromCsv(request.networkAllowList),
          domainAllowList: fromCsv(request.domainAllowList),
        };
        if (
          network.networkBlockAll !== current.networkBlockAll ||
          !deepEqual(network.networkAllowList, fromCsv(current.networkAllowList)) ||
          !deepEqual(network.domainAllowList, fromCsv(current.domainAllowList))
        ) {
          yield* sandboxes.updateNetworkSettings({
            sandboxIdOrName: sandboxId,
            networkBlockAll: network.networkBlockAll,
            networkAllowList: network.networkAllowList.join(","),
            domainAllowList: network.domainAllowList.join(","),
          });
        }
        // Sync public previews.
        if ((news.public ?? false) !== current.public) {
          yield* sandboxes.updatePublicStatus({
            sandboxIdOrName: sandboxId,
            isPublic: news.public ?? false,
          });
        }
        // Sync lifecycle intervals that were declared.
        const intervals = [
          [request.autoStopInterval, current.autoStopInterval, sandboxes.setAutostopInterval],
          [request.autoPauseInterval, current.autoPauseInterval, sandboxes.setAutoPauseInterval],
          [
            request.autoArchiveInterval,
            current.autoArchiveInterval,
            sandboxes.setAutoArchiveInterval,
          ],
          [request.autoDeleteInterval, current.autoDeleteInterval, sandboxes.setAutoDeleteInterval],
        ] as const;
        for (const [want, have, set] of intervals) {
          if (want !== undefined && want !== have) {
            yield* set({ sandboxIdOrName: sandboxId, interval: want });
          }
        }
        // Sync the TTL. Daytona reports only when it fires, and setting it
        // restarts the clock, so it's written when the declared TTL changes
        // (or on adoption).
        if (
          !created &&
          request.ttlMinutes !== undefined &&
          (olds === undefined || request.ttlMinutes !== toMinutes(olds.ttl))
        ) {
          yield* sandboxes.setTtl({ sandboxIdOrName: sandboxId, ttlMinutes: request.ttlMinutes });
        } else if (!created && request.ttlMinutes === undefined && olds?.ttl !== undefined) {
          yield* sandboxes.setTtl({ sandboxIdOrName: sandboxId, ttlMinutes: 0 });
        }
        // Sync secrets. Daytona doesn't report a sandbox's mounted secrets,
        // so the declared set is written whenever it differs from the last
        // deploy (or on adoption, when there is no last deploy).
        if (request.secrets !== undefined || olds?.secrets !== undefined) {
          const previous = olds?.secrets ? yield* resolveCreate(name, olds, labels) : undefined;
          if (olds === undefined || !deepEqual(previous?.secrets ?? [], request.secrets ?? [])) {
            yield* sandboxes.updateSandboxSecrets({
              sandboxIdOrName: sandboxId,
              secrets: request.secrets ?? [],
            });
          }
        }
        // Sync run state last, from a fresh observation.
        const final = yield* syncRunState(
          yield* sandboxes.getSandbox({ sandboxIdOrName: sandboxId }),
          news.state ?? "started",
        );
        return toSandboxAttributes(final);
      }),
      delete: Effect.fn(function* ({ output }) {
        yield* deleteSandboxById(output.sandboxId);
      }),
    }),
  );
