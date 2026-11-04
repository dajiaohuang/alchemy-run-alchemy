import * as sandboxes from "@distilled.cloud/daytona/sandbox";
import * as secrets from "@distilled.cloud/daytona/secret";
import * as snapshots from "@distilled.cloud/daytona/snapshots";
import * as volumes from "@distilled.cloud/daytona/volumes";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Shared from "alchemy/RuntimeObjects";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { RUNTIME_OWNER_LABEL } from "./internal/Labels.ts";
import type { Providers } from "./Providers.ts";
import { deleteSandboxById } from "./Sandbox.ts";
import { deleteSnapshot } from "./Snapshot.ts";
import { deleteVolumeById } from "./Volume.ts";

/** The kinds of object a runtime binding can create. */
export type RuntimeObjectKind = "sandbox" | "volume" | "secret" | "snapshot";

/** What happens to objects a binding created at runtime when the stack is destroyed. */
export type OnDestroy = Shared.OnDestroy;

export interface RuntimeObjectsProps extends Shared.RuntimeObjectsProps<RuntimeObjectKind> {
  /**
   * Volumes and snapshots carry no labels, so they are swept by name: only
   * those whose name starts with this prefix, which the binding's user
   * chooses (`CreateVolume({ prefix: "user-" })`). Required to sweep them.
   */
  namePrefix?: string;
}

export interface RuntimeObjectsAttributes extends Shared.RuntimeObjectsAttributes<RuntimeObjectKind> {
  /** Name prefix for volumes and snapshots. */
  namePrefix: string | undefined;
}

export type RuntimeObjects = Resource<
  "Daytona.RuntimeObjects",
  RuntimeObjectsProps,
  RuntimeObjectsAttributes,
  never,
  Providers
>;

/**
 * Bookkeeping for the objects a runtime binding creates (sandboxes per
 * request, volumes and secrets per user, snapshots of running sandboxes).
 * They are not in alchemy state, so the binding declares this resource:
 * it carries the owner stamp the binding writes on everything it creates,
 * and, with `onDestroy: "delete"`, deletes those objects when the stack is
 * destroyed.
 *
 * Sandboxes are stamped with an `alchemy.owner` label and secrets with a
 * description marker, both the stamp's `owner`. Volumes and snapshots have
 * neither, so they are swept by the name prefix the binding's user chose.
 *
 * Bindings declare this for you; you don't use it directly.
 * @internal
 */
export const RuntimeObjects = Resource<RuntimeObjects>("Daytona.RuntimeObjects");

/** The label stamped on sandboxes a binding creates. */
export const ownerLabels = (owner: string) => ({ [RUNTIME_OWNER_LABEL]: owner });

/** The marker appended to the description of secrets a binding creates. */
export const descriptionMarker = (owner: string) => `[alchemy:${owner}]`;

const sweepKind = Effect.fn(function* (attrs: RuntimeObjectsAttributes, kind: RuntimeObjectKind) {
  switch (kind) {
    case "sandbox": {
      const owned = yield* sandboxes.listSandboxes
        .items({ labels: JSON.stringify({ [RUNTIME_OWNER_LABEL]: attrs.owner }) })
        .pipe(
          Stream.filter((s) => s.labels?.[RUNTIME_OWNER_LABEL] === attrs.owner),
          Stream.runCollect,
        );
      yield* Effect.forEach(Array.from(owned), (s) => deleteSandboxById(s.id), { concurrency: 8 });
      return;
    }
    case "secret": {
      const owned = yield* secrets.listSecretsPaginated.items({}).pipe(
        Stream.filter((s) => (s.description ?? "").includes(descriptionMarker(attrs.owner))),
        Stream.runCollect,
      );
      yield* Effect.forEach(
        Array.from(owned),
        (s) =>
          secrets
            .deleteSecret({ secretId: s.id })
            .pipe(Effect.catchTag("SecretNotFound", () => Effect.void)),
        { concurrency: 8 },
      );
      return;
    }
    case "volume": {
      const prefix = attrs.namePrefix;
      if (prefix === undefined) return;
      const all = yield* volumes.listVolumes({});
      yield* Effect.forEach(
        all.filter((v) => v.name.startsWith(prefix) && v.state !== "deleted"),
        (v) => deleteVolumeById(v.id),
        { concurrency: 8 },
      );
      return;
    }
    case "snapshot": {
      const prefix = attrs.namePrefix;
      if (prefix === undefined) return;
      const owned = yield* snapshots.getAllSnapshots.items({ name: prefix }).pipe(
        Stream.filter((s) => s.name.startsWith(prefix) && !s.general),
        Stream.runCollect,
      );
      yield* Effect.forEach(Array.from(owned), (s) => deleteSnapshot(s.id), { concurrency: 8 });
      return;
    }
  }
});

const sweep = (attrs: RuntimeObjectsAttributes) =>
  Effect.forEach(attrs.kinds, (kind) => sweepKind(attrs, kind), { discard: true });

/**
 * The shared stamp, plus the name prefix. `alchemy/RuntimeObjects`'
 * `lifecycle` keeps only the shared attributes, and volumes and snapshots
 * are swept by the user's prefix, so the lifecycle is written out here.
 */
export const RuntimeObjectsProvider = () =>
  Provider.effect(
    RuntimeObjects,
    Effect.succeed({
      stables: ["tags", "owner", "prefix"],
      nuke: { skip: true },
      list: () => Effect.succeed([]),
      reconcile: Effect.fn(function* ({ id, news, output }) {
        return { ...(yield* Shared.stamp(id, news, output)), namePrefix: news.namePrefix };
      }),
      delete: Effect.fn(function* ({ output }) {
        if (output.onDestroy !== "retain") yield* sweep(output);
      }),
    }),
  );
