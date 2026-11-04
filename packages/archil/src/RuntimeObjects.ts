import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Shared from "alchemy/RuntimeObjects";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { deleteDisk } from "./Disk.ts";
import { listAllDisks, listSandboxes } from "./internal/archil.ts";
import type { Providers } from "./Providers.ts";
import { deleteSandbox } from "./Sandbox.ts";

export { runtimeName, type OnDestroy } from "alchemy/RuntimeObjects";

/** The kinds of object Archil's run-time clients create. */
export type RuntimeObjectKind = "disk" | "sandbox";

export type RuntimeObjectsProps = Shared.RuntimeObjectsProps<RuntimeObjectKind>;
export type RuntimeObjectsAttributes = Shared.RuntimeObjectsAttributes<RuntimeObjectKind>;

export type RuntimeObjects = Resource<
  "Archil.RuntimeObjects",
  RuntimeObjectsProps,
  RuntimeObjectsAttributes,
  never,
  Providers
>;

/**
 * The disks and sandboxes a run-time client creates on behalf of one
 * binding. Archil has no tags, so ownership is the stamp's name prefix:
 * the client puts it on everything it creates, and destroying this
 * resource deletes whatever carries it (unless `onDestroy` is
 * `"retain"`). The `Create…` bindings declare one for you; you never
 * declare it yourself.
 *
 * @resource
 * @product Disk
 */
export const RuntimeObjects = Resource<RuntimeObjects>("Archil.RuntimeObjects");

/** Delete every run-time object with a prefix: sandboxes first, then disks. */
export const sweep = Effect.fn(function* (prefix: string, kinds: ReadonlyArray<RuntimeObjectKind>) {
  const owned = (name: string | undefined) => Shared.isRuntimeName(prefix, name);
  if (kinds.includes("sandbox")) {
    const sandboxes = (yield* listSandboxes()).filter(
      (s) => owned(s.name) && s.status !== "deleted",
    );
    // A sandbox with forks can't be deleted until they are, so whatever
    // conflicts is retried once its forks are gone.
    let pending = sandboxes.map((s) => s.sandbox_id);
    for (let round = 0; pending.length > 0 && round < 5; round++) {
      const conflicted = yield* Effect.forEach(
        pending,
        (sid) =>
          deleteSandbox(sid).pipe(
            Effect.as(undefined),
            Effect.catchTag("SandboxStateConflict", () => Effect.succeed(sid)),
          ),
        { concurrency: 4 },
      );
      const next = conflicted.filter((sid) => sid !== undefined);
      if (next.length === pending.length) {
        yield* Effect.forEach(next, (sid) => deleteSandbox(sid), { discard: true });
      }
      pending = next;
    }
  }
  if (kinds.includes("disk")) {
    const all = yield* Stream.runCollect(listAllDisks());
    const disks = all.filter((d) => owned(d.name) && d.status !== "deleted");
    yield* Effect.forEach(disks, (d) => deleteDisk(d.id), { concurrency: 4 });
  }
});

// `Shared.lifecycle` would be this provider, but its `stables` is a
// readonly tuple that `Provider.effect` rejects (it wants a mutable
// array), so the provider is spelled out over the shared `stamp`.
export const RuntimeObjectsProvider = () =>
  Provider.effect(
    RuntimeObjects,
    Effect.succeed({
      stables: ["tags", "owner", "prefix"],
      nuke: { skip: true },
      list: () => Effect.succeed([]),
      read: Effect.fn(function* ({ output }) {
        return output;
      }),
      reconcile: Effect.fn(function* ({ id, news, output }) {
        return yield* Shared.stamp(id, news, output);
      }),
      delete: Effect.fn(function* ({ output }) {
        if (output.onDestroy === "retain") return;
        yield* sweep(output.prefix, output.kinds);
      }),
    }),
  );
