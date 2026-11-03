import * as archil from "@distilled.cloud/archil";
import { isResolved } from "alchemy/Diff";
import { createPhysicalName } from "alchemy/PhysicalName";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Effect from "effect/Effect";
import { deleteDisk } from "./Disk.ts";
import { listSandboxes, toDnsLabel } from "./internal/archil.ts";
import type { Providers } from "./Providers.ts";
import { deleteSandbox } from "./Sandbox.ts";

/** What happens to objects created at run time when the stack is destroyed. */
export type OnDestroy = "delete" | "retain";

export interface RuntimeObjectsProps {
  /** Kinds of object the run-time client creates. */
  kinds: ReadonlyArray<"disk" | "sandbox">;
  /**
   * `"delete"` removes every object the run-time client created when this
   * resource is destroyed; `"retain"` leaves them (e.g. user data in prod).
   * @default "delete"
   */
  onDestroy?: OnDestroy;
}

export interface RuntimeObjectsAttributes {
  /**
   * Name prefix stamped on every object the run-time client creates
   * (`<prefix>-<name>`). Lowercase, so it is valid for sandboxes too.
   */
  prefix: string;
  kinds: ReadonlyArray<"disk" | "sandbox">;
  onDestroy: OnDestroy;
}

export type RuntimeObjects = Resource<
  "Archil.RuntimeObjects",
  RuntimeObjectsProps,
  RuntimeObjectsAttributes,
  never,
  Providers
>;

/**
 * The disks and sandboxes a run-time client creates on behalf of one
 * binding. Archil has no tags, so ownership is a name prefix: the client
 * stamps it on everything it creates, and destroying this resource deletes
 * whatever carries it (unless `onDestroy` is `"retain"`). The `Create…`
 * bindings declare one for you; you never declare it yourself.
 *
 * @resource
 * @product Disk
 */
export const RuntimeObjects = Resource<RuntimeObjects>("Archil.RuntimeObjects");

/** The full name a run-time object gets. */
export const runtimeName = (prefix: string, name: string) => `${prefix}-${name}`;

/** Delete every run-time object with a prefix: sandboxes first, then disks. */
export const sweep = Effect.fn(function* (
  prefix: string,
  kinds: ReadonlyArray<"disk" | "sandbox">,
) {
  const owned = (name: string | undefined) => name?.startsWith(`${prefix}-`) ?? false;
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
    const { data } = yield* archil.listDisks({ limit: 1000 });
    const disks = data.filter((d) => owned(d.name) && d.status !== "deleted");
    yield* Effect.forEach(disks, (d) => deleteDisk(d.id), { concurrency: 4 });
  }
});

export const RuntimeObjectsProvider = () =>
  Provider.effect(
    RuntimeObjects,
    Effect.gen(function* () {
      return {
        stables: ["prefix"],
        diff: Effect.fn(function* ({ news }) {
          if (!isResolved(news)) return undefined;
          return undefined;
        }),
        read: Effect.fn(function* ({ output }) {
          return output;
        }),
        reconcile: Effect.fn(function* ({ id, news, output }) {
          const prefix =
            output?.prefix ??
            toDnsLabel(
              yield* createPhysicalName({
                id,
                maxLength: 30,
                suffixLength: 8,
                delimiter: "-",
                lowercase: true,
              }),
            );
          return { prefix, kinds: news.kinds, onDestroy: news.onDestroy ?? "delete" };
        }),
        delete: Effect.fn(function* ({ output }) {
          if (output.onDestroy === "retain") return;
          yield* sweep(output.prefix, output.kinds);
        }),
      };
    }),
  );
