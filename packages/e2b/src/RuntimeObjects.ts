import * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import * as secrets from "@distilled.cloud/e2b/secrets";
import * as Provider from "alchemy/Provider";
import { Resource } from "alchemy/Resource";
import * as Shared from "alchemy/RuntimeObjects";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type { Providers } from "./Providers.ts";

/** What E2B bindings create at runtime and clean up when their host is destroyed. */
export type RuntimeObjectKind = "sandboxes" | "secrets";

/**
 * The metadata key that marks an object a binding created at runtime, as
 * opposed to one the stack declares (an `E2B.Sandbox`, an `E2B.Secret`).
 */
export const RUNTIME_KIND_KEY = "alchemy::runtime";

export interface RuntimeObjects extends Resource<
  "E2B.RuntimeObjects",
  Shared.RuntimeObjectsProps<RuntimeObjectKind>,
  Shared.RuntimeObjectsAttributes<RuntimeObjectKind>,
  never,
  Providers
> {}

/**
 * The sandboxes and secrets a host's bindings create at runtime. Each
 * carries the resource's `tags` in its E2B metadata; destroying the host
 * deletes them (`onDestroy: "retain"` keeps them). `E2B.CreateSandbox` and
 * `E2B.CreateSecret` declare one in their host's namespace, so you rarely
 * declare it yourself.
 *
 * ### Declaring one
 * **Example:** Keep a host's sandboxes when it is destroyed
 * ```typescript
 * const kept = yield* E2B.RuntimeObjects("Kept", { kinds: ["sandboxes"], onDestroy: "retain" });
 * ```
 *
 * @resource
 * @product Sandbox
 */
export const RuntimeObjects = Resource<RuntimeObjects>("E2B.RuntimeObjects");

/** E2B's `metadata` filter: `key=value&…`. */
export const metadataQuery = (metadata: Readonly<Record<string, string>>) =>
  new URLSearchParams(Object.entries(metadata)).toString();

const matches = (
  tags: Readonly<Record<string, string>>,
  metadata: Readonly<Record<string, string | undefined>> | undefined,
) => Object.entries(tags).every(([key, value]) => metadata?.[key] === value);

/** Deletes every sandbox and secret stamped with the resource's tags. */
const sweep = Effect.fn("E2B.RuntimeObjects.sweep")(function* (
  attrs: Shared.RuntimeObjectsAttributes<RuntimeObjectKind>,
) {
  if (Object.keys(attrs.tags).length === 0) return;
  if (attrs.kinds.includes("sandboxes")) {
    const running = Array.from(
      yield* Stream.runCollect(
        sandboxes.listSandboxes.items({
          metadata: metadataQuery(attrs.tags),
          state: ["running", "paused"],
          limit: 100,
        }),
      ),
    );
    yield* Effect.forEach(
      running.filter((sandbox) => matches(attrs.tags, sandbox.metadata)),
      (sandbox) =>
        sandboxes
          .deleteSandbox({ sandboxID: sandbox.sandboxID })
          .pipe(Effect.catchTag("NotFound", () => Effect.void)),
      { concurrency: 8, discard: true },
    );
  }
  if (attrs.kinds.includes("secrets")) {
    const all = Array.from(yield* Stream.runCollect(secrets.listSecrets.items({ limit: 100 })));
    yield* Effect.forEach(
      all.filter((secret) => matches(attrs.tags, secret.metadata)),
      (secret) =>
        secrets
          .deleteSecret({ secretID: secret.secretID })
          .pipe(Effect.catchTag("NotFound", () => Effect.void)),
      { concurrency: 8, discard: true },
    );
  }
});

export const RuntimeObjectsProvider = () =>
  Provider.effect(RuntimeObjects, Effect.succeed(Shared.lifecycle(sweep)));
