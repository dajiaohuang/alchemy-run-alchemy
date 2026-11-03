/**
 * How Archil bindings reach the control plane from any host.
 *
 * Archil only mints API keys from a console session and can't scope them,
 * so a host's control-plane clients use the key the stack deploys with,
 * recorded per host and read back at run time. A host that only mounts
 * disks never holds an API key at all; it gets a per-disk `Archil.DiskUser`
 * instead (see `MountDisk`).
 */
import { controlPlaneUrl, Credentials, DEFAULT_REGION } from "@distilled.cloud/archil/Credentials";
import * as Binding from "alchemy/Binding";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import type * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { DeployerKey } from "./internal/DeployerKey.ts";
import { RuntimeObjects, type OnDestroy } from "./RuntimeObjects.ts";

/** Runs Archil SDK effects and streams with the host's own API key. */
export interface Authorize {
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, Exclude<R, Credentials | HttpClient.HttpClient> | RuntimeContext>;
  stream<A, E, R>(
    stream: Stream.Stream<A, E, R>,
  ): Stream.Stream<A, E, Exclude<R, Credentials | HttpClient.HttpClient> | RuntimeContext>;
}

/**
 * Record (at deploy) and read back (at run time) the API key a host's
 * Archil clients call the control plane with. Every Archil binding on one
 * host shares one record (the engine de-duplicates the declaration).
 */
export const ApiAccess = Effect.gen(function* () {
  const Key = yield* DeployerKey;
  return Effect.fn(function* () {
    const host = yield* Binding.Host;
    const token = yield* Key(`${host?.LogicalId ?? "Script"}ArchilApiKey`, {});
    const apiKey = yield* token.apiKey;
    const region = yield* token.region;
    const credentials = Effect.all([apiKey, region]).pipe(
      Effect.map(([key, r]) => ({
        apiKey: key,
        apiBaseUrl: controlPlaneUrl(r) ?? controlPlaneUrl(DEFAULT_REGION)!,
        region: r,
      })),
    );
    const layer = Layer.mergeAll(Layer.succeed(Credentials, credentials), FetchHttpClient.layer);
    const authorize = Object.assign(
      <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.provide(layer)),
      {
        stream: <A, E, R>(stream: Stream.Stream<A, E, R>) => stream.pipe(Stream.provide(layer)),
      },
      // TypeScript can't infer a generic call signature whose requirement is
      // `Exclude<R, …>` from `Effect.provide`, so the shape is asserted.
    ) as unknown as Authorize;
    return authorize;
  });
});

/**
 * Declare (at deploy) and read back (at run time) the name prefix a
 * binding stamps on the objects its client creates, so destroying the stack
 * can sweep them.
 */
export const RuntimePrefix = Effect.gen(function* () {
  const Objects = yield* RuntimeObjects;
  return Effect.fn(function* (
    label: string,
    kinds: ReadonlyArray<"disk" | "sandbox">,
    onDestroy: OnDestroy | undefined,
  ) {
    const host = yield* Binding.Host;
    const objects = yield* Objects(`${host?.LogicalId ?? ""}${label}Objects`, {
      kinds,
      onDestroy,
    });
    return yield* objects.prefix;
  });
});
