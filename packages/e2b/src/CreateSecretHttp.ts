import * as secrets from "@distilled.cloud/e2b/secrets";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { declareSweep } from "./CreateSandboxHttp.ts";
import {
  CreateSecret,
  type CreateSecretBindingOptions,
  type SecretsClient,
} from "./CreateSecret.ts";
import { E2BUnexpectedResponse } from "./Errors.ts";
import { bindE2B } from "./internal/Bindings.ts";
import { toSecretAttributes } from "./Secret.ts";
import { Sweep } from "./Sweep.ts";

/**
 * HTTP implementation of {@link CreateSecret}. Works on any host.
 *
 * ### Provide the layer
 * **Example:** On a Worker
 * ```typescript
 * Effect.gen(function* () {
 *   const secrets = yield* E2B.CreateSecret();
 *   // ...
 * }).pipe(Effect.provide(E2B.CreateSecretHttp))
 * ```
 *
 * @layer
 * @product Secret
 * @provides E2B.CreateSecret
 */
export const CreateSecretHttp = Layer.effect(
  CreateSecret,
  Effect.gen(function* () {
    // Yielding the resource class erases its `Providers` requirement; the
    // stack satisfies it at plan time.
    const sweep = yield* Sweep;
    return Effect.fn("E2B.CreateSecret")(function* (options?: CreateSecretBindingOptions) {
      const auth = yield* bindE2B;
      yield* declareSweep(sweep, "secrets", auth.deployStamp, options?.onDestroy);

      const get = (nameOrId: string) =>
        auth.authorize(secrets.getSecret({ secretID: nameOrId.toLowerCase() })).pipe(
          Effect.map((secret): secrets.Secret | undefined => secret),
          Effect.catchTag("NotFound", () => Effect.undefined),
        );

      const client: SecretsClient = {
        put: Effect.fn("E2B.CreateSecret.put")(function* (name, value, putOptions) {
          const metadata = { ...putOptions?.metadata, ...(yield* auth.stamp) };
          const existing = yield* get(name);
          const stored =
            existing !== undefined
              ? yield* auth.authorize(
                  secrets.updateSecret({ secretID: existing.secretID, value, metadata }),
                )
              : yield* auth.authorize(secrets.createSecret({ name, value, metadata })).pipe(
                  // A concurrent put created it first: add our version.
                  Effect.catchTag("Conflict", () =>
                    Effect.gen(function* () {
                      const raced = yield* get(name);
                      if (raced === undefined) {
                        return yield* new E2BUnexpectedResponse({
                          message: `E2B secret ${name} vanished during put`,
                        });
                      }
                      return yield* auth.authorize(
                        secrets.updateSecret({ secretID: raced.secretID, value, metadata }),
                      );
                    }),
                  ),
                );
          return toSecretAttributes(stored);
        }),
        get: (nameOrId) =>
          get(nameOrId).pipe(
            Effect.map((secret) => (secret === undefined ? undefined : toSecretAttributes(secret))),
          ),
        list: Effect.fn("E2B.CreateSecret.list")(function* () {
          const stamp = yield* auth.stamp;
          const all = yield* auth.authorize(
            Stream.runCollect(secrets.listSecrets.items({ limit: 100 })),
          );
          return Array.from(all)
            .filter((secret) =>
              Object.entries(stamp).every(([key, value]) => secret.metadata[key] === value),
            )
            .map(toSecretAttributes);
        }),
        delete: (nameOrId) =>
          Effect.flatMap(get(nameOrId), (secret) =>
            secret === undefined
              ? Effect.void
              : auth.authorize(secrets.deleteSecret({ secretID: secret.secretID })).pipe(
                  Effect.asVoid,
                  Effect.catchTag("NotFound", () => Effect.void),
                ),
          ),
      };
      return client;
    });
  }),
);
