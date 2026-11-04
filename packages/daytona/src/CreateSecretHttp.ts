import * as secrets from "@distilled.cloud/daytona/secret";
import * as Binding from "alchemy/Binding";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { CreateSecret, type CreateSecretClient, type CreateSecretOptions } from "./CreateSecret.ts";
import { makeAccess } from "./DaytonaBindings.ts";
import { descriptionMarker, RuntimeObjects } from "./RuntimeObjects.ts";
import { ensureSecret, findSecretByName, toSecretAttributes } from "./Secret.ts";

/** {@link CreateSecret} over Daytona's HTTP API; works on any host. */
export const CreateSecretHttp = Layer.effect(
  CreateSecret,
  Effect.gen(function* () {
    const access = yield* makeAccess;
    const Objects = yield* RuntimeObjects;
    return Effect.fn(function* (options?: CreateSecretOptions) {
      const host = yield* Binding.Host;
      const { authorize } = yield* access("Secrets", ["manage:secrets"]);
      const objects = yield* Objects(`${host?.LogicalId ?? ""}SecretsObjects`, {
        kinds: ["secret"],
        onDestroy: options?.onDestroy ?? "delete",
      });
      const marker = Effect.map(yield* objects.owner, descriptionMarker);
      const client: CreateSecretClient = {
        put: (input) =>
          authorize(
            Effect.gen(function* () {
              const mark = yield* marker;
              return yield* ensureSecret({
                name: input.name,
                value: input.value,
                description: input.description ? `${input.description} ${mark}` : mark,
                hosts: input.hosts,
              });
            }),
          ),
        get: (name) =>
          authorize(
            findSecretByName(name).pipe(
              Effect.map((s) => (s ? toSecretAttributes(s, "") : undefined)),
            ),
          ),
        list: () =>
          authorize(
            secrets.listSecretsPaginated.items({}).pipe(
              Stream.runCollect,
              Effect.map((all) => Array.from(all, (s) => toSecretAttributes(s, ""))),
            ),
          ),
        delete: (name) =>
          authorize(
            Effect.gen(function* () {
              const found = yield* findSecretByName(name);
              if (found === undefined) return;
              yield* secrets
                .deleteSecret({ secretId: found.id })
                .pipe(Effect.catchTag("SecretNotFound", () => Effect.void));
            }),
          ),
      };
      return client;
    });
  }),
);
