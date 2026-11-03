import * as dict from "@distilled.cloud/modal/dict";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { makeOpenModalBinding } from "./BindingHttp.ts";
import { CreateDict, type DictHandle, type DictsClient } from "./CreateDict.ts";
import { ensureDict, lookupDict, type DictAttributes } from "./Dict.ts";
import { DictOps, makeReadWriteDictClient } from "./DictClient.ts";
import type { Environment } from "./Environment.ts";
import { environmentOf, listAll, operations } from "./internal.ts";

/** The operations the client calls, resolved by `ModalBindings.withCredentials`. */
const DictsOps = operations({
  getOrCreate: dict.dictGetOrCreate,
  delete: dict.deleteDict,
  list: dict.listDict,
  client: DictOps,
});

type DictsOps = Effect.Success<typeof DictsOps>;

const handleOf = (attributes: DictAttributes, ops: DictsOps): DictHandle => ({
  ...makeReadWriteDictClient(Effect.succeed(attributes.dictId), ops.client),
  dictId: Effect.succeed(attributes.dictId),
  name: attributes.name,
  attributes,
});

/** A dicts client over an environment name resolved per call. */
export const makeDictsClient = (
  environment: Effect.Effect<string | undefined>,
  ops: DictsOps,
): DictsClient => ({
  getOrCreate: (name) =>
    Effect.flatMap(environment, (env) => ensureDict(ops.getOrCreate, name, env)).pipe(
      Effect.map((attributes) => handleOf(attributes, ops)),
    ),
  get: (name) =>
    Effect.flatMap(environment, (env) => lookupDict(ops.getOrCreate, name, env)).pipe(
      Effect.map((attributes) => (attributes ? handleOf(attributes, ops) : undefined)),
    ),
  delete: (name) =>
    Effect.gen(function* () {
      const env = yield* environment;
      const found = yield* lookupDict(ops.getOrCreate, name, env);
      if (!found) return;
      yield* ops
        .delete({ dictId: found.dictId })
        .pipe(Effect.catchTag("DictNotFound", () => Effect.void));
    }),
  list: () =>
    Effect.gen(function* () {
      const env = yield* environment;
      const items = yield* listAll(
        (pagination) =>
          ops
            .list({ environmentName: environmentOf(env), pagination })
            .pipe(Effect.map((response) => response.dicts ?? [])),
        (item) => item.createdAt,
      );
      return items.flatMap((item) => (item.name ? [item.name] : []));
    }),
});

/**
 * {@link CreateDict} over Modal's API. Grants the host's identity (a scoped
 * `Modal.ServiceUser`) the `contributor` role on the environment.
 *
 * @layer
 * @product Dict
 * @provides Modal.CreateDict
 */
export const CreateDictHttp = Layer.effect(
  CreateDict,
  makeOpenModalBinding({
    tag: "Modal.CreateDict",
    operations: DictsOps,
    role: "contributor",
    environmentOf: (environment?: Environment) => environment?.name,
    make: Effect.fn(function* (ops, environment?: Environment) {
      const name = environment ? yield* environment.name : Effect.succeed(undefined);
      return makeDictsClient(name, ops);
    }),
  }),
);
