import * as volume from "@distilled.cloud/modal/volume";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { makeOpenModalBinding } from "./BindingHttp.ts";
import { CreateVolume, type VolumeHandle, type VolumesClient } from "./CreateVolume.ts";
import type { Environment } from "./Environment.ts";
import { environmentOf, listAll, operations } from "./internal.ts";
import { ensureVolume, lookupVolume, type VolumeAttributes } from "./Volume.ts";
import { makeReadWriteVolumeClient } from "./VolumeClient.ts";
import { VolumeFileOps } from "./VolumeFiles.ts";

/** The operations the client calls, resolved by `ModalBindings.withCredentials`. */
const VolumesOps = operations({
  getOrCreate: volume.volumeGetOrCreate,
  delete: volume.deleteVolume,
  list: volume.listVolume,
  client: VolumeFileOps,
});

type VolumesOps = Effect.Success<typeof VolumesOps>;

const handleOf = (attributes: VolumeAttributes, ops: VolumesOps): VolumeHandle => ({
  ...makeReadWriteVolumeClient(Effect.succeed(attributes.volumeId), ops.client),
  volumeId: Effect.succeed(attributes.volumeId),
  name: attributes.name,
  attributes,
});

/** A volumes client over an environment name resolved per call. */
export const makeVolumesClient = (
  environment: Effect.Effect<string | undefined>,
  ops: VolumesOps,
): VolumesClient => ({
  getOrCreate: (name) =>
    Effect.flatMap(environment, (env) => ensureVolume(ops.getOrCreate, name, env)).pipe(
      Effect.map((attributes) => handleOf(attributes, ops)),
    ),
  get: (name) =>
    Effect.flatMap(environment, (env) => lookupVolume(ops.getOrCreate, name, env)).pipe(
      Effect.map((attributes) => (attributes ? handleOf(attributes, ops) : undefined)),
    ),
  delete: (name) =>
    Effect.gen(function* () {
      const env = yield* environment;
      const found = yield* lookupVolume(ops.getOrCreate, name, env);
      if (!found) return;
      yield* ops
        .delete({ volumeId: found.volumeId, environmentName: environmentOf(env) })
        .pipe(Effect.catchTag("VolumeNotFound", () => Effect.void));
    }),
  list: () =>
    Effect.gen(function* () {
      const env = yield* environment;
      const items = yield* listAll(
        (pagination) =>
          ops
            .list({ environmentName: environmentOf(env), pagination })
            .pipe(Effect.map((response) => response.items ?? [])),
        (item) => item.createdAt,
      );
      return items.flatMap((item) => {
        const name = item.label ?? item.metadata?.name;
        return name ? [name] : [];
      });
    }),
});

/**
 * {@link CreateVolume} over Modal's API. Grants the host's identity (a scoped
 * `Modal.ServiceUser`) the `contributor` role on the environment.
 *
 * @layer
 * @product Volume
 * @provides Modal.CreateVolume
 */
export const CreateVolumeHttp = Layer.effect(
  CreateVolume,
  makeOpenModalBinding({
    tag: "Modal.CreateVolume",
    operations: VolumesOps,
    role: "contributor",
    environmentOf: (environment?: Environment) => environment?.name,
    make: Effect.fn(function* (ops, environment?: Environment) {
      const name = environment ? yield* environment.name : Effect.succeed(undefined);
      return makeVolumesClient(name, ops);
    }),
  }),
);
