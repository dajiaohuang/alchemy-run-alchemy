import * as volumes from "@distilled.cloud/daytona/volumes";
import * as Binding from "alchemy/Binding";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  CreateVolume,
  NameOutsidePrefix,
  type CreateVolumeClient,
  type CreateVolumeOptions,
} from "./CreateVolume.ts";
import { makeAccess } from "./DaytonaBindings.ts";
import { RuntimeObjects } from "./RuntimeObjects.ts";
import { deleteVolumeById, ensureVolume, getVolumeByName, toVolumeAttributes } from "./Volume.ts";

/** {@link CreateVolume} over Daytona's HTTP API; works on any host. */
export const CreateVolumeHttp = Layer.effect(
  CreateVolume,
  Effect.gen(function* () {
    const access = yield* makeAccess;
    const Objects = yield* RuntimeObjects;
    return Effect.fn(function* (options?: CreateVolumeOptions) {
      const host = yield* Binding.Host;
      const { authorize } = yield* access("Volumes", [
        "read:volumes",
        "write:volumes",
        "delete:volumes",
      ]);
      yield* Objects(`${host?.LogicalId ?? ""}VolumesObjects`, {
        kinds: ["volume"],
        onDestroy: options?.onDestroy ?? "retain",
        namePrefix: options?.prefix,
      });
      const prefix = options?.prefix;
      const client: CreateVolumeClient = {
        getOrCreate: ({ name }) =>
          prefix !== undefined && !name.startsWith(prefix)
            ? Effect.fail(new NameOutsidePrefix({ name, prefix }))
            : authorize(ensureVolume(name).pipe(Effect.map(toVolumeAttributes))),
        get: (name) =>
          authorize(
            getVolumeByName(name).pipe(Effect.map((v) => (v ? toVolumeAttributes(v) : undefined))),
          ),
        list: () =>
          authorize(
            volumes
              .listVolumes({})
              .pipe(
                Effect.map((all) =>
                  all.filter((v) => v.state !== "deleted").map(toVolumeAttributes),
                ),
              ),
          ),
        delete: (volumeId) => authorize(deleteVolumeById(volumeId)),
      };
      return client;
    });
  }),
);
