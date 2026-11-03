import * as image from "@distilled.cloud/modal/image";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import { operations } from "./internal.ts";

/** Modal's builder finished the image build with a failure. */
export class ImageBuildFailed extends Data.TaggedError("Modal.ImageBuildFailed")<{
  readonly imageId: string;
  readonly status: string;
  readonly message: string;
  /** The last lines the build printed. */
  readonly logs: string;
}> {}

/** The build didn't finish within its time budget. */
export class ImageBuildTimeout extends Data.TaggedError("Modal.ImageBuildTimeout")<{
  readonly imageId: string;
  readonly message: string;
}> {}

/** What an image is built from, after Dockerfile lowering. */
export interface ImageBuildSpec {
  /** A registry image to start from (`FROM <ref>`). */
  readonly dockerTag?: string;
  /** A Modal image to start from. */
  readonly baseImageId?: string;
  /**
   * Dockerfile commands. With `dockerTag`/`baseImageId` they follow the
   * generated `FROM`; otherwise they are a whole Dockerfile.
   */
  readonly commands: ReadonlyArray<string>;
  readonly contextFiles?: ReadonlyArray<{ readonly filename: string; readonly data: Uint8Array }>;
  /** A mount whose files the commands `COPY` (e.g. Modal's standalone Python). */
  readonly contextMountId?: string;
  /** Build arguments (`ARG` values). */
  readonly buildArgs?: Readonly<Record<string, string>>;
  /** Secrets (ids) the build steps see as environment variables. */
  readonly secretIds?: ReadonlyArray<string>;
  /** Credentials to pull a private base image. */
  readonly registry?: {
    readonly authType: "aws" | "gcp" | "static";
    readonly secretId: string;
  };
  /** Run the build on a GPU (`"A10G"`, `"H100"`, ...). */
  readonly gpu?: string;
  readonly builderVersion?: string;
  readonly forceBuild?: boolean;
}

const AUTH_TYPE = {
  aws: "REGISTRY_AUTH_TYPE_AWS",
  gcp: "REGISTRY_AUTH_TYPE_GCP",
  static: "REGISTRY_AUTH_TYPE_STATIC_CREDS",
} as const;

/** A `"A100-80GB:2"`-style GPU spec as Modal's `GPUConfig`. */
export const gpuConfig = (gpu: string | undefined): image.GPUConfig | undefined => {
  if (gpu === undefined) return undefined;
  const [type, count] = gpu.split(":");
  return { gpuType: type!.toUpperCase(), count: count ? Number(count) : 1 };
};

/** The operations a build needs, resolved by `ModalBindings.withCredentials` (or at deploy). */
export const ImageBuildOps = operations({
  getOrCreate: image.imageGetOrCreate,
  joinStreaming: image.imageJoinStreaming,
});

export type ImageBuildOps = Effect.Success<typeof ImageBuildOps>;

const FINISHED = (status: string | undefined) =>
  status !== undefined && status !== "GENERIC_STATUS_UNSPECIFIED";

/**
 * Build (or reuse) an image with Modal's builder and wait until it is ready.
 * Modal content-addresses images, so the same spec returns the same image
 * without rebuilding. Fails with {@link ImageBuildFailed} on a failed build.
 */
export const buildImage = Effect.fn(function* (
  ops: ImageBuildOps,
  appId: string,
  spec: ImageBuildSpec,
  options: { readonly note?: (message: string) => Effect.Effect<void> } = {},
) {
  const from = spec.baseImageId
    ? { baseImages: [{ imageId: spec.baseImageId, dockerTag: "base" }], first: ["FROM base"] }
    : spec.dockerTag
      ? { baseImages: [], first: [`FROM ${spec.dockerTag}`] }
      : { baseImages: [], first: [] as string[] };
  const contextFiles = yield* Effect.sync(() =>
    (spec.contextFiles ?? []).map((file) => ({
      filename: file.filename,
      data: Buffer.from(file.data).toString("base64"),
    })),
  );
  const created = yield* ops.getOrCreate({
    appId,
    image: {
      baseImages: from.baseImages,
      dockerfileCommands: [...from.first, ...spec.commands],
      contextFiles,
      contextMountId: spec.contextMountId,
      secretIds: spec.secretIds ? [...spec.secretIds] : undefined,
      imageRegistryConfig: spec.registry
        ? { registryAuthType: AUTH_TYPE[spec.registry.authType], secretId: spec.registry.secretId }
        : undefined,
      gpuConfig: gpuConfig(spec.gpu),
      buildArgs: spec.buildArgs ? { ...spec.buildArgs } : undefined,
    },
    builderVersion: spec.builderVersion,
    forceBuild: spec.forceBuild,
  });
  const imageId = created.imageId;
  if (!imageId) {
    return yield* new ImageBuildFailed({
      imageId: "",
      status: "GENERIC_STATUS_UNSPECIFIED",
      message: "Modal returned no image id",
      logs: "",
    });
  }
  let result = created.result;
  let lastEntryId: string | undefined;
  const logs: string[] = [];
  // Each join call streams build logs for up to ~55 s; a build may take many.
  for (let attempt = 0; !FINISHED(result?.status) && attempt < 60; attempt++) {
    if (attempt === 0 && options.note) yield* options.note(`Building image ${imageId}...`);
    const { items } = yield* ops.joinStreaming({
      imageId,
      timeout: 55,
      lastEntryId,
      includeLogsForFinished: true,
    });
    for (const item of items) {
      if (item.entryId) lastEntryId = item.entryId;
      for (const log of item.taskLogs ?? []) {
        if (log.data) logs.push(log.data);
      }
      if (FINISHED(item.result?.status)) result = item.result;
    }
  }
  if (!FINISHED(result?.status)) {
    return yield* new ImageBuildTimeout({
      imageId,
      message: `Modal image ${imageId} was still building after 55 minutes`,
    });
  }
  if (result?.status !== "GENERIC_STATUS_SUCCESS") {
    return yield* new ImageBuildFailed({
      imageId,
      status: result?.status ?? "GENERIC_STATUS_UNSPECIFIED",
      message: result?.exception || `Modal image build ${imageId} ended with ${result?.status}`,
      logs: logs.join("").split("\n").slice(-40).join("\n"),
    });
  }
  return imageId;
});
