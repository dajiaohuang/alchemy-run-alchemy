import * as archil from "@distilled.cloud/archil";
import * as Binding from "alchemy/Binding";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ApiAccess, RuntimePrefix } from "./ArchilBindings.ts";
import { makeSandboxHandle, type SandboxHandle } from "./Clients.ts";
import type { Image } from "./Image.ts";
import type { ArchilFailed, ArchilNotReady } from "./internal/archil.ts";
import { findSandboxByName, listSandboxes, waitForSandbox } from "./internal/archil.ts";
import { runtimeName, type OnDestroy } from "./RuntimeObjects.ts";
import { deleteSandbox, toCreateSandboxRequest, type SandboxProps } from "./Sandbox.ts";

export interface CreateSandboxOptions {
  /**
   * What happens to the sandboxes this client created when the stack is
   * destroyed.
   * @default "delete"
   */
  onDestroy?: OnDestroy;
}

/** A run-time create: a {@link Sandbox}'s props, with a required name. */
export interface CreateSandboxRequest extends Omit<SandboxProps, "name"> {
  /** Name, stamped with this binding's prefix. Lowercase letters, digits and `-`. */
  readonly name: string;
}

export type CreateSandboxError =
  | archil.CreateSandboxError
  | archil.GetSandboxError
  | archil.ListSandboxesError
  | ArchilFailed
  | ArchilNotReady;

export interface CreateSandboxClient {
  /**
   * Create a sandbox (or return the one that already has this name) and
   * wait until it is running. Takes a {@link Sandbox}'s props; the bound
   * image is the default.
   */
  create(
    request: CreateSandboxRequest,
  ): Effect.Effect<SandboxHandle, CreateSandboxError, RuntimeContext>;
  /** A sandbox this client created, or `undefined`. */
  get(
    name: string,
  ): Effect.Effect<SandboxHandle | undefined, archil.ListSandboxesError, RuntimeContext>;
  /** Every sandbox this client created. */
  list(): Effect.Effect<SandboxHandle[], archil.ListSandboxesError, RuntimeContext>;
  /** Delete a sandbox this client created. Deleting a missing sandbox succeeds. */
  delete(
    name: string,
  ): Effect.Effect<
    void,
    archil.ListSandboxesError | Effect.Error<ReturnType<typeof deleteSandbox>>,
    RuntimeContext
  >;
}

/**
 * Create sandboxes at run time, one per user, session, or task: Firecracker
 * microVMs with disks mounted, public ports, and an outbound network policy
 * that can inject credentials at the boundary. Bind an {@link Image} to make
 * it the default image; a run-time `create` takes the same props as a
 * deploy-time {@link Sandbox}.
 *
 * Every sandbox is named `<prefix>-<name>`, with a prefix this binding
 * owns. Destroying the stack deletes the sandboxes the client created,
 * unless `onDestroy` is `"retain"`.
 *
 * ### Sandboxes per Task
 * **Example:** Run a task in a sandbox with the user's disk mounted
 * ```typescript
 * export default class Api extends Cloudflare.Worker<Api>()(
 *   "Api",
 *   { main: import.meta.url },
 *   Effect.gen(function* () {
 *     const disks = yield* Archil.CreateDisk();
 *     const sandboxes = yield* Archil.CreateSandbox(Agent);
 *     return {
 *       fetch: Effect.gen(function* () {
 *         const home = yield* disks.create("user-1");
 *         const sandbox = yield* sandboxes.create({
 *           name: "task-1",
 *           mounts: [{ disk: home.diskId, path: "/home/user" }],
 *           network: { egress: { default: "deny", allow: ["registry.npmjs.org"] } },
 *           idleTtlSeconds: 300,
 *         });
 *         const result = yield* sandbox.exec("npm test", { cwd: "/home/user/app" });
 *         yield* sandbox.pause();
 *         return HttpServerResponse.text(result.stdout);
 *       }),
 *     };
 *   }).pipe(Effect.provide(Layer.mergeAll(Archil.CreateDiskHttp, Archil.CreateSandboxHttp))),
 * ) {}
 * ```
 *
 * @binding
 * @product Sandbox
 */
export interface CreateSandbox extends Binding.Service<
  CreateSandbox,
  "Archil.CreateSandbox",
  (image?: Image, options?: CreateSandboxOptions) => Effect.Effect<CreateSandboxClient>
> {}

export const CreateSandbox = Binding.Service<CreateSandbox>("Archil.CreateSandbox");

/** {@link CreateSandbox} over Archil's HTTP API, usable from any host. */
export const CreateSandboxHttp = Layer.effect(
  CreateSandbox,
  Effect.gen(function* () {
    const access = yield* ApiAccess;
    const prefixes = yield* RuntimePrefix;
    return Effect.fn(function* (image?: Image, options?: CreateSandboxOptions) {
      const authorize = yield* access();
      const prefix = yield* prefixes("Sandboxes", ["sandbox"], options?.onDestroy);
      const imageId = image ? yield* image.imageId : undefined;
      const fullName = (name: string) => Effect.map(prefix, (p) => runtimeName(p, name));
      // Forks are stamped like the sandboxes they come from, so destroy
      // sweeps them too; an unnamed fork gets a random name.
      const forkName = Effect.fn(function* (name: string | undefined) {
        const id = yield* Effect.sync(() => crypto.randomUUID().slice(0, 8));
        return yield* fullName(name ?? `fork-${id}`);
      });
      const handle = (s: { sandbox_id: string; name: string }) =>
        makeSandboxHandle({ sandboxId: s.sandbox_id, name: s.name }, authorize, forkName);
      return {
        create: (request) =>
          authorize(
            Effect.gen(function* () {
              const name = yield* fullName(request.name);
              const existing = yield* findSandboxByName(name);
              if (existing) return handle(existing);
              const defaultImage =
                request.image === undefined && request.baseImage === undefined && imageId
                  ? yield* imageId
                  : request.image;
              const { data } = yield* archil
                .createSandbox(toCreateSandboxRequest(name, { ...request, image: defaultImage }))
                .pipe(
                  Effect.catchTag("SandboxAlreadyExists", (conflict) =>
                    findSandboxByName(name).pipe(
                      Effect.flatMap((found) =>
                        found ? Effect.succeed({ data: found }) : Effect.fail(conflict),
                      ),
                    ),
                  ),
                );
              return handle(yield* waitForSandbox(data.sandbox_id));
            }),
          ),
        get: (name) =>
          authorize(
            Effect.gen(function* () {
              const sandboxName = yield* fullName(name);
              const found = yield* findSandboxByName(sandboxName);
              return found ? handle(found) : undefined;
            }),
          ),
        list: () =>
          authorize(
            Effect.gen(function* () {
              const p = yield* prefix;
              return (yield* listSandboxes())
                .filter((s) => s.name.startsWith(`${p}-`) && s.status !== "deleted")
                .map(handle);
            }),
          ),
        delete: (name) =>
          authorize(
            Effect.gen(function* () {
              const sandboxName = yield* fullName(name);
              const found = yield* findSandboxByName(sandboxName);
              if (found) yield* deleteSandbox(found.sandbox_id);
            }),
          ),
      } satisfies CreateSandboxClient;
    });
  }),
);
