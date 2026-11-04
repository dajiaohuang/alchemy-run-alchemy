import * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import * as Binding from "alchemy/Binding";
import * as Namespace from "alchemy/Namespace";
import type * as Output from "alchemy/Output";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import {
  CreateSandbox,
  type CreateSandboxBindingOptions,
  type SandboxBase,
  type SandboxesClient,
} from "./CreateSandbox.ts";
import { bindE2B } from "./E2BBindings.ts";
import { SandboxBaseMissing } from "./Errors.ts";
import { denyUnlisted, makeSandboxHandle } from "./internal/SandboxHandle.ts";
import { metadataQuery, Sweep } from "./Sweep.ts";

/** The `E2B.Sweep` constructor, as a binding layer's construction yields it. */
type SweepConstructor = Effect.Success<typeof Sweep>;

/**
 * Declares the host's sweep for the runtime-created objects of `kind`.
 * Deploy-time only; a no-op inside the deployed host and without a host.
 *
 * @internal
 */
export const declareSweep = Effect.fn(function* (
  sweep: SweepConstructor,
  kind: "sandboxes" | "secrets",
  stamp: Readonly<Record<string, string>>,
  onDestroy: "delete" | "retain" | undefined,
) {
  if (globalThis.__ALCHEMY_RUNTIME__) return;
  const host = yield* Binding.Host;
  if (host === undefined) return;
  yield* Namespace.push(
    host.LogicalId,
    Effect.asVoid(
      sweep(`E2B${kind === "sandboxes" ? "Sandbox" : "Secret"}Sweep`, {
        stamp: { ...stamp },
        kinds: [kind],
        ...(onDestroy === undefined ? {} : { onDestroy }),
      }),
    ),
  );
});

/**
 * HTTP implementation of {@link CreateSandbox}: E2B's control plane with the
 * team key, and envd inside each sandbox with that sandbox's own token.
 * Works on any host.
 *
 * ### Provide the layer
 * **Example:** On a Cloudflare Worker
 * ```typescript
 * Effect.gen(function* () {
 *   const sandboxes = yield* E2B.CreateSandbox(Agent);
 *   // ...
 * }).pipe(Effect.provide(E2B.CreateSandboxHttp))
 * ```
 *
 * @layer
 * @product Sandbox
 * @provides E2B.CreateSandbox
 */
export const CreateSandboxHttp = Layer.effect(
  CreateSandbox,
  Effect.gen(function* () {
    // Yielding the resource class erases its `Providers` requirement; the
    // stack satisfies it at plan time.
    const sweep = yield* Sweep;
    return Effect.fn("E2B.CreateSandbox")(function* (
      base?: SandboxBase,
      options?: CreateSandboxBindingOptions,
    ) {
      const auth = yield* bindE2B;
      yield* declareSweep(sweep, "sandboxes", auth.deployStamp, options?.onDestroy);
      // Both a Template and a Snapshot expose the template id sandboxes boot from.
      const baseId: Effect.Effect<string> | undefined =
        base === undefined
          ? undefined
          : yield* (base.templateId as Output.Output<string, never>).asEffect();

      const withStamp = Effect.fn(function* (
        metadata: Readonly<Record<string, string | undefined>> | undefined,
      ) {
        return { ...metadata, ...(yield* auth.stamp) };
      });

      const client: SandboxesClient = {
        create: Effect.fn("E2B.CreateSandbox.create")(function* (createOptions) {
          const templateID =
            createOptions?.templateID ?? (baseId === undefined ? undefined : yield* baseId);
          if (templateID === undefined) {
            return yield* new SandboxBaseMissing({
              message: "E2B.CreateSandbox() has no bound base: pass `templateID` to create()",
            });
          }
          const domain = yield* auth.domain;
          const created = yield* auth.authorize(
            sandboxes.createSandbox({
              ...createOptions,
              ...(createOptions?.network === undefined
                ? {}
                : { network: denyUnlisted(createOptions.network) }),
              templateID,
              metadata: yield* withStamp(createOptions?.metadata),
            }),
          );
          return makeSandboxHandle(auth, created, domain);
        }),
        connect: Effect.fn("E2B.CreateSandbox.connect")(function* (sandboxId, connectOptions) {
          const domain = yield* auth.domain;
          const connected = yield* auth.authorize(
            sandboxes.connectSandbox({ sandboxID: sandboxId, ...connectOptions }),
          );
          return makeSandboxHandle(auth, connected, domain);
        }),
        get: (sandboxId) => auth.authorize(sandboxes.getSandbox({ sandboxID: sandboxId })),
        list: Effect.fn("E2B.CreateSandbox.list")(function* (listOptions) {
          const template =
            listOptions?.template ?? (baseId === undefined ? undefined : yield* baseId);
          const all = yield* auth.authorize(
            Stream.runCollect(
              sandboxes.listSandboxes.items({
                ...listOptions,
                ...(template === undefined ? {} : { template }),
                metadata:
                  listOptions?.metadata === undefined
                    ? undefined
                    : metadataQuery(listOptions.metadata),
              }),
            ),
          );
          return Array.from(all);
        }),
        kill: (sandboxId) =>
          auth.authorize(sandboxes.deleteSandbox({ sandboxID: sandboxId })).pipe(
            Effect.asVoid,
            Effect.catchTag("NotFound", () => Effect.void),
          ),
        metrics: (sandboxIds) =>
          auth.authorize(sandboxes.listSandboxesMetrics({ sandbox_ids: [...sandboxIds] })),
      };
      return client;
    });
  }),
);
