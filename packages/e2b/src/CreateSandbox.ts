import type * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import * as Binding from "alchemy/Binding";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import type * as Effect from "effect/Effect";
import type { E2BError, E2BSandbox } from "./SandboxClient.ts";
import type { Snapshot } from "./Snapshot.ts";
import type { Template } from "./Template.ts";

/** A template or snapshot sandboxes are created from. */
export type SandboxBase = Template | Snapshot;

/**
 * E2B's `createSandbox` parameters. `templateID` defaults to the bound base;
 * `metadata` is merged with the host's ownership stamp.
 */
export type CreateSandboxOptions = Omit<sandboxes.CreateSandboxRequest, "templateID"> & {
  readonly templateID?: string;
};

/** E2B's `listSandboxes` filters, with `metadata` as a record instead of a query string. */
export type ListSandboxesOptions = Omit<
  sandboxes.ListSandboxesRequest,
  "metadata" | "nextToken"
> & {
  readonly metadata?: Readonly<Record<string, string>>;
};

export interface CreateSandboxBindingOptions {
  /**
   * What happens to the sandboxes this host created when the host is
   * destroyed: `"delete"` kills them, `"retain"` leaves them running.
   * @default "delete"
   */
  readonly onDestroy?: "delete" | "retain";
}

/** E2B sandboxes: create, look up, list and kill them. */
export interface SandboxesClient {
  /** Creates a sandbox from the bound base (or `templateID`). */
  create(options?: CreateSandboxOptions): Effect.Effect<E2BSandbox, E2BError, RuntimeContext>;
  /**
   * A handle to an existing sandbox. A paused sandbox is resumed; `timeout`
   * (seconds) resets its lifetime.
   */
  connect(
    sandboxId: string,
    options?: { readonly timeout?: number; readonly memory?: boolean },
  ): Effect.Effect<E2BSandbox, E2BError, RuntimeContext>;
  /** A sandbox's details, without resuming it. */
  get(sandboxId: string): Effect.Effect<sandboxes.SandboxDetail, E2BError, RuntimeContext>;
  /** Sandboxes, filtered by metadata, state or template (the bound base by default). */
  list(
    options?: ListSandboxesOptions,
  ): Effect.Effect<ReadonlyArray<sandboxes.ListedSandbox>, E2BError, RuntimeContext>;
  /** Kills a sandbox. Killing one that is gone succeeds. */
  kill(sandboxId: string): Effect.Effect<void, E2BError, RuntimeContext>;
  /** The latest CPU, memory and disk metrics of several sandboxes at once, by id. */
  metrics(
    sandboxIds: ReadonlyArray<string>,
  ): Effect.Effect<sandboxes.SandboxesWithMetrics, E2BError, RuntimeContext>;
}

/**
 * Create and manage E2B sandboxes from a Worker, Function or any other host.
 *
 * `E2B.CreateSandbox(base)` creates sandboxes from a template or snapshot
 * declared in the stack; `E2B.CreateSandbox()` takes the template per call.
 * Both return the same client, whose calls take E2B's own parameters.
 *
 * Every sandbox this host creates carries an ownership stamp in its
 * metadata, and the sandboxes still running or paused when the host is
 * destroyed are killed (`onDestroy: "retain"` keeps them).
 *
 * The host gets its own E2B API key, minted at deploy and revoked when the
 * host is destroyed. E2B keys are not scoped, so the key reaches the whole
 * team; it is never the deployer's own.
 *
 * ### Creating sandboxes
 * **Example:** One sandbox per request, killed when the request ends
 * ```typescript
 * const sandboxes = yield* E2B.CreateSandbox(Agent);
 * // in fetch:
 * const sandbox = yield* Effect.acquireRelease(
 *   sandboxes.create({ timeout: 120 }),
 *   (sandbox) => Effect.ignore(sandbox.kill()),
 * );
 * const result = yield* sandbox.commands.run("node --version");
 * ```
 *
 * **Example:** A long-lived sandbox per task, found by metadata
 * ```typescript
 * const [existing] = yield* sandboxes.list({ metadata: { task }, state: ["running", "paused"] });
 * const sandbox = existing
 *   ? yield* sandboxes.connect(existing.sandboxID)
 *   : yield* sandboxes.create({ metadata: { task }, autoPause: true, timeout: 1800 });
 * ```
 *
 * ### Network and secrets
 * **Example:** Allow only GitHub and inject a token at egress
 * ```typescript
 * const github = yield* GitHubToken;
 * const sandbox = yield* sandboxes.create({
 *   network: {
 *     allowOut: ["github.com", "*.github.com"],
 *     rules: { "api.github.com": [{ transform: { headers: { Authorization: `Bearer ${github.ref}` } } }] },
 *   },
 * });
 * ```
 *
 * @binding
 * @product Sandbox
 */
export interface CreateSandbox extends Binding.Service<
  CreateSandbox,
  "E2B.CreateSandbox",
  (base?: SandboxBase, options?: CreateSandboxBindingOptions) => Effect.Effect<SandboxesClient>
> {}

export const CreateSandbox = Binding.Service<CreateSandbox>("E2B.CreateSandbox");
