import type * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import * as Binding from "alchemy/Binding";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import type { Sandbox } from "./Sandbox.ts";
import type { E2BError, E2BSandbox } from "./SandboxClient.ts";

/** One declared sandbox, from a host. */
export interface ConnectedSandbox {
  /** The sandbox's id. */
  readonly sandboxId: Effect.Effect<string, never, RuntimeContext>;
  /**
   * A handle to the sandbox, resumed if it was paused. `timeout` sets how
   * long it runs before it pauses again.
   */
  connect(options?: {
    readonly timeout?: Duration.Input;
  }): Effect.Effect<E2BSandbox, E2BError, RuntimeContext>;
  /** Live details (state, resources, network), without resuming it. */
  describe(): Effect.Effect<sandboxes.SandboxDetail, E2BError, RuntimeContext>;
  /** Pauses the sandbox now, keeping its memory and files. */
  pause(): Effect.Effect<void, E2BError, RuntimeContext>;
}

/**
 * Work with one {@link Sandbox} declared in the stack, from a Worker,
 * Function or any other host: resume it, run commands, read and write
 * files, call its ports, snapshot or fork it.
 *
 * The host gets its own E2B API key, minted at deploy and revoked when the
 * host is destroyed. E2B keys are not scoped, so the key reaches the whole
 * team; it is never the deployer's own.
 *
 * ### Running commands in a dev box
 * **Example:** Pull and test on request
 * ```typescript
 * const devBox = yield* E2B.ConnectSandbox(DevBox);
 * return {
 *   fetch: Effect.gen(function* () {
 *     const sandbox = yield* devBox.connect();
 *     const test = yield* sandbox.commands.run("git pull && npm test", { cwd: "/home/user/repo" });
 *     return yield* HttpServerResponse.json(test);
 *   }),
 * };
 * ```
 *
 * ### Calling a program in the sandbox
 * **Example:** Forward a request to port 3000
 * ```typescript
 * const sandbox = yield* api.connect();
 * const response = yield* sandbox.fetch(3000, HttpClientRequest.get("/health"));
 * ```
 *
 * ### Pausing it early
 * **Example:** Pause when the work is done
 * ```typescript
 * yield* devBox.pause();
 * ```
 *
 * @binding
 * @product Sandbox
 */
export interface ConnectSandbox extends Binding.Service<
  ConnectSandbox,
  "E2B.ConnectSandbox",
  (sandbox: Sandbox) => Effect.Effect<ConnectedSandbox>
> {}

export const ConnectSandbox = Binding.Service<ConnectSandbox>("E2B.ConnectSandbox");
