import * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import type * as Output from "alchemy/Output";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ConnectSandbox, type ConnectedSandbox } from "./ConnectSandbox.ts";
import { bindE2B } from "./E2BBindings.ts";
import { makeSandboxHandle } from "./internal/SandboxHandle.ts";
import type { Sandbox } from "./Sandbox.ts";

/**
 * HTTP implementation of {@link ConnectSandbox}: E2B's control plane with
 * the host's key, and envd inside the sandbox with the sandbox's own token.
 * Works on any host.
 *
 * ### Provide the layer
 * **Example:** On a Cloudflare Worker
 * ```typescript
 * Effect.gen(function* () {
 *   const devBox = yield* E2B.ConnectSandbox(DevBox);
 *   // ...
 * }).pipe(Effect.provide(E2B.ConnectSandboxHttp))
 * ```
 *
 * @layer
 * @product Sandbox
 * @provides E2B.ConnectSandbox
 */
export const ConnectSandboxHttp = Layer.succeed(
  ConnectSandbox,
  Effect.fn("E2B.ConnectSandbox")(function* (sandbox: Sandbox) {
    const auth = yield* bindE2B;
    const sandboxId = yield* (sandbox.sandboxId as Output.Output<string, never>).asEffect();

    const client: ConnectedSandbox = {
      sandboxId,
      connect: Effect.fn("E2B.ConnectSandbox.connect")(function* (options) {
        const id = yield* sandboxId;
        const domain = yield* auth.domain;
        const connected = yield* auth.authorize(
          sandboxes.connectSandbox({
            sandboxID: id,
            ...(options?.timeout === undefined
              ? {}
              : {
                  timeout: Math.max(
                    1,
                    Math.round(Duration.toMillis(Duration.fromInputUnsafe(options.timeout)) / 1000),
                  ),
                }),
          }),
        );
        return makeSandboxHandle(auth, connected, domain);
      }),
      describe: Effect.fn("E2B.ConnectSandbox.describe")(function* () {
        const id = yield* sandboxId;
        return yield* auth.authorize(sandboxes.getSandbox({ sandboxID: id }));
      }),
      pause: Effect.fn("E2B.ConnectSandbox.pause")(function* () {
        const id = yield* sandboxId;
        yield* auth.authorize(sandboxes.pauseSandbox({ sandboxID: id }));
      }),
    };
    return client;
  }),
);
