import * as Binding from "alchemy/Binding";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { ApiAccess } from "./ArchilBindings.ts";
import { makeSandboxHandle, type SandboxHandle } from "./Clients.ts";
import type { Sandbox } from "./Sandbox.ts";

/**
 * A handle on a deploy-time sandbox. The same methods as a
 * {@link SandboxHandle}; the id and name resolve at run time.
 */
export interface ConnectSandboxClient extends Omit<SandboxHandle, "sandboxId" | "name"> {
  readonly sandboxId: Effect.Effect<string, never, RuntimeContext>;
  readonly name: Effect.Effect<string, never, RuntimeContext>;
}

/**
 * Act on a sandbox declared at deploy time from a Worker, Lambda, or any
 * other host: run commands, move files, pause and resume, fork it into a
 * new sandbox, open ports, and change its network policy.
 *
 * ### Driving a Dev Box
 * **Example:** Build on the dev box, fork a preview per pull request
 * ```typescript
 * export default class Api extends Cloudflare.Worker<Api>()(
 *   "Api",
 *   { main: import.meta.url },
 *   Effect.gen(function* () {
 *     const devbox = yield* Archil.ConnectSandbox(DevBox);
 *     return {
 *       fetch: Effect.gen(function* () {
 *         yield* devbox.exec("git pull && pnpm build", { cwd: "/workspace" });
 *         const preview = yield* devbox.fork({ name: "pr-42" });
 *         const port = yield* preview.exposePort(3000);
 *         return HttpServerResponse.text(`https://${port.hostname}`);
 *       }),
 *     };
 *   }).pipe(Effect.provide(Archil.ConnectSandboxHttp)),
 * ) {}
 * ```
 *
 * @binding
 * @product Sandbox
 */
export interface ConnectSandbox extends Binding.Service<
  ConnectSandbox,
  "Archil.ConnectSandbox",
  (sandbox: Sandbox) => Effect.Effect<ConnectSandboxClient>
> {}

export const ConnectSandbox = Binding.Service<ConnectSandbox>("Archil.ConnectSandbox");

/** {@link ConnectSandbox} over Archil's HTTP API, usable from any host. */
export const ConnectSandboxHttp = Layer.effect(
  ConnectSandbox,
  Effect.gen(function* () {
    const access = yield* ApiAccess;
    return Effect.fn(function* (sandbox: Sandbox) {
      const authorize = yield* access();
      const sandboxId = yield* sandbox.sandboxId;
      const name = yield* sandbox.name;
      const handle = Effect.map(Effect.all([sandboxId, name]), ([sid, n]) =>
        makeSandboxHandle({ sandboxId: sid, name: n }, authorize),
      );
      const via =
        <K extends keyof SandboxHandle>(key: K) =>
        (...args: any[]): any =>
          Effect.flatMap(handle, (h) => (h[key] as (...a: any[]) => any)(...args));
      return {
        sandboxId,
        name,
        refresh: via("refresh"),
        exec: via("exec"),
        run: via("run"),
        attach: via("attach"),
        kill: via("kill"),
        writeFile: via("writeFile"),
        readFile: via("readFile"),
        start: via("start"),
        stop: via("stop"),
        pause: via("pause"),
        resume: via("resume"),
        fork: via("fork"),
        setTimeout: via("setTimeout"),
        exposePort: via("exposePort"),
        unexposePort: via("unexposePort"),
        ports: via("ports"),
        createPortToken: via("createPortToken"),
        revokePortToken: via("revokePortToken"),
        listPortTokens: () => Stream.unwrap(Effect.map(handle, (h) => h.listPortTokens())),
        setEgress: via("setEgress"),
        delete: via("delete"),
      } satisfies ConnectSandboxClient;
    });
  }),
);
