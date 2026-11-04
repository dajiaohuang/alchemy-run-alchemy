import * as Daytona from "@alchemy.run/daytona";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { SERVICE_PORT, ServiceBox } from "./shared.ts";

/**
 * An Effect program served from a sandbox: it reads its environment, and
 * reads its own sandbox through ConnectSandbox with a key minted for it.
 */
export default class Api extends Daytona.Service<Api>()(
  "Api",
  {
    sandbox: ServiceBox,
    main: import.meta.url,
    port: SERVICE_PORT,
    env: { GREETING: "hello-from-daytona" },
  },
  Effect.gen(function* () {
    const box = yield* Daytona.ConnectSandbox(ServiceBox);
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const url = new URL(request.url, "http://service");
        if (url.pathname === "/box") {
          // Platform calls work from any sandbox; below Tier 3 the toolbox
          // proxy isn't on Daytona's egress allowlist.
          const self = yield* box.getSandbox().pipe(Effect.orDie);
          return yield* HttpServerResponse.json({ sandboxId: self.id, state: self.state });
        }
        const greeting = yield* Config.String("GREETING").pipe(Effect.orDie);
        const node = yield* Effect.sync(() => process.version);
        return yield* HttpServerResponse.json({ greeting, node });
      }),
    };
  }).pipe(Effect.provide(Daytona.ConnectSandboxHttp)),
) {}
