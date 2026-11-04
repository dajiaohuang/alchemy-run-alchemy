import * as Archil from "@alchemy.run/archil";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { ServiceBox, ServiceData } from "./resources.ts";

/** An Effect service in an Archil sandbox that reads a disk through ReadDisk. */
export default class EffectApi extends Archil.Service<EffectApi>()(
  "EffectApi",
  { sandbox: ServiceBox, main: import.meta.url, port: 3000 },
  Effect.gen(function* () {
    const files = yield* Archil.ReadDisk(ServiceData);
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const url = new URL(request.url, "http://service");
        if (url.pathname === "/read") {
          const text = yield* files.getText(url.searchParams.get("key") ?? "");
          return HttpServerResponse.text(text);
        }
        return HttpServerResponse.text("effect ok");
      }).pipe(
        Effect.catch((e) => Effect.succeed(HttpServerResponse.text(String(e), { status: 500 }))),
      ),
    };
  }).pipe(Effect.provide(Archil.ReadDiskHttp)),
) {}
