import * as Archil from "@alchemy.run/archil";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { Box, Files } from "./Storage.ts";

/** `PUT /:key` writes a file to the disk; `GET /:key` reads it back. */
export default class Api extends Archil.Service<Api>()(
  "Api",
  { sandbox: Box, main: import.meta.url, port: 3000 },
  Effect.gen(function* () {
    const files = yield* Archil.ReadWriteDisk(Files);
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const key = new URL(request.url, "http://api").pathname.slice(1);
        if (request.method === "PUT") {
          yield* files.put(key, yield* request.text);
          return HttpServerResponse.empty({ status: 201 });
        }
        const file = yield* files.head(key);
        if (file === undefined) {
          return HttpServerResponse.text("not found", { status: 404 });
        }
        return HttpServerResponse.text(yield* files.getText(key));
      }).pipe(
        Effect.catch((error) =>
          Effect.succeed(HttpServerResponse.text(String(error), { status: 500 })),
        ),
      ),
    };
  }).pipe(Effect.provide(Archil.ReadWriteDiskHttp)),
) {}
