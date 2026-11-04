import * as Archil from "@alchemy.run/archil";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as archil from "@distilled.cloud/archil";
import * as Objects from "@distilled.cloud/archil/Objects";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as Schedule from "effect/Schedule";
import EffectApi from "./fixtures/service/effect.ts";
import { asyncHello, ServiceBox, ServiceData } from "./fixtures/service/resources.ts";

const { test } = Test.make({ providers: Archil.providers() });

const getText = (url: string) =>
  Effect.gen(function* () {
    const client = HttpClient.filterStatusOk(yield* HttpClient.HttpClient);
    return yield* client.get(url).pipe(
      Effect.flatMap((r) => r.text),
      // A fresh hostname or a waking sandbox can take a few seconds.
      Effect.retry({ schedule: Schedule.spaced("2 seconds"), times: 20 }),
    );
  });

const services = (sid: string) =>
  archil
    .listSandboxPorts({ sid })
    .pipe(Effect.map(({ data }) => (data.ports ?? []).map((p) => p.port)));

test.provider(
  "Effect and async services run in one sandbox, redeploy with new env, and are removed",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const app = (greeting: string) =>
        Effect.gen(function* () {
          const box = yield* ServiceBox;
          const data = yield* ServiceData;
          const api = yield* EffectApi;
          const hello = yield* asyncHello(greeting);
          return { box, data, api, hello };
        });

      const { box, data, api, hello } = yield* stack.deploy(app("hello"));
      expect(api.url).toMatch(/^https:\/\/3000-/);
      expect(hello.url).toMatch(/^https:\/\/3001-/);

      // Effect form: the binding reads a file seeded out of band.
      yield* Objects.putObject({ disk: data.diskId }, "note.txt", "from the disk").pipe(
        Effect.provide(Objects.ObjectCredentialsFromApiKey),
      );
      expect(yield* getText(`${api.url}/`)).toBe("effect ok");
      expect(yield* getText(`${api.url}/read?key=note.txt`)).toBe("from the disk");

      // Async form: the handler gets env as plain strings.
      expect(JSON.parse(yield* getText(`${hello.url}/x`))).toEqual({
        greeting: "hello",
        path: "/x",
      });

      // Changing env redeploys the async service in place.
      const updated = yield* stack.deploy(app("bonjour"));
      expect(updated.hello.name).toBe(hello.name);
      expect(JSON.parse(yield* getText(`${updated.hello.url}/y`))).toEqual({
        greeting: "bonjour",
        path: "/y",
      });

      // Removing a service unexposes its port and leaves the other running.
      yield* stack.deploy(
        Effect.gen(function* () {
          yield* ServiceBox;
          yield* ServiceData;
          return yield* EffectApi;
        }),
      );
      expect(yield* services(box.sandboxId)).toEqual([3000]);
      expect(yield* getText(`${api.url}/`)).toBe("effect ok");

      yield* stack.destroy();
      const gone = yield* archil.getSandbox({ sid: box.sandboxId }).pipe(
        Effect.map(({ data }) => data.status === "deleted"),
        Effect.catchTag("SandboxNotFound", () => Effect.succeed(true)),
      );
      expect(gone).toBe(true);
    }),
  { tags: ["provider:archil", "provider:archil:service", "live"], timeout: 600_000 },
);
