import * as Cloudflare from "@alchemy.run/cloudflare";
import * as E2B from "@alchemy.run/e2b";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import * as webhooks from "@distilled.cloud/e2b/webhooks";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import E2BEventsWorker, { EVENTS_PATH, SeenEvents } from "./fixtures/events-worker.ts";

const providers = Layer.mergeAll(E2B.providers(), Cloudflare.providers());
const { test } = Test.make({ providers });

test.provider(
  "a Worker receives verified sandbox lifecycle events, rejects unsigned ones, and its webhook is removed on destroy",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const { url } = yield* stack.deploy(
        Effect.gen(function* () {
          yield* SeenEvents;
          const worker = yield* E2BEventsWorker;
          return { url: worker.url.as<string>() };
        }),
      );
      const deliveryUrl = `${url}${EVENTS_PATH}`;
      const hooks = yield* webhooks.listWebhooks({});
      const hook = hooks.find((h) => h.url === deliveryUrl);
      expect(hook?.events.sort()).toEqual([
        "sandbox.lifecycle.created",
        "sandbox.lifecycle.killed",
      ]);

      // A fresh workers.dev URL answers 404, then briefly 5xx, until it
      // propagates.
      const client = HttpClient.filterStatus(
        yield* HttpClient.HttpClient,
        (status) => status !== 404 && status < 500,
      );
      // An unsigned delivery is rejected before the handler runs.
      const forged = yield* client
        .execute(
          HttpClientRequest.post(deliveryUrl).pipe(
            HttpClientRequest.bodyJsonUnsafe({
              id: "forged",
              type: "sandbox.lifecycle.killed",
              sandbox_id: "x",
            }),
          ),
        )
        .pipe(Effect.retry({ schedule: Schedule.exponential("500 millis"), times: 8 }));
      expect(forged.status).toBe(401);

      // A real sandbox's lifecycle reaches the handler.
      const sandbox = yield* sandboxes.createSandbox({ templateID: "base", timeout: 60 });
      yield* sandboxes.deleteSandbox({ sandboxID: sandbox.sandboxID });
      const seen = yield* client.get(`${url}/?sandbox=${sandbox.sandboxID}`).pipe(
        Effect.flatMap((res) => res.json),
        Effect.map((body) => body as { created: boolean; killed: boolean }),
        Effect.repeat({
          schedule: Schedule.spaced("3 seconds"),
          until: (body) => body.created && body.killed,
          times: 30,
        }),
      );
      expect(seen).toEqual({ created: true, killed: true });

      yield* stack.destroy();
      const after = yield* webhooks.listWebhooks({});
      expect(after.some((h) => h.url === deliveryUrl)).toBe(false);
    }),
  {
    tags: ["provider:e2b", "provider:e2b:webhook", "provider:cloudflare", "live"],
    timeout: 600_000,
  },
);
