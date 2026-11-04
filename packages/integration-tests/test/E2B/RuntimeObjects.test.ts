import * as Cloudflare from "@alchemy.run/cloudflare";
import * as E2B from "@alchemy.run/e2b";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as apiKeys from "@distilled.cloud/e2b/api_keys";
import * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import * as secrets from "@distilled.cloud/e2b/secrets";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import { BindingAgent, ProbeWebhook, RotatedSecret } from "./fixtures/resources.ts";
import E2BWorker from "./fixtures/worker.ts";

const providers = Layer.mergeAll(E2B.providers(), Cloudflare.providers());
const { test } = Test.make({ providers });

/** Letters and digits only, lowercased: how a stack name survives inside a physical name. */
const squash = (name: string) => name.toLowerCase().replace(/[^a-z0-9]/g, "");

/**
 * The ids of the API keys a stack owns. Generated names start with the stack
 * name, and the stack's only API key is its host's minted one; the name is
 * truncated to 64 characters, so match on a prefix.
 */
const stackKeyIds = (stackName: string) =>
  apiKeys
    .listApiKeys({})
    .pipe(
      Effect.map(
        (all) =>
          new Set(
            all
              .filter((key) => squash(key.name).startsWith(squash(stackName).slice(0, 24)))
              .map((key) => key.id),
          ),
      ),
    );

test.provider(
  "destroying a host kills its runtime sandboxes and secrets and revokes its minted key",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const before = yield* stackKeyIds(stack.name);
      const { url } = yield* stack.deploy(
        Effect.gen(function* () {
          yield* BindingAgent;
          yield* RotatedSecret;
          yield* ProbeWebhook;
          const worker = yield* E2BWorker;
          return { url: worker.url.as<string>() };
        }),
      );
      const name = `alchemy-${Test.defaultStage()}-leftover`
        .toLowerCase()
        .replace(/[^a-z0-9-]/g, "-");
      // A fresh workers.dev URL answers 404 until it propagates.
      const client = HttpClient.filterStatus(
        yield* HttpClient.HttpClient,
        (status) => status !== 404,
      );
      const res = yield* client
        .get(`${url}/leftover?name=${name}`)
        .pipe(Effect.retry({ schedule: Schedule.exponential("500 millis"), times: 8 }));
      const left = (yield* res.json) as { sandboxId: string; secretId: string };
      expect(res.status).toBe(200);

      // The Worker ran on its own minted key, not the deployer's credentials.
      const minted = [...(yield* stackKeyIds(stack.name))].filter((id) => !before.has(id));
      expect(minted.length).toBeGreaterThan(0);
      // E2B records a key's last use asynchronously.
      const usedAt = yield* apiKeys.listApiKeys({}).pipe(
        Effect.map((all) =>
          all.filter((key) => minted.includes(key.id)).map((key) => key.lastUsed ?? undefined),
        ),
        Effect.repeat({
          schedule: Schedule.spaced("3 seconds"),
          until: (stamps) => stamps.some((stamp) => stamp !== undefined),
          times: 10,
        }),
      );
      expect(usedAt.some((stamp) => stamp !== undefined)).toBe(true);

      // Stamped with the host's RuntimeObjects: its tags plus the kind.
      const alive = yield* sandboxes.getSandbox({ sandboxID: left.sandboxId });
      expect(alive.metadata?.["alchemy::runtime"]).toBe("sandboxes");
      expect(alive.metadata?.["alchemy::id"]).toContain("E2BSandboxes");
      const stored = yield* secrets.getSecret({ secretID: left.secretId });
      expect(stored.metadata["alchemy::runtime"]).toBe("secrets");
      expect(stored.metadata["alchemy::id"]).toContain("E2BSecrets");

      yield* stack.destroy();

      const sandbox = yield* sandboxes.getSandbox({ sandboxID: left.sandboxId }).pipe(
        Effect.map((s): string => s.state),
        Effect.catchTag("NotFound", () => Effect.succeed("gone")),
      );
      expect(sandbox).toBe("gone");
      const secret = yield* secrets.getSecret({ secretID: left.secretId }).pipe(
        Effect.map((): string => "present"),
        Effect.catchTag("NotFound", () => Effect.succeed("gone")),
      );
      expect(secret).toBe("gone");
      // Destroying the host revokes its key.
      const after = yield* stackKeyIds(stack.name);
      expect(minted.filter((id) => after.has(id))).toEqual([]);
    }),
  {
    tags: ["provider:e2b", "provider:e2b:runtime-objects", "provider:cloudflare", "live"],
    timeout: 600_000,
  },
);

test.provider(
  "onDestroy: retain keeps the host's runtime sandboxes",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const objects = yield* stack.deploy(
        Effect.gen(function* () {
          // Declared directly, as CreateSandbox({ onDestroy: "retain" }) declares it.
          return yield* E2B.RuntimeObjects("Retained", {
            kinds: ["sandboxes"],
            onDestroy: "retain",
          });
        }),
      );
      expect(objects.onDestroy).toBe("retain");
      expect(objects.tags["alchemy::id"]).toBe("Retained");
      // On a public template: E2B refuses to delete a template while a
      // sandbox runs on it, and this one is kept.
      const kept = yield* sandboxes.createSandbox({
        templateID: "base",
        timeout: 120,
        metadata: { ...objects.tags, "alchemy::runtime": "sandboxes" },
      });
      yield* stack.destroy();
      const state = yield* sandboxes.getSandbox({ sandboxID: kept.sandboxID }).pipe(
        Effect.map((s): string => s.state),
        Effect.catchTag("NotFound", () => Effect.succeed("gone")),
      );
      yield* sandboxes.deleteSandbox({ sandboxID: kept.sandboxID }).pipe(Effect.ignore);
      expect(state).toBe("running");
    }),
  { tags: ["provider:e2b", "provider:e2b:runtime-objects", "live"], timeout: 600_000 },
);
