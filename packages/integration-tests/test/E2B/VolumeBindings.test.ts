import * as Cloudflare from "@alchemy.run/cloudflare";
import * as E2B from "@alchemy.run/e2b";
import { describe, expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import { BindingAgent, SharedVolume } from "./fixtures/resources.ts";
import E2BVolumeWorker from "./fixtures/volume-worker.ts";

// E2B enables volumes per team; others get `VolumesNotEnabled` (pinned in
// Volume.test.ts). E2B_TEST_VOLUMES=1 runs these on an enabled team.
const volumesEnabled = !!process.env.E2B_TEST_VOLUMES;

const providers = Layer.mergeAll(E2B.providers(), Cloudflare.providers());
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({ providers });

const Stack = Alchemy.Stack(
  "E2BVolumeBindingsStack",
  { providers, state: Cloudflare.state() },
  Effect.gen(function* () {
    yield* BindingAgent;
    yield* SharedVolume;
    const worker = yield* E2BVolumeWorker;
    return { url: worker.url.as<string>() };
  }),
);

const stack = volumesEnabled ? beforeAll(deploy(Stack)) : Effect.die("E2B_TEST_VOLUMES is not set");
afterAll.skipIf(!volumesEnabled || !!process.env.NO_DESTROY)(destroy(Stack));

const tags = ["provider:e2b", "provider:cloudflare", "provider:cloudflare:worker", "live"];

const get = <A>(path: string) =>
  Effect.gen(function* () {
    const { url } = yield* stack;
    // A fresh workers.dev URL answers 404 until it propagates.
    const client = HttpClient.filterStatus(
      yield* HttpClient.HttpClient,
      (status) => status !== 404,
    );
    const res = yield* client
      .get(`${url}${path}`)
      .pipe(Effect.retry({ schedule: Schedule.exponential("500 millis"), times: 8 }));
    const body = (yield* res.json) as A & { error?: string };
    expect(body.error).toBeUndefined();
    expect(res.status).toBe(200);
    return body;
  });

describe("ReadVolume / WriteVolume / ReadWriteVolume", () => {
  test.skipIf(!volumesEnabled)(
    "reads and writes a declared volume's files without a sandbox",
    Effect.gen(function* () {
      const body = yield* get<{ read: string; listed: string[]; size: number; exists: boolean }>(
        "/volume",
      );
      expect(body.read).toBe("written");
      expect(body.listed).toContain("a.txt");
      expect(body.size).toBe(7);
      expect(body.exists).toBe(false);
    }),
    { tags, timeout: 120_000 },
  );
});

describe("CreateVolume", () => {
  test.skipIf(!volumesEnabled)(
    "creates a volume by name, mounts it into a sandbox, and deletes it",
    Effect.gen(function* () {
      const name = `alchemy-${Test.defaultStage()}-vol`.toLowerCase().replace(/[^a-z0-9-]/g, "-");
      const body = yield* get<{ same: boolean; seen: string; back: string; gone: boolean }>(
        `/volumes?name=${name}`,
      );
      expect(body.same).toBe(true);
      expect(body.seen).toBe("from the volume api");
      expect(body.back).toBe("from the sandbox");
      expect(body.gone).toBe(true);
    }),
    { tags, timeout: 240_000 },
  );
});
