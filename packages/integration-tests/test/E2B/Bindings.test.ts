import * as Cloudflare from "@alchemy.run/cloudflare";
import * as E2B from "@alchemy.run/e2b";
import { describe, expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as sandboxes from "@distilled.cloud/e2b/sandboxes";
import * as Alchemy from "alchemy";
import { Stack as StackService } from "alchemy/Stack";
import { Stage } from "alchemy/Stage";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import { BindingAgent, ProbeWebhook, RotatedSecret } from "./fixtures/resources.ts";
import E2BWorker from "./fixtures/worker.ts";

const providers = Layer.mergeAll(E2B.providers(), Cloudflare.providers());
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({ providers });

const Stack = Alchemy.Stack(
  "E2BBindingsStack",
  { providers, state: Cloudflare.state() },
  Effect.gen(function* () {
    const agent = yield* BindingAgent;
    yield* RotatedSecret;
    yield* ProbeWebhook;
    const worker = yield* E2BWorker;
    return { url: worker.url.as<string>(), templateId: agent.templateId };
  }),
);

const stack = beforeAll(deploy(Stack));
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

const tags = ["provider:e2b", "provider:cloudflare", "provider:cloudflare:worker", "live"];

/** A stage-unique name for objects the fixture creates at runtime. */
const unique = (suffix: string) =>
  Effect.succeed(
    `alchemy-${Test.defaultStage()}-${suffix}`.toLowerCase().replace(/[^a-z0-9-]/g, "-"),
  );

// Out-of-band verification resolves E2B credentials the way the stack does:
// through the Alchemy profile, via `E2B.providers()`.
const distilled = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.provide(
      E2B.providers().pipe(
        Layer.provideMerge(
          Layer.succeed(StackService, {
            name: "E2BBindingsVerify",
            stage: Test.defaultStage(),
            resources: {},
            bindings: {},
            actions: {},
          }),
        ),
        Layer.provideMerge(Layer.succeed(Stage, Test.defaultStage())),
      ),
    ),
  );

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

describe("CreateSandbox", () => {
  test(
    "runs commands and file operations in a sandbox from the bound template",
    Effect.gen(function* () {
      const { templateId } = yield* stack;
      const body = yield* get<{
        templateId: string;
        marker: string;
        ok: { stdout: string; stderr: string; exitCode: number };
        failedExitCode: number;
        exec: string;
        read: string;
        listed: string[];
        movedExists: boolean;
        removedExists: boolean;
        events: string[];
      }>("/sandbox/commands");
      expect(body.templateId).toBe(templateId);
      expect(body.marker.trim()).toBe("bound");
      expect(body.ok).toMatchObject({ stdout: "out\n", stderr: "err\n", exitCode: 0 });
      expect(body.failedExitCode).toBe(3);
      expect(body.exec).toBe("argv");
      expect(body.read).toBe("hello");
      expect(body.listed).toContain("a.txt");
      expect(body.movedExists).toBe(true);
      expect(body.removedExists).toBe(false);
      expect(body.events[0]).toBe("Start");
      expect(body.events).toContain("Stdout");
      expect(body.events.at(-1)).toBe("End");
    }),
    { tags, timeout: 240_000 },
  );

  test(
    "finds a sandbox by metadata, pauses, resumes with its files, and kills it",
    Effect.gen(function* () {
      const task = yield* unique("lifecycle");
      const body = yield* get<{
        foundIds: string[];
        sandboxId: string;
        pausedState: string;
        resumedState: string;
        metadata: Record<string, string>;
        state: string;
        killedAgain: string;
      }>(`/sandbox/lifecycle?task=${task}`);
      expect(body.foundIds).toContain(body.sandboxId);
      expect(body.pausedState).toBe("paused");
      expect(body.resumedState).toBe("running");
      expect(body.state).toBe("kept");
      expect(body.metadata.task).toBe(task);
      expect(body.metadata["alchemy::host"]).toBe("E2BWorker");
      expect(body.killedAgain).toBe("ok");
      const gone = yield* distilled(
        sandboxes.getSandbox({ sandboxID: body.sandboxId }).pipe(
          Effect.map((s): string => s.state),
          Effect.catchTag("NotFound", () => Effect.succeed("gone")),
        ),
      );
      expect(gone).toBe("gone");
    }),
    { tags, timeout: 240_000 },
  );

  test(
    "forks a running sandbox; the fork sees the parent's files",
    Effect.gen(function* () {
      const body = yield* get<{ parent: string; fork: string; content: string }>("/sandbox/fork");
      expect(body.fork).not.toBe(body.parent);
      expect(body.content).toBe("from parent");
    }),
    { tags, timeout: 240_000 },
  );

  test(
    "snapshots a sandbox and creates a new sandbox from the snapshot",
    Effect.gen(function* () {
      const name = yield* unique("snap");
      const body = yield* get<{ snapshotId: string; listed: number; content: string }>(
        `/sandbox/snapshot?name=${name}`,
      );
      expect(body.listed).toBeGreaterThanOrEqual(1);
      expect(body.content).toBe("snapshotted");
    }),
    { tags, timeout: 300_000 },
  );
});

describe("CreateSnapshot", () => {
  test(
    "takes a warm snapshot from setup commands and boots a sandbox from it",
    Effect.gen(function* () {
      const name = yield* unique("warm");
      const body = yield* get<{ content: string }>(`/snapshots?name=${name}`);
      expect(body.content.trim()).toBe("warm");
    }),
    { tags, timeout: 300_000 },
  );
});

describe("CreateSecret", () => {
  test(
    "injects a runtime secret at egress without exposing it inside the sandbox",
    Effect.gen(function* () {
      const name = yield* unique("secret");
      const body = yield* get<{
        ref: string;
        listed: boolean;
        injected: boolean;
        leaked: boolean;
        gone: boolean;
      }>(`/secrets?name=${name}`);
      expect(body.ref).toBe(`\${e2b.secrets.${name}}`);
      expect(body.listed).toBe(true);
      expect(body.injected).toBe(true);
      expect(body.leaked).toBe(false);
      expect(body.gone).toBe(true);
    }),
    { tags, timeout: 240_000 },
  );
});

describe("WriteSecret", () => {
  test(
    "rotates a declared secret to new versions",
    Effect.gen(function* () {
      const body = yield* get<{ first: number; second: number }>("/rotate");
      expect(body.second).toBe(body.first + 1);
    }),
    { tags, timeout: 120_000 },
  );
});

describe("CreateTemplate", () => {
  test(
    "builds a template at runtime and reuses the build for the same inputs",
    Effect.gen(function* () {
      const name = yield* unique("tpl");
      const body = yield* get<{ sameBuild: boolean; greeting: string }>(`/templates?name=${name}`);
      expect(body.sameBuild).toBe(true);
      expect(body.greeting).toBe("built at runtime");
    }),
    { tags, timeout: 600_000 },
  );
});

describe("ReadTeam", () => {
  test(
    "reads the team's sandbox event log",
    Effect.gen(function* () {
      const body = yield* get<{ count: number }>("/team");
      expect(body.count).toBeGreaterThan(0);
    }),
    { tags, timeout: 120_000 },
  );
});

describe("ReadWebhook", () => {
  test(
    "reads a declared webhook's delivery history and stats",
    Effect.gen(function* () {
      const body = yield* get<{ groups: number; total: number }>("/webhook");
      // A wrong or missing webhook id fails the route with NotFound.
      expect(typeof body.groups).toBe("number");
      expect(typeof body.total).toBe("number");
    }),
    { tags, timeout: 120_000 },
  );
});
