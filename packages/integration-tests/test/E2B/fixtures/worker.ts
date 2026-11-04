import * as Cloudflare from "@alchemy.run/cloudflare";
import * as E2B from "@alchemy.run/e2b";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import { BindingAgent, BindingBox, ProbeWebhook, RotatedSecret } from "./resources.ts";

const BindingsLive = Layer.mergeAll(
  E2B.CreateSandboxHttp,
  E2B.ConnectSandboxHttp,
  E2B.CreateSecretHttp,
  E2B.CreateTemplateHttp,
  E2B.CreateSnapshotHttp,
  E2B.WriteSecretHttp,
  E2B.ReadTeamHttp,
  E2B.ReadWebhookHttp,
);

export default class E2BWorker extends Cloudflare.Worker<E2BWorker>()(
  "E2BWorker",
  { main: import.meta.url },
  Effect.gen(function* () {
    const sandboxes = yield* E2B.CreateSandbox(BindingAgent);
    const anySandboxes = yield* E2B.CreateSandbox();
    const declaredBox = yield* BindingBox;
    const box = yield* E2B.ConnectSandbox(declaredBox);
    const secrets = yield* E2B.CreateSecret();
    const templates = yield* E2B.CreateTemplate();
    const snapshots = yield* E2B.CreateSnapshot();
    const rotate = yield* E2B.WriteSecret(RotatedSecret);
    const team = yield* E2B.ReadTeam();
    const probe = yield* E2B.ReadWebhook(ProbeWebhook);

    /** A sandbox from the bound template, killed when the request ends. */
    const scopedSandbox = (options?: E2B.CreateSandboxOptions) =>
      Effect.acquireRelease(sandboxes.create({ timeout: 300, ...options }), (sandbox) =>
        Effect.ignore(sandbox.kill()),
      );

    const routes: Record<string, (query: URLSearchParams) => Effect.Effect<unknown, unknown, any>> =
      {
        // CreateSandbox(base): commands and files through envd.
        "/sandbox/commands": () =>
          Effect.gen(function* () {
            const sandbox = yield* scopedSandbox();
            const marker = yield* sandbox.files.readText("/home/user/marker.txt");
            const ok = yield* sandbox.commands.run("echo out; echo err >&2");
            const failed = yield* sandbox.commands.run("exit 3");
            const exec = yield* sandbox.commands.exec(["printf", "%s", "argv"]);
            yield* sandbox.files.write("/home/user/dir/a.txt", "hello");
            const read = yield* sandbox.files.readText("/home/user/dir/a.txt");
            const listed = yield* sandbox.files.list("/home/user/dir");
            yield* sandbox.files.move("/home/user/dir/a.txt", "/home/user/dir/b.txt");
            const movedExists = yield* sandbox.files.exists("/home/user/dir/b.txt");
            yield* sandbox.files.remove("/home/user/dir/b.txt");
            const removedExists = yield* sandbox.files.exists("/home/user/dir/b.txt");
            const events = yield* sandbox.commands.start("echo streamed").pipe(
              Stream.map((event) => event._tag),
              Stream.runCollect,
            );
            return {
              templateId: sandbox.templateId,
              marker,
              ok,
              failedExitCode: failed.exitCode,
              exec: exec.stdout,
              read,
              listed: listed.map((entry) => entry.name),
              movedExists,
              removedExists,
              events: Array.from(events),
            };
          }),
        // CreateSandbox(base): lifecycle — metadata lookup, pause, resume, timeout, kill.
        "/sandbox/lifecycle": (query) =>
          Effect.gen(function* () {
            const task = query.get("task") ?? "lifecycle";
            const sandbox = yield* sandboxes.create({ timeout: 300, metadata: { task } });
            yield* sandbox.files.write("/home/user/state.txt", "kept");
            const found = yield* sandboxes.list({ metadata: { task } });
            yield* sandbox.setTimeout("10 minutes");
            yield* sandbox.pause();
            const paused = yield* sandboxes.get(sandbox.sandboxId);
            const resumed = yield* sandboxes.connect(sandbox.sandboxId, { timeout: 300 });
            const state = yield* resumed.files.readText("/home/user/state.txt");
            const detail = yield* resumed.describe();
            yield* resumed.kill();
            const killedAgain = yield* sandboxes.kill(sandbox.sandboxId).pipe(Effect.as("ok"));
            return {
              foundIds: found.map((s) => s.sandboxID),
              sandboxId: sandbox.sandboxId,
              pausedState: paused.state,
              resumedState: detail.state,
              metadata: detail.metadata,
              state,
              killedAgain,
            };
          }),
        // ConnectSandbox: the declared sandbox, resumed on connect.
        "/box/run": () =>
          Effect.gen(function* () {
            const sandbox = yield* box.connect({ timeout: "5 minutes" });
            const marker = yield* sandbox.files.readText("/home/user/marker.txt");
            yield* sandbox.files.write("/home/user/box-state.txt", "kept");
            return { sandboxId: yield* box.sandboxId, marker };
          }),
        "/box/pause": () =>
          Effect.gen(function* () {
            yield* box.pause();
            const paused = yield* box.describe();
            const resumed = yield* box.connect({ timeout: "5 minutes" });
            const state = yield* resumed.files.readText("/home/user/box-state.txt");
            const detail = yield* box.describe();
            return { pausedState: paused.state, resumedState: detail.state, state };
          }),
        // CreateSandbox.metrics: several sandboxes in one call.
        "/sandbox/metrics": () =>
          Effect.gen(function* () {
            const sandbox = yield* scopedSandbox();
            // E2B reports a new sandbox's first metrics after a few seconds.
            const metrics = yield* sandboxes.metrics([sandbox.sandboxId]).pipe(
              Effect.repeat({
                schedule: Schedule.spaced("2 seconds"),
                until: (m) => m.sandboxes[sandbox.sandboxId] !== undefined,
                times: 15,
              }),
            );
            return { sandboxId: sandbox.sandboxId, ids: Object.keys(metrics.sandboxes) };
          }),
        // Fork: the copy sees the parent's files.
        "/sandbox/fork": () =>
          Effect.gen(function* () {
            const sandbox = yield* scopedSandbox();
            yield* sandbox.files.write("/home/user/forked.txt", "from parent");
            const [result] = yield* sandbox.fork({ count: 1, timeout: "5 minutes" });
            if (result?._tag !== "Forked") return { error: result };
            const content = yield* result.sandbox.files.readText("/home/user/forked.txt");
            yield* result.sandbox.kill();
            return { parent: sandbox.sandboxId, fork: result.sandbox.sandboxId, content };
          }),
        // sandbox.snapshot + create from it + CreateSnapshot list/delete.
        "/sandbox/snapshot": (query) =>
          Effect.gen(function* () {
            const name = query.get("name") ?? "alchemy-binding-snapshot";
            const sandbox = yield* scopedSandbox();
            yield* sandbox.files.write("/home/user/snap.txt", "snapshotted");
            const snapshot = yield* sandbox.snapshot({ name });
            const listed = yield* snapshots.list({ name });
            const restored = yield* Effect.acquireRelease(
              anySandboxes.create({ templateID: snapshot.snapshotID, timeout: 300 }),
              (s) => Effect.ignore(s.kill()),
            );
            const content = yield* restored.files.readText("/home/user/snap.txt");
            // E2B keeps a snapshot while a sandbox runs on it.
            yield* restored.kill();
            yield* snapshots.delete(name);
            return { snapshotId: snapshot.snapshotID, listed: listed.length, content };
          }),
        // CreateSnapshot.create: boot, set up, snapshot, kill.
        "/snapshots": (query) =>
          Effect.gen(function* () {
            const name = query.get("name") ?? "alchemy-binding-warm";
            const info = yield* snapshots.create(name, {
              template: query.get("template") ?? "base",
              run: ["echo warm > /home/user/warm.txt"],
            });
            const sandbox = yield* Effect.acquireRelease(
              anySandboxes.create({ templateID: info.snapshotID, timeout: 300 }),
              (s) => Effect.ignore(s.kill()),
            );
            const content = yield* sandbox.files.readText("/home/user/warm.txt");
            yield* sandbox.kill();
            yield* snapshots.delete(name);
            return { content };
          }),
        // CreateSecret: a runtime secret injected at egress, never visible in the sandbox.
        "/secrets": (query) =>
          Effect.gen(function* () {
            const name = query.get("name") ?? "alchemy-binding-secret";
            const value = "s3cr3t-value";
            const secret = yield* secrets.put(name, Redacted.make(value));
            const listed = yield* secrets.list();
            const sandbox = yield* scopedSandbox({
              network: {
                allowOut: ["postman-echo.com"],
                rules: {
                  "postman-echo.com": [
                    { transform: { headers: { "X-Alchemy-Secret": secret.ref } } },
                  ],
                },
              },
            });
            const echoed = yield* sandbox.commands.run("curl -s https://postman-echo.com/headers");
            const env = yield* sandbox.commands.run("env");
            yield* secrets.delete(name);
            const gone = yield* secrets.get(name);
            return {
              ref: secret.ref,
              listed: listed.some((s) => s.name === secret.name),
              injected: echoed.stdout.includes(value),
              leaked: env.stdout.includes(value),
              gone: gone === undefined,
            };
          }),
        // WriteSecret: rotate a declared secret.
        "/rotate": () =>
          Effect.gen(function* () {
            const first = yield* rotate.put(Redacted.make("second"));
            const second = yield* rotate.put(Redacted.make("third"));
            return { first: first.version, second: second.version };
          }),
        // CreateTemplate: a runtime build, then the cached build.
        "/templates": (query) =>
          Effect.gen(function* () {
            const name = query.get("name") ?? "alchemy-binding-template";
            const spec = {
              name,
              fromTemplate: "base",
              files: [{ path: "/home/user/greeting.txt", contents: "built at runtime" }],
            };
            const first = yield* templates.getOrBuild(spec);
            const second = yield* templates.getOrBuild(spec);
            const sandbox = yield* Effect.acquireRelease(
              anySandboxes.create({ templateID: first.templateId, timeout: 300 }),
              (s) => Effect.ignore(s.kill()),
            );
            const greeting = yield* sandbox.files.readText("/home/user/greeting.txt");
            yield* sandbox.kill();
            yield* templates.delete(first.templateId);
            return { sameBuild: first.buildId === second.buildId, greeting };
          }),
        // Leaves a sandbox and a secret behind, as a crashed request would; the
        // host's sweep removes them when the host is destroyed.
        "/leftover": (query) =>
          Effect.gen(function* () {
            const name = query.get("name") ?? "alchemy-binding-leftover";
            const sandbox = yield* sandboxes.create({ timeout: 600, metadata: { leftover: name } });
            const secret = yield* secrets.put(name, Redacted.make("left behind"));
            return { sandboxId: sandbox.sandboxId, secretId: secret.secretId };
          }),
        // ReadWebhook: delivery history and stats of a declared webhook.
        "/webhook": () =>
          Effect.gen(function* () {
            const deliveries = yield* probe.deliveries({});
            const stats = yield* probe.stats({});
            return { groups: deliveries.data.length, total: stats.total };
          }),
        // ReadTeam: the lifecycle event log.
        "/team": () =>
          Effect.gen(function* () {
            const events = yield* team.events({ limit: 5 });
            return { count: events.length };
          }),
      };

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const url = new URL(request.url, "http://worker");
        const route = routes[url.pathname];
        if (route === undefined) return HttpServerResponse.text("not found", { status: 404 });
        return yield* route(url.searchParams).pipe(
          Effect.scoped,
          Effect.flatMap((body) => HttpServerResponse.json(body)),
          Effect.catchCause((cause) =>
            HttpServerResponse.json({ error: String(cause) }, { status: 500 }),
          ),
        );
      }),
    };
  }).pipe(Effect.provide(BindingsLive)),
) {}
