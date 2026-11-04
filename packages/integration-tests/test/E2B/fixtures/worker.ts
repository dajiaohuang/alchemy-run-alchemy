import * as Cloudflare from "@alchemy.run/cloudflare";
import * as E2B from "@alchemy.run/e2b";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
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
        // The sandbox handle: processes, stdin, PTYs, files, ports, logs, network.
        "/sandbox/handle": () =>
          Effect.gen(function* () {
            const sandbox = yield* scopedSandbox();
            const text = (event: E2B.ProcessEvent) =>
              event._tag === "Stdout" || event._tag === "Pty"
                ? new TextDecoder().decode(event.data)
                : "";

            // Files: mkdir, compose, read bytes, stat, watch.
            yield* sandbox.files.makeDir("/home/user/h");
            yield* sandbox.files.write("/home/user/h/a.txt", "a");
            yield* sandbox.files.write("/home/user/h/b.txt", "b");
            yield* sandbox.files.compose(
              ["/home/user/h/a.txt", "/home/user/h/b.txt"],
              "/home/user/h/ab.txt",
            );
            const composed = new TextDecoder().decode(
              yield* sandbox.files.read("/home/user/h/ab.txt"),
            );
            const stat = yield* sandbox.files.stat("/home/user/h/ab.txt");
            const watcher = yield* sandbox.files.watch("/home/user/h").pipe(
              Stream.filter((event) => event._tag === "Filesystem"),
              Stream.take(1),
              Stream.runCollect,
              Effect.forkScoped,
            );
            yield* Effect.sleep("1 second");
            yield* sandbox.files.write("/home/user/h/watched.txt", "w");
            const watched = yield* Fiber.join(watcher).pipe(Effect.timeout("20 seconds"));

            // Processes: stdin, list, re-attach, close stdin, kill.
            const cat = yield* sandbox.commands
              .start("cat", { tag: "cat", stdin: true })
              .pipe(Stream.map(text), Stream.mkString, Effect.forkScoped);
            yield* Effect.sleep("1 second");
            const reattached = yield* sandbox.commands
              .connect({ tag: "cat" })
              .pipe(Stream.map(text), Stream.mkString, Effect.forkScoped);
            yield* Effect.sleep("500 millis");
            const running = yield* sandbox.commands.list();
            yield* sandbox.commands.sendStdin({ tag: "cat" }, "ping\n");
            yield* sandbox.commands.closeStdin({ tag: "cat" });
            const catOut = yield* Fiber.join(cat).pipe(Effect.timeout("20 seconds"));
            const reattachedOut = yield* Fiber.join(reattached).pipe(Effect.timeout("20 seconds"));
            yield* sandbox.commands
              .start("sleep 300", { tag: "sleeper" })
              .pipe(Stream.take(1), Stream.runDrain);
            yield* sandbox.commands.kill({ tag: "sleeper" });
            yield* Effect.sleep("500 millis");
            const afterKill = yield* sandbox.commands.list();

            // A PTY: type, resize, read the output.
            const pty = yield* sandbox.pty
              .create({ cols: 80, rows: 24, tag: "pty" })
              .pipe(Stream.map(text), Stream.mkString, Effect.forkScoped);
            yield* Effect.sleep("1 second");
            yield* sandbox.pty.resize({ tag: "pty" }, { cols: 100, rows: 30 });
            yield* sandbox.pty.sendInput({ tag: "pty" }, "echo pty-$((6*7)); stty size; exit\n");
            const ptyOut = yield* Fiber.join(pty).pipe(Effect.timeout("20 seconds"));

            // Ports: a server in the sandbox, reached through its URL.
            yield* sandbox.commands.run(
              "nohup python3 -m http.server 8000 --directory /home/user/h >/dev/null 2>&1 &",
            );
            const served = yield* sandbox.fetch(8000, HttpClientRequest.get("/ab.txt")).pipe(
              Effect.flatMap((res) =>
                res.status === 200 ? res.text : Effect.fail(new Error(`status ${res.status}`)),
              ),
              Effect.retry({ schedule: Schedule.spaced("1 second"), times: 15 }),
            );

            // Lifetime, network, logs.
            yield* sandbox.refresh("1 minute");
            yield* sandbox.updateNetwork({ allowOut: ["github.com"] });
            const detail = yield* sandbox.describe();
            const logs = yield* sandbox.logs({ limit: 10 });

            return {
              composed,
              statName: stat.name,
              watched: Array.from(watched).map((event) =>
                event._tag === "Filesystem" ? event.name : "",
              ),
              listedCat: running.some((p) => p.tag === "cat"),
              catOut,
              reattachedOut,
              sleeperKilled: !afterKill.some((p) => p.tag === "sleeper"),
              ptyOut,
              served,
              host: sandbox.getHost(8000),
              url: sandbox.url(8000),
              allowOut: detail.network?.allowOut ?? [],
              logs: logs.logs.length,
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
            // Reads and tags on the runtime-built template.
            const found = yield* templates.get(first.templateId);
            const listed = yield* templates.list();
            const defaults = yield* templates.listDefaults();
            const searched = yield* templates.search({ search: name });
            const status = yield* templates.buildStatus(first.templateId, first.buildId);
            const logs = yield* templates.buildLogs(first.templateId, first.buildId);
            yield* templates.setPublic(first.templateId, false);
            yield* templates.tags.assign(name, ["v1"]);
            const tagged = yield* templates.tags.exists(first.templateId, "v1");
            const tagCount = yield* templates.tags.count(first.templateId);
            const tagList = yield* templates.tags.list(first.templateId);
            const groups = yield* templates.tags.groups(first.templateId);
            const assignments = yield* templates.tags.assignments(first.templateId, "v1");
            yield* templates.tags.remove(name, ["v1"]);
            const untagged = yield* templates.tags.exists(first.templateId, "v1");
            yield* templates.delete(first.templateId);
            return {
              sameBuild: first.buildId === second.buildId,
              greeting,
              foundBuild: found?.buildID === first.buildId,
              listed: listed.some((t) => t.templateID === first.templateId),
              defaults: defaults.length,
              searched: searched.data.length,
              status: status.status,
              logs: logs.logs.length,
              tagged,
              tagCount,
              tags: tagList.map((t) => t.tag),
              groups: groups.tags.length,
              assignments: assignments.data.length,
              untagged,
            };
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
            const sandboxId = events[0]?.sandboxId;
            const sandboxEvents =
              sandboxId === undefined ? [] : yield* team.sandboxEvents(sandboxId, { limit: 5 });
            const now = Math.floor(Date.now() / 1000);
            const metrics = yield* team.metrics({ start: now - 3600, end: now });
            const peak = yield* team.maxMetric({
              metric: "concurrent_sandboxes",
              start: now - 3600,
              end: now,
            });
            return {
              count: events.length,
              sandboxEvents: sandboxEvents.length,
              teamId: yield* team.teamId,
              metrics: metrics.length,
              peak: peak.value,
            };
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
