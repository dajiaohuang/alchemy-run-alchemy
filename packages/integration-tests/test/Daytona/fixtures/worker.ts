import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Daytona from "@alchemy.run/daytona";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import { DevBox } from "./resources.ts";

export default class DaytonaWorker extends Cloudflare.Worker<DaytonaWorker>()(
  "DaytonaWorker",
  { main: import.meta.url },
  Effect.gen(function* () {
    const sandboxes = yield* Daytona.CreateSandbox();
    const devBox = yield* Daytona.ConnectSandbox(DevBox);
    const volumes = yield* Daytona.CreateVolume({
      onDestroy: "delete",
      prefix: "alchemy-bindings-",
    });
    const secrets = yield* Daytona.CreateSecret();
    const images = yield* Daytona.CreateSnapshot();
    const organization = yield* Daytona.ReadOrganization();
    const usage = yield* Daytona.ReadUsage();
    const audit = yield* Daytona.ReadAuditLogs();

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const url = new URL(request.url, "http://worker");
        const prefix = url.searchParams.get("prefix") ?? "test";

        if (url.pathname === "/create-sandbox") {
          const sandbox = yield* Effect.acquireRelease(
            sandboxes.create({
              name: `alchemy-bindings-${prefix}`,
              labels: { purpose: "bindings-test" },
            }),
            (s) => sandboxes.delete(s.sandboxId).pipe(Effect.ignore),
          );
          const echo = yield* sandbox.exec("echo hello > /tmp/greeting && cat /tmp/greeting");
          const info = yield* sandbox.fs.getFileInfo({ path: "/tmp/greeting" });
          const found = yield* sandboxes.get(sandbox.name);
          // The list lags creation by a few seconds.
          const listed = yield* sandboxes.list({ labels: { purpose: "bindings-test" } }).pipe(
            Effect.repeat({
              schedule: Schedule.spaced("1 second"),
              until: (all) => all.some((s) => s.id === sandbox.sandboxId),
              times: 15,
            }),
          );
          return yield* HttpServerResponse.json({
            echo,
            size: info.size,
            found: found?.sandboxId === sandbox.sandboxId,
            listed: listed.some((s) => s.id === sandbox.sandboxId),
          });
        }

        if (url.pathname === "/create-sandbox-resources") {
          // A snapshot sizes the sandbox; Daytona refuses resources on create.
          const error = yield* sandboxes
            .create({ name: `alchemy-resources-${prefix}`, cpu: 2 })
            .pipe(Effect.flip);
          return yield* HttpServerResponse.json({ error: error._tag });
        }

        if (url.pathname === "/computer-use") {
          const sandbox = yield* Effect.acquireRelease(
            sandboxes.create({
              name: `alchemy-desktop-${prefix}`,
              labels: { purpose: "computer-use-test" },
            }),
            (s) => sandboxes.delete(s.sandboxId).pipe(Effect.ignore),
          );
          const desktop = sandbox.computerUse;
          yield* desktop.startComputerUse();
          // The X server and window manager take a few seconds to come up.
          const status = yield* desktop.getComputerUseStatus().pipe(
            Effect.repeat({
              schedule: Schedule.spaced("2 seconds"),
              until: (s) => s.status === "active",
              times: 20,
            }),
          );
          const display = yield* desktop.getDisplayInfo();
          yield* desktop.moveMouse({ x: 100, y: 120 });
          const mouse = yield* desktop.getMousePosition();
          const screenshot = yield* desktop.takeScreenshot({ showCursor: true });
          yield* desktop.stopComputerUse();
          return yield* HttpServerResponse.json({
            status: status.status,
            displays: display.displays?.length ?? 0,
            mouse: { x: mouse.x, y: mouse.y },
            screenshotBytes: screenshot.screenshot?.length ?? 0,
          });
        }

        if (url.pathname === "/connect-sandbox") {
          const uname = yield* devBox.exec("uname -s");
          const workDir = yield* devBox.info.getWorkDir();
          const preview = yield* devBox.getPortPreviewUrl({ port: 3000 });
          return yield* HttpServerResponse.json({ uname, workDir, previewUrl: preview.url });
        }

        if (url.pathname === "/volume") {
          const name = `alchemy-bindings-${prefix}`;
          const created = yield* volumes.getOrCreate({ name });
          const again = yield* volumes.getOrCreate({ name });
          const outside = yield* volumes
            .getOrCreate({ name: `elsewhere-${prefix}` })
            .pipe(Effect.flip);
          const listed = yield* volumes.list();
          yield* volumes.delete(created.volumeId);
          const after = yield* volumes.get(name);
          return yield* HttpServerResponse.json({
            same: created.volumeId === again.volumeId,
            state: created.state,
            rejected: outside._tag,
            listed: listed.some((v) => v.volumeId === created.volumeId),
            deleted: after === undefined,
          });
        }

        if (url.pathname === "/secret") {
          const name = `alchemy-bindings-${prefix}`;
          const first = yield* secrets.put({ name, value: "one", hosts: ["api.github.com"] });
          const second = yield* secrets.put({ name, value: "two", hosts: ["api.github.com"] });
          const found = yield* secrets.get(name);
          yield* secrets.delete(name);
          const after = yield* secrets.get(name);
          return yield* HttpServerResponse.json({
            same: first.secretId === second.secretId,
            placeholder: first.placeholder !== "",
            found: found?.secretId === first.secretId,
            deleted: after === undefined,
          });
        }

        if (url.pathname === "/snapshot") {
          const missing = yield* images.get(`alchemy-bindings-missing-${prefix}`);
          const listed = yield* images.list({ limit: 5 });
          return yield* HttpServerResponse.json({
            missing: missing === undefined,
            listed: Array.isArray(listed),
          });
        }

        if (url.pathname === "/organization") {
          const regions = yield* organization.listSharedRegions();
          const classes = yield* organization.listAvailableSandboxClasses();
          return yield* HttpServerResponse.json({
            regions: regions.length,
            classes: classes.length,
          });
        }

        if (url.pathname === "/usage") {
          const to = new Date().toISOString();
          const from = new Date(Date.now() - 86_400_000).toISOString();
          const total = yield* usage.getOrganizationUsageAggregated({ from, to });
          return yield* HttpServerResponse.json({ ok: total !== undefined });
        }

        if (url.pathname === "/audit") {
          const page = yield* audit.getOrganizationAuditLogs({ limit: 5 });
          return yield* HttpServerResponse.json({ items: page.items.length });
        }

        return HttpServerResponse.text("not found", { status: 404 });
      }).pipe(
        Effect.scoped,
        // Answer failures and defects with the cause so a failing test shows it.
        Effect.catchCause((cause) =>
          Effect.succeed(HttpServerResponse.text(Cause.pretty(cause), { status: 500 })),
        ),
      ),
    };
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Daytona.CreateSandboxHttp,
        Daytona.ConnectSandboxHttp,
        Daytona.CreateVolumeHttp,
        Daytona.CreateSecretHttp,
        Daytona.CreateSnapshotHttp,
        Daytona.ReadOrganizationHttp,
        Daytona.ReadUsageHttp,
        Daytona.ReadAuditLogsHttp,
      ),
    ),
  ),
) {}
