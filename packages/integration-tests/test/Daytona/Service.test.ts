import * as Daytona from "@alchemy.run/daytona";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as sandboxes from "@distilled.cloud/daytona/sandbox";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as Schedule from "effect/Schedule";
import Api from "./fixtures/service/api.ts";
import { ServiceBox } from "./fixtures/service/shared.ts";

const { test } = Test.make({ providers: Daytona.providers() });

const tags = ["provider:daytona", "provider:daytona:service", "live"];

/** GET a path of the service, retrying while the preview proxy warms up. */
const get = (url: string) =>
  HttpClient.get(url).pipe(
    Effect.flatMap((res) =>
      res.status === 200
        ? res.json
        : Effect.flatMap(res.text, (text) => Effect.fail(new Error(`${res.status}: ${text}`))),
    ),
    Effect.retry({ schedule: Schedule.spaced("2 seconds"), times: 15 }),
  );

const sandboxState = (id: string) =>
  sandboxes.getSandbox({ sandboxIdOrName: id }).pipe(
    Effect.map((s) => s.state),
    Effect.catchTag("SandboxNotFound", () => Effect.succeed("gone" as const)),
  );

test.provider.skipIf(!!process.env.FAST)(
  "an Effect program served from a sandbox: deploy, bindings, heal after a restart, destroy",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const program = Effect.gen(function* () {
        const box = yield* ServiceBox;
        const api = yield* Api;
        return { box, api };
      });

      const first = yield* stack.deploy(program);
      expect(first.api.sandboxId).toEqual(first.box.sandboxId);
      expect(first.api.port).toEqual(3000);
      expect(first.api.url).toContain(first.box.sandboxId);

      const served = (yield* get(first.api.url)) as { greeting: string; node: string };
      expect(served.greeting).toEqual("hello-from-daytona");
      expect(served.node).toMatch(/^v\d+/);

      // A binding the program uses at runtime: its own sandbox, through a minted key.
      expect(yield* get(`${first.api.url}/box`)).toEqual({
        sandboxId: first.box.sandboxId,
        state: "started",
      });

      // Redeploying unchanged leaves the running process alone.
      const again = yield* stack.deploy(program);
      expect(again.api.commandId).toEqual(first.api.commandId);

      // Stopping the sandbox ends the process; the next deploy starts both.
      yield* sandboxes.stopSandbox({ sandboxIdOrName: first.box.sandboxId });
      yield* sandboxState(first.box.sandboxId).pipe(
        Effect.repeat({
          schedule: Schedule.spaced("2 seconds"),
          until: (state) => state === "stopped",
          times: 30,
        }),
      );
      const healed = yield* stack.deploy(program);
      expect(healed.api.url).toEqual(first.api.url);
      expect(healed.api.commandId).not.toEqual(first.api.commandId);
      expect(((yield* get(healed.api.url)) as { greeting: string }).greeting).toEqual(
        "hello-from-daytona",
      );

      yield* stack.destroy();
      const gone = yield* sandboxState(first.box.sandboxId).pipe(
        Effect.repeat({
          schedule: Schedule.spaced("2 seconds"),
          until: (state) => state === "gone" || state === "destroyed",
          times: 30,
        }),
      );
      expect(["gone", "destroyed"]).toContain(gone);
    }),
  { tags, timeout: 600_000 },
);
