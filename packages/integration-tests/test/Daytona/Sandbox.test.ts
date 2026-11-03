import * as Daytona from "@alchemy.run/daytona";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as sandboxes from "@distilled.cloud/daytona/sandbox";
import * as Effect from "effect/Effect";

const { test } = Test.make({ providers: Daytona.providers() });

const observe = (id: string) =>
  sandboxes.getSandbox({ sandboxIdOrName: id }).pipe(
    Effect.map((s) => (s.state === "destroyed" ? undefined : s)),
    Effect.catchTag("SandboxNotFound", () => Effect.undefined),
  );

// Orgs below Daytona's Tier 3 can't set sandbox network rules; set
// DAYTONA_TEST_NETWORK=1 on an account that can.
const networkEntitled = !!process.env.DAYTONA_TEST_NETWORK;

test.provider(
  "create a sandbox, sync labels and intervals in place, stop it, replace it, delete it",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const created = yield* stack.deploy(
        Daytona.Sandbox("Box", {
          labels: { team: "alchemy" },
          autoStopInterval: "30 minutes",
        }),
      );
      expect(created.state).toEqual("started");
      expect(created.labels).toEqual({ team: "alchemy" });
      const observed = yield* sandboxes.getSandbox({ sandboxIdOrName: created.sandboxId });
      expect(observed.labels?.["alchemy.id"]).toContain("Box");
      expect(observed.autoStopInterval).toEqual(30);

      // Labels and intervals update in place; state stops it.
      const updated = yield* stack.deploy(
        Daytona.Sandbox("Box", {
          labels: { team: "alchemy", env: "test" },
          autoStopInterval: "1 hour",
          state: "stopped",
        }),
      );
      expect(updated.sandboxId).toEqual(created.sandboxId);
      expect(updated.state).toEqual("stopped");
      const after = yield* sandboxes.getSandbox({ sandboxIdOrName: created.sandboxId });
      expect(after.labels?.env).toEqual("test");
      expect(after.autoStopInterval).toEqual(60);

      // Environment variables are fixed at creation: changing them replaces the sandbox.
      const replaced = yield* stack.deploy(
        Daytona.Sandbox("Box", {
          labels: { team: "alchemy", env: "test" },
          env: { MODE: "replaced" },
          state: "stopped",
        }),
      );
      expect(replaced.sandboxId).not.toEqual(created.sandboxId);
      expect(yield* observe(created.sandboxId)).toBeUndefined();

      yield* stack.destroy();
      expect(yield* observe(replaced.sandboxId)).toBeUndefined();
    }),
  { tags: ["provider:daytona", "live"], timeout: 600_000 },
);

test.provider.skipIf(networkEntitled)(
  "network settings below Tier 3 fail with NetworkAccessRestricted",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      const error = yield* stack
        .deploy(Daytona.Sandbox("Restricted", { domainAllowList: ["github.com"] }))
        .pipe(Effect.flip);
      expect(error._tag).toEqual("NetworkAccessRestricted");
      yield* stack.destroy();
    }),
  { tags: ["provider:daytona", "live"], timeout: 120_000 },
);

test.provider.skipIf(!networkEntitled)(
  "domain allow list syncs in place",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const created = yield* stack.deploy(
        Daytona.Sandbox("Net", { domainAllowList: ["github.com"] }),
      );
      const observed = yield* sandboxes.getSandbox({ sandboxIdOrName: created.sandboxId });
      expect(observed.domainAllowList).toEqual("github.com");

      const updated = yield* stack.deploy(
        Daytona.Sandbox("Net", { domainAllowList: ["github.com", "registry.npmjs.org"] }),
      );
      expect(updated.sandboxId).toEqual(created.sandboxId);
      const after = yield* sandboxes.getSandbox({ sandboxIdOrName: created.sandboxId });
      expect((after.domainAllowList ?? "").split(",").sort()).toEqual([
        "github.com",
        "registry.npmjs.org",
      ]);

      yield* stack.destroy();
      expect(yield* observe(created.sandboxId)).toBeUndefined();
    }),
  { tags: ["provider:daytona", "live"], timeout: 600_000 },
);
