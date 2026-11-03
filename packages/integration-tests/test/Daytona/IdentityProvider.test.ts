import * as Daytona from "@alchemy.run/daytona";
import { expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as organizations from "@distilled.cloud/daytona/organizations";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";

const { test } = Test.make({ providers: Daytona.providers() });

test.provider(
  "register an OIDC identity provider, update its domains, delete it",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();
      // Daytona refuses SSO on a personal organization.
      const props = (emailDomains: string[]) =>
        Effect.gen(function* () {
          const organization = yield* Daytona.Organization("Team", { defaultRegionId: "us" });
          return yield* Daytona.IdentityProvider("Sso", {
            organization,
            issuerUrl: "https://accounts.google.com",
            clientId: "alchemy-test",
            clientSecret: Redacted.make("not-a-secret"),
            emailDomains,
            enabled: false,
          });
        });
      const created = yield* stack.deploy(props([`${stack.stage}.example.com`]));
      const updated = yield* stack.deploy(
        props([`${stack.stage}.example.com`, `${stack.stage}.example.org`]),
      );
      expect(updated.identityProviderId).toEqual(created.identityProviderId);
      expect([...updated.emailDomains].sort()).toEqual([
        `${stack.stage}.example.com`,
        `${stack.stage}.example.org`,
      ]);
      yield* stack.destroy();
      const all = yield* organizations
        .listOrganizationIdentityProviders({ organizationId: created.organizationId })
        .pipe(Effect.catchTag("OrganizationNotFound", () => Effect.succeed([])));
      expect(all.some((p) => p.id === created.identityProviderId)).toBe(false);
    }),
  { tags: ["provider:daytona", "live"], timeout: 120_000 },
);
