import { DEFAULT_DOMAIN } from "@distilled.cloud/e2b/Credentials";
import { AuthError } from "alchemy/Auth/AuthProvider";
import { getEnv, getEnvRedacted } from "alchemy/Auth/Env";
import {
  makeStoredAuthProvider,
  storedSecret,
  storedValueText,
  type StoredAuthConfig,
} from "alchemy/Auth/StoredAuthProvider";
import * as Effect from "effect/Effect";
import type * as Redacted from "effect/Redacted";

export const E2B_AUTH_PROVIDER_NAME = "E2B";

export type E2BAuthConfig = StoredAuthConfig;

/**
 * Resolved E2B credentials.
 *
 * `accessToken` is a personal access token: it reaches every control-plane
 * endpoint, and it is the only credential that can mint API keys, which
 * bindings and `E2B.ApiKey` need. `teamId` is looked up from the token when
 * unset. `apiKey` is a team API key: enough for resources, not for bindings.
 */
export interface E2BResolvedCredentials {
  readonly apiKey: Redacted.Redacted<string> | undefined;
  readonly accessToken: Redacted.Redacted<string> | undefined;
  readonly teamId: string | undefined;
  readonly domain: string;
  readonly source: { readonly type: E2BAuthConfig["method"] | "env"; readonly details?: string };
}

const readEnvironment = Effect.gen(function* () {
  const apiKey = yield* getEnvRedacted("E2B_API_KEY");
  const accessToken = yield* getEnvRedacted("E2B_ACCESS_TOKEN");
  if (!apiKey && !accessToken) {
    return yield* AuthError.make({
      message:
        "E2B CI credentials not found. Set E2B_ACCESS_TOKEN (or E2B_API_KEY for resources only).",
    });
  }
  return {
    apiKey,
    accessToken,
    teamId: yield* getEnv("E2B_TEAM_ID"),
    domain: (yield* getEnv("E2B_DOMAIN")) ?? DEFAULT_DOMAIN,
    source: { type: "env" as const },
  } satisfies E2BResolvedCredentials;
});

const e2bAuth = makeStoredAuthProvider<E2BResolvedCredentials>({
  provider: E2B_AUTH_PROVIDER_NAME,
  fields: [
    {
      name: "accessToken",
      label:
        "E2B access token (sk_e2b_…, accessToken in ~/.e2b/config.json after `e2b auth login`)",
      secret: true,
      optional: true,
    },
    {
      name: "teamId",
      label: "E2B team id, if not your default team",
      optional: true,
    },
    {
      name: "apiKey",
      label: "E2B API key (e2b_…), only if you have no access token; bindings need the token",
      secret: true,
      optional: true,
    },
    {
      name: "domain",
      label: "E2B domain",
      optional: true,
      placeholder: DEFAULT_DOMAIN,
    },
  ],
  toResolved: (values, source) => ({
    apiKey: storedSecret(values.apiKey),
    accessToken: storedSecret(values.accessToken),
    teamId: storedValueText(values.teamId),
    domain: storedValueText(values.domain) ?? DEFAULT_DOMAIN,
    source: { type: source },
  }),
  readEnvironment,
  environment: [
    {
      name: "E2B_ACCESS_TOKEN",
      required: false,
      secret: true,
      description:
        "Personal access token. Reaches every endpoint and mints the API keys bindings and E2B.ApiKey need.",
    },
    {
      name: "E2B_TEAM_ID",
      required: false,
      description: "Team id sent with the access token. Defaults to the token's default team.",
    },
    {
      name: "E2B_API_KEY",
      required: false,
      secret: true,
      description:
        "Team API key, sent as X-API-Key. Enough for resources; bindings need the access token.",
    },
    {
      name: "E2B_DOMAIN",
      required: false,
      description: "E2B domain for self-hosted deployments.",
    },
  ],
});

/**
 * Layer that registers the E2B {@link AuthProvider} into the
 * {@link AuthProviders} registry when built. Included in `E2B.providers()`
 * so `alchemy profile edit --add E2B` can configure it.
 */
export const E2BAuth = e2bAuth.layer;

/** Schema of E2B's inline static-token values. */
export const E2BStoredCredentials = e2bAuth.storedSchema;
export type E2BStoredCredentials = typeof E2BStoredCredentials.Type;
