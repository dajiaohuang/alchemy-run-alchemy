/**
 * How E2B bindings reach E2B from any host.
 *
 * E2B authenticates with a team API key and has no identity federation or
 * key scopes, so every host — a Cloudflare Worker, a Lambda, a container, an
 * E2B sandbox — gets its own API key, minted at deploy as an `E2B.ApiKey` in
 * the host's namespace and deleted with the host. The deployer's credentials
 * never reach a host. Minting takes the deployer's access token. The binding
 * captures the key (and the E2B domain) as named runtime Outputs; inside the
 * deployed host they are read back from the host's env, still `Redacted`.
 * Every runtime call provides `Credentials` from those accessors, so nothing
 * reads `process.env`.
 *
 * Bindings also capture an ownership stamp (`alchemy::stack`,
 * `alchemy::stage`, `alchemy::host`) that runtime-created sandboxes and
 * secrets carry in their metadata, so the host's sweep can delete them when
 * the host is destroyed.
 */
import { ConfigError } from "@distilled.cloud/core/errors";
import {
  apiBaseUrlFor,
  Credentials,
  CredentialsFromEnv,
  DEFAULT_DOMAIN,
} from "@distilled.cloud/e2b/Credentials";
import { orDieCredentialsUnavailable } from "alchemy/Auth/Resolve";
import * as Binding from "alchemy/Binding";
import * as Namespace from "alchemy/Namespace";
import * as Output from "alchemy/Output";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Stack } from "alchemy/Stack";
import { Stage } from "alchemy/Stage";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import type * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import { ApiKey } from "./ApiKey.ts";
import { withTeamId } from "./Credentials.ts";

/** A plain value as an Output with no requirements (`asOutput` leaves them `any`). */
const output = <A>(value: A) => Output.asOutput(value) as Output.Output<A, never>;

/** Metadata stamped on objects a host creates at runtime. */
export type RuntimeStamp = Readonly<Record<string, string>>;

export interface E2BAuth {
  /** Runs a distilled call with the bound credentials and an HTTP client. */
  readonly authorize: <A, E>(
    effect: Effect.Effect<A, E, Credentials | HttpClient.HttpClient>,
  ) => Effect.Effect<A, E, RuntimeContext>;
  /** The ownership stamp for objects this host creates. */
  readonly stamp: Effect.Effect<RuntimeStamp>;
  /** The same stamp as plain data, for the host's sweep; empty at runtime. */
  readonly deployStamp: RuntimeStamp;
  /** The E2B domain sandboxes are addressed under. */
  readonly domain: Effect.Effect<string>;
  /** The team the host's key belongs to. */
  readonly teamId: Effect.Effect<string>;
}

/** The deploying stack's credentials: the ambient profile-backed ones, else the env. */
const deployCredentials = Effect.gen(function* () {
  const ambient = yield* Effect.serviceOption(Credentials);
  const resolve = Option.isSome(ambient)
    ? ambient.value
    : yield* Credentials.pipe(Effect.provide(CredentialsFromEnv));
  return yield* resolve;
});

const requireAccessToken = (config: { readonly accessToken?: Redacted.Redacted<string> }) =>
  config.accessToken === undefined
    ? Effect.fail(
        new ConfigError({
          message:
            "E2B bindings mint an API key for each host, which takes an E2B access token: " +
            "add accessToken to the E2B profile (alchemy profile edit --add E2B) or set E2B_ACCESS_TOKEN",
        }),
      )
    : Effect.void;

/**
 * Mints the current host's API key and binds it, the domain and the
 * ownership stamp onto the host, and returns runtime accessors for them.
 * Call it in a binding's construction; every binding on a host shares the
 * one key.
 */
export const bindE2B = Effect.gen(function* () {
  // Yielding the resource class erases its `Providers` requirement; the
  // stack satisfies it at plan time.
  const makeKey = yield* ApiKey;
  let apiKey: Output.Output<Redacted.Redacted<string>, never> = output(Redacted.make(""));
  let domain = DEFAULT_DOMAIN;
  let teamId = "";
  let stamp: RuntimeStamp = {};
  if (!globalThis.__ALCHEMY_RUNTIME__) {
    const config = yield* deployCredentials.pipe(orDieCredentialsUnavailable("E2B"));
    yield* requireAccessToken(config).pipe(orDieCredentialsUnavailable("E2B"));
    domain = config.domain;
    teamId = (yield* withTeamId(config).pipe(orDieCredentialsUnavailable("E2B"))).teamId ?? "";
    const host = yield* Binding.Host;
    if (host !== undefined) {
      const hostKey = yield* Namespace.push(host.LogicalId, makeKey("E2BHostKey", {}));
      apiKey = hostKey.key as Output.Output<Redacted.Redacted<string>, never>;
    }
    const stack = yield* Effect.serviceOption(Stack);
    const stage = yield* Effect.serviceOption(Stage);
    stamp = {
      "alchemy::stack": Option.match(stack, { onNone: () => "", onSome: (s) => s.name }),
      "alchemy::stage": Option.getOrElse(stage, () => ""),
      "alchemy::host": host?.LogicalId ?? "",
    };
  }
  const key = yield* Output.named(apiKey, "E2B_API_KEY").asEffect();
  const boundDomain = yield* Output.named(output(domain), "E2B_DOMAIN").asEffect();
  const boundStamp = yield* Output.named(output(JSON.stringify(stamp)), "E2B_STAMP").asEffect();
  const boundTeamId = yield* Output.named(output(teamId), "E2B_TEAM_ID").asEffect();

  const domainOf = Effect.map(boundDomain, (value) => value || DEFAULT_DOMAIN);
  const credentials = Effect.gen(function* () {
    const resolvedDomain = yield* domainOf;
    return {
      apiKey: yield* key,
      domain: resolvedDomain,
      apiBaseUrl: apiBaseUrlFor(resolvedDomain),
    };
  });
  const services = Layer.mergeAll(Layer.succeed(Credentials, credentials), FetchHttpClient.layer);

  return {
    authorize: (effect) => effect.pipe(Effect.provide(services)),
    stamp: Effect.map(boundStamp, (json): RuntimeStamp => {
      try {
        return JSON.parse(json || "{}");
      } catch {
        return {};
      }
    }),
    domain: domainOf,
    teamId: boundTeamId,
    deployStamp: stamp,
  } satisfies E2BAuth;
});
