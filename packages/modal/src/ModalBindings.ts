import {
  Credentials,
  CredentialsFromEnv,
  credentials,
  withTaskCommandRouter,
} from "@distilled.cloud/modal/Credentials";
import { orDieCredentialsUnavailable } from "alchemy/Auth/Resolve";
import * as Binding from "alchemy/Binding";
import type { Input } from "alchemy/Input";
import * as Output from "alchemy/Output";
import { CurrentRuntimeContext } from "alchemy/RuntimeContext";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import { withAuthTokens } from "./Credentials.ts";
import { ServiceUser } from "./ServiceUser.ts";

/**
 * How a Modal binding reaches Modal from its host.
 *
 * Modal grants access per environment, as a `viewer` or a `contributor`. So:
 *
 * - any host (a Cloudflare Worker, a Lambda, a Modal Function, Server or
 *   Sandbox): {@link Bind} creates a scoped `Modal.ServiceUser` for the
 *   host (`<host>ModalIdentity`), grants it the role each binding asks for
 *   on the bound resource's environment, and binds its token pair into the
 *   host's env (`MODAL_TOKEN_ID`, and `MODAL_TOKEN_SECRET` kept `Redacted`);
 * - no host (a script or a deploy-time Action): nothing is bound, and calls
 *   use the deployer's credentials.
 *
 * Import it as a namespace. A binding resolves its operations with
 * {@link withCredentials} and gets its `bind` from {@link Bind} when its layer
 * is built, then binds behind its own `__ALCHEMY_RUNTIME__` guard:
 *
 * ```ts
 * Effect.gen(function* () {
 *   const put = yield* ModalBindings.withCredentials(modalQueue.putQueue);
 *   const bind = yield* ModalBindings.Bind;
 *   return Effect.fn(function* (queue: Queue) {
 *     if (!globalThis.__ALCHEMY_RUNTIME__) {
 *       yield* bind`Modal.WriteQueue(${queue})`({
 *         roles: [{ environment: queue.environment, role: "contributor" }],
 *       });
 *     }
 *     const queueId = yield* queue.queueId;
 *     return (value) => put({ queueId: yield* queueId, values: [...] });
 *   });
 * })
 * ```
 */
export const TOKEN_ENV = {
  id: "MODAL_TOKEN_ID",
  secret: "MODAL_TOKEN_SECRET",
  serverUrl: "MODAL_SERVER_URL",
} as const;

/** A role a binding needs in one environment. */
export interface EnvironmentGrant {
  /** The environment's name; `undefined` is the workspace default. */
  readonly environment: Input<string | undefined>;
  readonly role: "viewer" | "contributor";
}

/** What a Modal binding attaches to its host. */
export interface HostBinding {
  /** Environment roles the host's identity needs. */
  readonly roles?: ReadonlyArray<EnvironmentGrant>;
  /** Env vars the binding reads at runtime. */
  readonly env?: Record<string, unknown>;
}

/** The signature {@link Bind} yields. */
export type BindHost = (
  label: TemplateStringsArray,
  ...args: unknown[]
) => (data?: HostBinding) => Effect.Effect<void>;

type BindTarget = {
  bind: (...args: unknown[]) => (data: unknown) => Effect.Effect<void>;
};

const labelled = (verb: string, label: TemplateStringsArray): TemplateStringsArray => {
  const strings = [`${verb}(`, `, ${label[0]}`, ...label.slice(1)];
  strings[strings.length - 1] += ")";
  return Object.assign(strings, { raw: strings }) as unknown as TemplateStringsArray;
};

/**
 * The `bind` for a binding's layer: yield it while the layer is built, then
 * call it from the returned function behind a literal
 * `if (!globalThis.__ALCHEMY_RUNTIME__)` so the bundler drops it. On a host it
 * grants `data.roles` to the host's service user and binds that user's token
 * pair (and `data.env`) into the host's env; with no host it does nothing.
 */
export const Bind: Effect.Effect<BindHost> = Effect.gen(function* () {
  // Bindings only bind at deploy, so a host bundle captures nothing here
  // and drops the identity resource.
  if (globalThis.__ALCHEMY_RUNTIME__) return () => () => Effect.void;
  const Identity = yield* ServiceUser;
  const ambient = yield* Effect.serviceOption(Credentials);
  return (label: TemplateStringsArray, ...args: unknown[]) =>
    Effect.fn(function* (data: HostBinding = {}) {
      const host = yield* Binding.Host;
      if (host === undefined) return;
      const runtime = yield* CurrentRuntimeContext;
      if (runtime === undefined) return;

      // A binding that calls no Modal API (only `env`) needs no identity.
      if (data.roles !== undefined && data.roles.length > 0) {
        const user = yield* Identity(`${host.LogicalId}ModalIdentity`, { scoped: true });
        yield* (user as unknown as BindTarget).bind(
          labelled("Allow", label),
          host,
          ...args,
        )({ environments: data.roles });
        // The host talks to the deployer's Modal control plane.
        const resolve = Option.isSome(ambient)
          ? ambient.value
          : yield* Credentials.pipe(Effect.provide(CredentialsFromEnv));
        // Binding initialization can't fail; the engine reports this defect as
        // `CredentialsRequired`.
        const creds = yield* resolve.pipe(orDieCredentialsUnavailable("Modal"));
        yield* runtime.set(TOKEN_ENV.id, user.tokenId);
        yield* runtime.set(TOKEN_ENV.secret, user.tokenSecret);
        yield* runtime.set(TOKEN_ENV.serverUrl, Output.literal(creds.apiBaseUrl));
      }
      for (const [name, value] of Object.entries(data.env ?? {})) {
        yield* runtime.set(name, Output.isOutput(value) ? value : Output.literal(value));
      }
    });
});

/** The credentials and HTTP client distilled operations run with on this host. */
const operationContext = Effect.gen(function* () {
  const runtime = yield* CurrentRuntimeContext;
  if (globalThis.__ALCHEMY_RUNTIME__ && runtime !== undefined) {
    const tokenId = yield* runtime.get<string>(TOKEN_ENV.id);
    const tokenSecret = yield* runtime.get<string | Redacted.Redacted<string>>(TOKEN_ENV.secret);
    if (tokenId !== undefined && tokenSecret !== undefined) {
      const serverUrl = yield* runtime.get<string>(TOKEN_ENV.serverUrl);
      return yield* Layer.build(
        Layer.mergeAll(
          withAuthTokens(
            credentials({
              tokenId: Redacted.make(tokenId),
              tokenSecret: Redacted.isRedacted(tokenSecret)
                ? tokenSecret
                : Redacted.make(tokenSecret),
              apiBaseUrl: serverUrl,
            }),
          ),
          FetchHttpClient.layer,
        ),
      ).pipe(Effect.scoped);
    }
  }
  // No bound identity (deploy, scripts, Actions): the ambient Modal
  // credentials, falling back to `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET`.
  const ambientCredentials = yield* Effect.serviceOption(Credentials);
  const ambientHttp = yield* Effect.serviceOption(HttpClient.HttpClient);
  return Context.make(
    Credentials,
    Option.isSome(ambientCredentials)
      ? ambientCredentials.value
      : yield* Credentials.pipe(Effect.provide(withAuthTokens(CredentialsFromEnv))),
  ).pipe(
    Context.add(
      HttpClient.HttpClient,
      Option.isSome(ambientHttp)
        ? ambientHttp.value
        : yield* HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)),
    ),
  );
});

/**
 * Resolve a distilled operation (or several, with `Effect.all`) once when
 * the binding's layer is built: with the token pair {@link Bind} bound into
 * the host, or the deployer's credentials when there is none.
 *
 * @example
 * ```ts
 * const ops = yield* ModalBindings.withCredentials(
 *   operations({ getFile: volume.volumeGetFile2, putFiles: volume.volumePutFiles2 }),
 * );
 * ```
 */
export const withCredentials: <Op>(operation: Effect.Effect<Op, never, any>) => Effect.Effect<Op> =
  Effect.fn(function* <Op>(operation: Effect.Effect<Op, never, any>) {
    const context = yield* operationContext;
    return yield* (operation as Effect.Effect<Op>).pipe(Effect.provide(context));
  });

/**
 * Resolve a task command router operation for one sandbox: the router runs
 * per task, at a URL with its own JWT (from `sandboxGetCommandRouterAccess`).
 * The router credentials are applied on every call, because a resolved
 * operation runs with its caller's `Credentials` (the control plane's) over
 * the ones it captured. `withTaskCommandRouter` also supplies the HTTP/2
 * client the router requires.
 */
export const withRouter =
  <I, O, E>(operation: (input: I) => Effect.Effect<O, E, any>) =>
  (access: {
    readonly url: string;
    readonly jwt: string;
  }): Effect.Effect<(input: I) => Effect.Effect<O, E>> =>
    Effect.succeed((input: I) =>
      (operation(input) as Effect.Effect<O, E>).pipe(withTaskCommandRouter(access)),
    );
