import type * as Effect from "effect/Effect";

/**
 * Typed `env` for an async (non-Effect) {@link Service}. A service's
 * environment is process environment variables, so every entry is a
 * `string`: the object the handler receives as its second argument.
 *
 * @example
 * ```typescript
 * export const Hello = Archil.Service("Hello", {
 *   sandbox: Box,
 *   main: "./src/hello.ts",
 *   env: { GREETING: "hello" },
 * });
 * export type HelloEnv = Archil.InferEnv<typeof Hello>;
 * ```
 */
export type InferEnv<F> =
  F extends Effect.Effect<infer A, infer _E, infer _R>
    ? InferEnv<A>
    : F extends { readonly Props: { readonly env?: infer E } }
      ? InferEnvRecord<Exclude<E, undefined>>
      : InferEnvRecord<F>;

type InferEnvRecord<E> = {
  readonly [K in keyof E]: string;
};
