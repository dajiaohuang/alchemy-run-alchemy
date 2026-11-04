import * as queue from "@distilled.cloud/modal/queue";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { makeOpenModalBinding } from "./BindingHttp.ts";
import { CreateQueue, type QueueHandle, type QueuesClient } from "./CreateQueue.ts";
import type { Environment } from "./Environment.ts";
import { environmentOf, listAll, operations } from "./internal.ts";
import { ensureQueue, lookupQueue, type QueueAttributes } from "./Queue.ts";
import { QueueOps, makeReadWriteQueueClient } from "./QueueClient.ts";

/** The operations the client calls, resolved by `ModalBindings.withCredentials`. */
const QueuesOps = operations({
  getOrCreate: queue.queueGetOrCreate,
  delete: queue.deleteQueue,
  list: queue.listQueue,
  client: QueueOps,
});

export type QueuesOps = Effect.Success<typeof QueuesOps>;

const handleOf = (attributes: QueueAttributes, ops: QueuesOps): QueueHandle => ({
  ...makeReadWriteQueueClient(Effect.succeed(attributes.queueId), ops.client),
  queueId: Effect.succeed(attributes.queueId),
  name: attributes.name,
  attributes,
});

/** A queues client over an environment name resolved per call. */
export const makeQueuesClient = (
  environment: Effect.Effect<string | undefined>,
  ops: QueuesOps,
): QueuesClient => ({
  getOrCreate: (name) =>
    Effect.flatMap(environment, (env) => ensureQueue(ops.getOrCreate, name, env)).pipe(
      Effect.map((attributes) => handleOf(attributes, ops)),
    ),
  get: (name) =>
    Effect.flatMap(environment, (env) => lookupQueue(ops.getOrCreate, name, env)).pipe(
      Effect.map((attributes) => (attributes ? handleOf(attributes, ops) : undefined)),
    ),
  delete: (name) =>
    Effect.gen(function* () {
      const env = yield* environment;
      const found = yield* lookupQueue(ops.getOrCreate, name, env);
      if (!found) return;
      yield* ops
        .delete({ queueId: found.queueId })
        .pipe(Effect.catchTag("QueueNotFound", () => Effect.void));
    }),
  list: () =>
    Effect.gen(function* () {
      const env = yield* environment;
      const items = yield* listAll(
        (pagination) =>
          ops
            .list({ environmentName: environmentOf(env), pagination })
            .pipe(Effect.map((response) => response.queues ?? [])),
        (item) => item.createdAt,
      );
      return items.flatMap((item) => (item.name ? [item.name] : []));
    }),
});

/**
 * {@link CreateQueue} over Modal's API. Grants the host's identity (a scoped
 * `Modal.ServiceUser`) the `contributor` role on the environment.
 *
 * @layer
 * @product Queue
 * @provides Modal.CreateQueue
 */
export const CreateQueueHttp = Layer.effect(
  CreateQueue,
  makeOpenModalBinding({
    tag: "Modal.CreateQueue",
    operations: QueuesOps,
    role: "contributor",
    environmentOf: (environment?: Environment) => environment?.name,
    make: Effect.fn(function* (ops, environment?: Environment) {
      const name = environment ? yield* environment.name : Effect.succeed(undefined);
      return makeQueuesClient(name, ops);
    }),
  }),
);
