import * as functions from "@distilled.cloud/modal/functions";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { makeFunctionBinding } from "./FunctionTarget.ts";
import { operations, timestampOf } from "./internal.ts";
import type { FunctionTarget } from "./InvokeFunction.ts";
import { ReadFunction, type FunctionReader } from "./ReadFunction.ts";
import type { Server } from "./Server.ts";

/** The operations the client calls, resolved by `ModalBindings.withCredentials`. */
const ReadFunctionOps = operations({
  currentStats: functions.functionGetCurrentStats,
  timeRangeStats: functions.functionGetTimeRangeStats,
  metrics: functions.functionGetMetrics,
  listCalls: functions.listFunctionCall,
  callCount: functions.functionCallCount,
  callInfo: functions.functionCallGetInfo,
  schedulingParams: functions.functionGetSchedulingParams,
});

type ReadFunctionOps = Effect.Success<typeof ReadFunctionOps>;

/**
 * {@link ReadFunction} over Modal's API. Grants the host's identity (a scoped
 * `Modal.ServiceUser`) the `viewer` role on the function's environment.
 *
 * @layer
 * @product Function
 * @provides Modal.ReadFunction
 */
export const ReadFunctionHttp = Layer.effect(
  ReadFunction,
  makeFunctionBinding({
    tag: "Modal.ReadFunction",
    operations: ReadFunctionOps,
    role: "viewer",
    make: ({ functionId }, ops, _target: FunctionTarget | Server) =>
      Effect.succeed({
        stats: () =>
          Effect.flatMap(functionId, (id) => ops.currentStats({ functionId: id })).pipe(
            Effect.map((stats) => ({
              backlog: stats.backlog ?? 0,
              runningInputs: stats.numRunningInputs ?? 0,
              containers: stats.numTotalTasks ?? 0,
              inputHeadroom: stats.inputHeadroom ?? 0,
            })),
          ),
        timeRangeStats: (range) =>
          Effect.flatMap(functionId, (id) =>
            ops.timeRangeStats({
              functionId: id,
              since: timestampOf(range.since),
              until: timestampOf(range.until ?? Date.now()),
              containerId: range.containerId,
            }),
          ),
        metrics: (range) =>
          Effect.flatMap(functionId, (id) =>
            ops.metrics({
              functionId: id,
              since: timestampOf(range.since),
              until: timestampOf(range.until ?? Date.now()),
              bucketSecs: range.bucketSecs,
              groups: range.groups ? [...range.groups] : undefined,
            }),
          ),
        calls: () =>
          Effect.flatMap(functionId, (id) => ops.listCalls({ functionId: id })).pipe(
            Effect.map((response) => response.functionCalls ?? []),
          ),
        callCount: (range) =>
          Effect.flatMap(functionId, (id) =>
            ops.callCount({
              functionId: id,
              since: timestampOf(range.since),
              until: timestampOf(range.until ?? Date.now()),
              bucketSecs: range.bucketSecs,
            }),
          ),
        callInfo: (call) =>
          Effect.flatMap(functionId, (id) =>
            ops.callInfo({
              functionId: id,
              functionCallId: call.functionCallId,
            }),
          ),
        autoscaler: () =>
          Effect.flatMap(functionId, (id) => ops.schedulingParams({ functionId: id })).pipe(
            Effect.map((response) => response.autoscalerConfiguration?.settings),
          ),
      } satisfies FunctionReader),
  }),
);
