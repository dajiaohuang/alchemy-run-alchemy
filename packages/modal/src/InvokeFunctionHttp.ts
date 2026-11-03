import * as functions from "@distilled.cloud/modal/functions";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { BlobOps, downloadBlob, MAX_INLINE_BYTES, uploadBlob } from "./Blob.ts";
import { decodeCbor, encodeCbor } from "./Cbor.ts";
import { makeFunctionBinding, type ResolvedFunction } from "./FunctionTarget.ts";
import { operations, seconds } from "./internal.ts";
import {
  FunctionCallFailed,
  FunctionCallTimeout,
  InvokeFunction,
  type CallOptions,
  type FunctionClient,
  type FunctionTarget,
} from "./InvokeFunction.ts";

/** The operations the client calls, resolved by `ModalBindings.withCredentials`. */
const InvokeOps = operations({
  map: functions.functionMap,
  putInputs: functions.functionPutInputs,
  getOutputs: functions.getFunctionOutputs,
  invokeAsync: functions.invokeFunctionAsync,
  cancel: functions.cancelFunctionCall,
  blobs: BlobOps,
});

type InvokeOps = Effect.Success<typeof InvokeOps>;

/** One input: CBOR `[args, kwargs]`, inline or (over 2 MiB) as a blob. */
const encodeInput = Effect.fn(function* (
  blobs: BlobOps,
  args: ReadonlyArray<unknown>,
  kwargs: Record<string, unknown>,
  method: string | undefined,
  final: boolean,
  forceBlob = false,
) {
  const bytes = yield* Effect.try({
    try: () => encodeCbor([args, kwargs]),
    catch: (cause) =>
      new FunctionCallFailed({
        status: "INVALID_ARGUMENTS",
        message: `Arguments aren't CBOR-encodable: ${String(cause)}`,
      }),
  });
  const payload =
    forceBlob || bytes.length > MAX_INLINE_BYTES
      ? { argsBlobId: yield* uploadBlob(blobs, bytes) }
      : { args: yield* Effect.sync(() => Buffer.from(bytes).toString("base64")) };
  return {
    ...payload,
    dataFormat: "DATA_FORMAT_CBOR",
    methodName: method,
    finalInput: final,
  } satisfies functions.FunctionInput;
});

/** The value of a finished output, or its failure. */
const decodeOutput = Effect.fn(function* (
  blobs: BlobOps,
  functionCallId: string,
  output: functions.GetFunctionOutputsItem,
) {
  const result = output.result;
  if (result?.status !== "GENERIC_STATUS_SUCCESS") {
    return yield* new FunctionCallFailed({
      status: result?.status ?? "GENERIC_STATUS_UNSPECIFIED",
      message: result?.exception || `Function call ${functionCallId} failed`,
      traceback: result?.traceback,
    });
  }
  const bytes = result.dataBlobId
    ? yield* downloadBlob(blobs, result.dataBlobId)
    : result.data
      ? yield* Effect.sync(() => new Uint8Array(Buffer.from(result.data!, "base64")))
      : undefined;
  if (bytes === undefined) return null;
  return yield* Effect.try({
    try: () => decodeCbor(bytes),
    catch: (cause) =>
      new FunctionCallFailed({
        status: "INVALID_RESULT",
        message: `The result isn't CBOR (return plain values, or set the function's serialization): ${String(cause)}`,
      }),
  });
});

/** Inputs per `FunctionMap`/`FunctionPutInputs` request. */
const BATCH = 49;

/** A client for a resolved function. */
export const makeFunctionClient = (resolved: ResolvedFunction, ops: InvokeOps): FunctionClient => {
  const { functionId, method } = resolved;

  /** Collect `count` outputs of a call, by input index. */
  const collect = Effect.fn(function* (
    functionCallId: string,
    count: number,
    timeout: number | string | undefined,
  ) {
    const budget = seconds(timeout) ?? 300;
    const rounds = Math.max(1, Math.ceil(budget / 55));
    const values = new Map<number, unknown>();
    let lastEntryId = "0-0";
    for (let round = 0; round < rounds && values.size < count; round++) {
      const response = yield* ops.getOutputs({
        functionCallId,
        maxValues: Math.min(1000, count - values.size),
        timeout: Math.min(55, budget),
        lastEntryId,
        clearOnSuccess: true,
      });
      if (response.lastEntryId) lastEntryId = response.lastEntryId;
      for (const output of response.outputs ?? []) {
        if (!output.result) continue;
        values.set(output.idx ?? 0, yield* decodeOutput(ops.blobs, functionCallId, output));
      }
    }
    if (values.size < count) {
      return yield* new FunctionCallTimeout({
        functionCallId,
        message: `Function call ${functionCallId} didn't finish within ${budget} seconds`,
      });
    }
    return Array.from({ length: count }, (_, idx) => values.get(idx));
  });

  const start = Effect.fn(function* (
    inputs: ReadonlyArray<readonly [ReadonlyArray<unknown>, Record<string, unknown>]>,
    type: "FUNCTION_CALL_TYPE_UNARY" | "FUNCTION_CALL_TYPE_MAP",
  ) {
    const id = yield* functionId;
    const items = yield* Effect.forEach(inputs, ([args, kwargs], idx) =>
      Effect.map(
        encodeInput(ops.blobs, args, kwargs, method, idx === inputs.length - 1),
        (input) => ({
          idx,
          input,
        }),
      ),
    );
    const response = yield* ops.map({
      functionId: id,
      functionCallType: type,
      functionCallInvocationType: "FUNCTION_CALL_INVOCATION_TYPE_SYNC",
      returnExceptions: false,
      pipelinedInputs: items.slice(0, BATCH),
    });
    const functionCallId = response.functionCallId ?? "";
    for (let offset = BATCH; offset < items.length; offset += BATCH) {
      yield* ops.putInputs({
        functionId: id,
        functionCallId,
        inputs: items.slice(offset, offset + BATCH),
      });
    }
    return functionCallId;
  });

  return {
    call: <A>(
      args: ReadonlyArray<unknown> = [],
      kwargs: Record<string, unknown> = {},
      options: CallOptions = {},
    ) =>
      start([[args, kwargs]], "FUNCTION_CALL_TYPE_UNARY").pipe(
        Effect.flatMap((id) => collect(id, 1, options.timeout)),
        // CBOR results are untyped; the caller names the shape it expects.
        Effect.map(([value]) => value as A),
      ),
    map: <A>(inputs: ReadonlyArray<ReadonlyArray<unknown>>, options: CallOptions = {}) =>
      inputs.length === 0
        ? Effect.succeed([] as A[])
        : start(
            inputs.map((args) => [args, {}] as const),
            "FUNCTION_CALL_TYPE_MAP",
          ).pipe(
            Effect.flatMap((id) => collect(id, inputs.length, options.timeout)),
            Effect.map((values) => values as A[]),
          ),
    spawn: (args = [], kwargs = {}) =>
      Effect.gen(function* () {
        const id = yield* functionId;
        const input = yield* encodeInput(ops.blobs, args, kwargs, method, true);
        let response = yield* ops.invokeAsync({ functionId: id, input });
        // Modal asks for the input as a blob when it's over the function's inline limit.
        if (response.retryWithBlobUpload) {
          const blobInput = yield* encodeInput(ops.blobs, args, kwargs, method, true, true);
          response = yield* ops.invokeAsync({ functionId: id, input: blobInput });
        }
        return { functionCallId: response.functionCallId ?? "" };
      }),
    result: <A>(call: { readonly functionCallId: string }, options: CallOptions = {}) =>
      collect(call.functionCallId, 1, options.timeout).pipe(Effect.map(([value]) => value as A)),
    cancel: (call) =>
      ops.cancel({ functionCallId: call.functionCallId }).pipe(
        Effect.catchTag("FunctionCallNotFound", () => Effect.void),
        Effect.asVoid,
      ),
  };
};

/**
 * {@link InvokeFunction} over Modal's API. Grants the host's identity (a scoped
 * `Modal.ServiceUser`) the `contributor` role on the function's environment.
 *
 * @layer
 * @product Function
 * @provides Modal.InvokeFunction
 */
export const InvokeFunctionHttp = Layer.effect(
  InvokeFunction,
  makeFunctionBinding({
    tag: "Modal.InvokeFunction",
    operations: InvokeOps,
    role: "contributor",
    make: (resolved, ops, _target: FunctionTarget) =>
      Effect.succeed(makeFunctionClient(resolved, ops)),
  }),
);
