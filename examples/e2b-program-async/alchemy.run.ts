// alchemy.run.ts
import * as E2B from "@alchemy.run/e2b";
import * as Alchemy from "alchemy";
import * as Output from "alchemy/Output";
import * as Effect from "effect/Effect";
import { Greeter } from "./src/Greeter.ts";

export default Alchemy.Stack(
  "E2BProgramAsyncExample",
  { providers: E2B.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const greeter = yield* Greeter;
    const sandbox = yield* E2B.Sandbox("GreeterSandbox", {
      template: greeter.templateId,
      build: greeter.buildId,
      network: { allowPublicTraffic: true },
    });
    return {
      url: Output.interpolate`https://3000-${sandbox.sandboxId}.${sandbox.domain}`,
    };
  }),
);
