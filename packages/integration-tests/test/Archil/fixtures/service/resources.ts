import * as Archil from "@alchemy.run/archil";

/** The sandbox both test services run in. */
export const ServiceBox = Archil.Sandbox("ServiceBox", { idleTtlSeconds: 300 });

/** A disk the Effect service reads through `ReadDisk`. */
export const ServiceData = Archil.Disk("ServiceData", {});

/** The async service, declared with a greeting so a redeploy can change it. */
export const asyncHello = (greeting: string) =>
  Archil.Service("AsyncHello", {
    sandbox: ServiceBox,
    main: `${import.meta.dirname}/async.ts`,
    port: 3001,
    env: { GREETING: greeting },
  });

export type HelloEnv = Archil.InferEnv<ReturnType<typeof asyncHello>>;
