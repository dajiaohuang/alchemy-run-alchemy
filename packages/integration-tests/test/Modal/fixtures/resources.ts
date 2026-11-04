import * as Modal from "@alchemy.run/modal";
import * as Redacted from "effect/Redacted";

export const Agents = Modal.Environment("BindingsEnv", {});

// Declarations as props: each is yielded with the resource that uses it.
export const Coder = Modal.App("BindingsApp", { environment: Agents });

export const Base = Modal.Image("BindingsBase", {
  app: Coder,
  image: "debian:bookworm-slim",
  commands: ["RUN echo ready > /ready.txt"],
});

export const Workspace = Modal.Volume("BindingsVolume", { environment: Agents });

export const Results = Modal.Dict("BindingsDict", { environment: Agents });

export const Jobs = Modal.Queue("BindingsQueue", { environment: Agents });

export const Token = Modal.Secret("BindingsSecret", {
  environment: Agents,
  env: { TOKEN: Redacted.make("dummy") },
});
