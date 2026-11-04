import * as Daytona from "@alchemy.run/daytona";

/** The sandbox the Service runs in: public, so its preview URL is open. */
export const ServiceBox = Daytona.Sandbox("ServiceBox", {
  public: true,
  autoStopInterval: 0,
});

export const SERVICE_PORT = 3000;
