import * as Archil from "@alchemy.run/archil";

/** The disk the API stores files on. */
export const Files = Archil.Disk("Files", {});

/** The sandbox both services run in. */
export const Box = Archil.Sandbox("Box", {});
