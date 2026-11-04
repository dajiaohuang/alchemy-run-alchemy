import * as Archil from "@alchemy.run/archil";

export const API_PORT = 3001;

/** One sandbox runs both the notes API and the website. */
export const Box = Archil.Sandbox("Box", {});

/** Every note is a JSON file on this disk. */
export const NotesDisk = Archil.Disk("Notes", {});
