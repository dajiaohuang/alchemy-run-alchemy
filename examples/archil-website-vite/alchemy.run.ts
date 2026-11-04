import * as Archil from "@alchemy.run/archil";
import * as Alchemy from "alchemy";
import * as Output from "alchemy/Output";
import * as Effect from "effect/Effect";
import Api from "./src/api.ts";
import { Box, NotesDisk } from "./src/shared.ts";

export default Alchemy.Stack(
  "ArchilWebsiteViteExample",
  {
    providers: Archil.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const sandbox = yield* Box;
    const disk = yield* NotesDisk;
    const api = yield* Api;
    const web = yield* Archil.Website.Vite("Web", {
      sandbox,
      env: {
        VITE_API_URL: Output.map(api.url, (url) => url ?? ""),
      },
      memo: {
        include: ["index.html", "src/**", "package.json", "vite.config.ts"],
      },
    });

    return {
      url: web.url,
      apiUrl: api.url,
      sandboxId: sandbox.sandboxId,
      diskId: disk.diskId,
    };
  }),
);
