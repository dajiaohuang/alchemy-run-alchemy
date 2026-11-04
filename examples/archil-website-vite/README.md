# Archil Website: Vite + Notes API

A Tailwind React SPA (`Archil.Website.Vite`) that reads and writes notes
through an Effect `Archil.Service`. Each note is a JSON file on an
`Archil.Disk`, written through `Archil.ReadWriteDisk`. The site and the
API run as two services in one `Archil.Sandbox`.

- `alchemy deploy` builds the SPA (inlining `VITE_API_URL`) and deploys
  both services.
- `alchemy dev` is Vite's own server (HMR included).

```sh
export ARCHIL_API_KEY=...
bun run deploy
```

The SPA is at `https://3000-<id>.<region>.archil.app`; the API is the
`3001-` hostname (`GET`/`POST /notes`). An idle sandbox stops and starts
again on the next request.

```sh
bun run destroy
```
