# Archil Service

Two services in one `Archil.Sandbox`:

- `src/Api.ts`: an Effect `Archil.Service` that stores files on an
  `Archil.Disk` through `Archil.ReadWriteDisk` (`PUT /:key`, `GET /:key`).
- `src/hello.ts`: an async service, a plain `fetch(request, env)` handler
  typed with `Archil.InferEnv`.

```sh
alchemy profile edit --profile default --add Archil
bun run deploy
curl -X PUT --data "hi" "$URL/hello.txt"
curl "$URL/hello.txt"
bun run destroy
```
