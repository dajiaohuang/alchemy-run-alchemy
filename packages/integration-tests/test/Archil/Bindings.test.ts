import * as Archil from "@alchemy.run/archil";
import * as Cloudflare from "@alchemy.run/cloudflare";
import { describe, expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as Alchemy from "alchemy";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import MountContainerLive from "./fixtures/mount-container.ts";
import { Preview, SeedBase } from "./fixtures/resources.ts";
import ArchilWorker from "./fixtures/worker.ts";

const providers = Layer.mergeAll(Archil.providers(), Cloudflare.providers());
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({ providers });

const Stack = Alchemy.Stack(
  "ArchilBindingsStack",
  { providers, state: Cloudflare.state() },
  Effect.gen(function* () {
    yield* SeedBase;
    const preview = yield* Preview;
    const worker = yield* ArchilWorker;
    return { url: worker.url.as<string>(), preview: preview.sandboxId };
  }).pipe(Effect.provide(MountContainerLive)),
);

const stack = beforeAll(deploy(Stack));
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

const call = (path: string, init?: { method?: "PUT"; body?: string }) =>
  Effect.gen(function* () {
    const { url } = yield* stack;
    const client = HttpClient.filterStatusOk(yield* HttpClient.HttpClient);
    const request =
      init?.method === "PUT"
        ? HttpClientRequest.put(`${url}${path}`).pipe(HttpClientRequest.bodyText(init.body ?? ""))
        : HttpClientRequest.get(`${url}${path}`);
    return yield* client
      .execute(request)
      .pipe(Effect.retry({ schedule: Schedule.exponential("500 millis"), times: 8 }));
  });

const json = <A>(path: string) =>
  call(path).pipe(Effect.flatMap((r) => r.json as Effect.Effect<A, any>));
const text = (path: string) => call(path).pipe(Effect.flatMap((r) => r.text));

class MountRouteFailed extends Data.TaggedError("MountRouteFailed")<{ readonly text: string }> {}

const tags = ["provider:archil", "provider:cloudflare", "provider:cloudflare:worker", "live"];

describe("WriteDisk + ReadDisk", () => {
  test(
    "a file written as an object is read back, listed, and shared",
    Effect.gen(function* () {
      yield* call("/put?key=hello.txt", { method: "PUT", body: "hello archil" });
      expect(yield* text("/get?key=hello.txt")).toBe("hello archil");
      expect(yield* json<string[]>("/list")).toContain("hello.txt");
      const { url } = yield* json<{ url: string }>("/share?key=hello.txt");
      const client = yield* HttpClient.HttpClient;
      const shared = yield* client.get(url).pipe(Effect.flatMap((r) => r.text));
      expect(shared).toBe("hello archil");
    }),
    { tags, timeout: 180_000 },
  );
});

describe("ReadWriteDisk", () => {
  test(
    "append, read, and delete in one client",
    Effect.gen(function* () {
      const body = yield* json<{ text: string; after: unknown }>("/roundtrip?key=log.txt");
      expect(body.text).toBe("ab");
      expect(body.after).toBeNull();
    }),
    { tags, timeout: 180_000 },
  );
});

describe("ExecDisk", () => {
  test(
    "exec sees the files on the disk, and grep finds them",
    Effect.gen(function* () {
      yield* call("/put?key=grep/needle.txt", { method: "PUT", body: "find the NEEDLE here" });
      const result = yield* json<{ exitCode: number; stdout: string }>(
        `/exec?command=${encodeURIComponent("cat /mnt/archil/grep/needle.txt")}`,
      );
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain("NEEDLE");
      const grep = yield* json<{ matches: Array<{ file: string }> }>("/grep?pattern=NEEDLE");
      expect(grep.matches.map((m) => m.file).join(",")).toContain("needle.txt");
    }),
    { tags, timeout: 180_000 },
  );
});

describe("ExecDisks + CreateDisk", () => {
  test(
    "one exec copies between two disks created at run time",
    Effect.gen(function* () {
      const result = yield* json<{ exitCode: number; stdout: string }>("/exec-many?user=exec-user");
      expect(result.exitCode).toBe(0);
      expect(result.stdout.trim()).toBe("copied");
    }),
    { tags, timeout: 180_000 },
  );
});

describe("CreateDisk", () => {
  test(
    "disks are created by name, stamped with the binding's prefix, and deleted",
    Effect.gen(function* () {
      const body = yield* json<{ stdout: string; listed: string[]; name: string; gone: boolean }>(
        "/disk?name=user-1&delete=1",
      );
      expect(body.stdout.trim()).toBe("user-1");
      expect(body.name.endsWith("-user-1")).toBe(true);
      expect(body.listed).toContain(body.name);
      expect(body.gone).toBe(true);
    }),
    { tags, timeout: 180_000 },
  );
});

describe("BranchDisk", () => {
  test(
    "a branch forks from the seeded checkpoint",
    Effect.gen(function* () {
      const body = yield* json<{ branch: Archil.BranchAttributes; all: string[] }>(
        "/branch?name=session-1",
      );
      expect(body.branch.fromCheckpoint).toBe("base");
      expect(body.all).toContain("session-1");
    }),
    { tags, timeout: 180_000 },
  );
});

describe("CreateImage", () => {
  test(
    "an image builds at run time",
    Effect.gen(function* () {
      const image = yield* json<Archil.ImageAttributes>("/image");
      expect(image.imageId).toMatch(/^[0-9a-f]{64}$/);
    }),
    { tags, timeout: 180_000 },
  );
});

describe("CreateSandbox", () => {
  test(
    "a sandbox runs a command, forks with its files, and is deleted",
    Effect.gen(function* () {
      const body = yield* json<{ stdout: string; fork: string }>("/sandbox?name=task-1");
      expect(body.stdout).toBe("hi from worker");
      expect(body.fork).toBe("hi from worker");
    }),
    { tags, timeout: 300_000 },
  );
});

describe("ConnectSandbox", () => {
  test(
    "the deploy-time sandbox runs commands and exposes ports",
    Effect.gen(function* () {
      const body = yield* json<{ stdout: string; hostname: string }>("/preview");
      expect(body.stdout).toBe("preview-ok");
      expect(body.hostname.length).toBeGreaterThan(0);
    }),
    { tags, timeout: 300_000 },
  );
});

describe("MountDisk", () => {
  test(
    "a Container mounts the disk with its own disk user and reads a file",
    Effect.gen(function* () {
      const { url } = yield* stack;
      yield* call("/put?key=mounted.txt", { method: "PUT", body: "through fuse" });
      const client = yield* HttpClient.HttpClient;
      // The container cold-starts; keep its error text so a failed mount
      // shows the Archil client's stderr instead of a bare 500.
      const body = yield* client.get(`${url}/mount?key=mounted.txt`).pipe(
        Effect.flatMap((res) =>
          Effect.flatMap(res.text, (text) =>
            res.status === 200 ? Effect.succeed(text) : Effect.fail(new MountRouteFailed({ text })),
          ),
        ),
        Effect.retry({ schedule: Schedule.spaced("5 seconds"), times: 18 }),
        Effect.catchTag("MountRouteFailed", (e) => Effect.succeed(e.text)),
      );
      expect(body).toBe("through fuse");
    }),
    { tags: [...tags, "provider:cloudflare:container"], timeout: 300_000 },
  );
});
