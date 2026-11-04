import * as AWS from "@alchemy.run/aws";
import * as Modal from "@alchemy.run/modal";
import { describe, expect } from "@alchemy.run/test";
import * as Test from "@alchemy.run/test/Alchemy";
import * as secret from "@distilled.cloud/modal/secret";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import ModalBindingsFunction from "./fixtures/bindings-function.ts";
import { Token } from "./fixtures/resources.ts";
import { hostIdentity } from "./hostIdentity.ts";

const providers = Layer.mergeAll(Modal.providers(), AWS.providers(), hostIdentity);
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({ providers });

const Stack = Alchemy.Stack(
  "ModalBindingsStack",
  { providers, state: AWS.state() },
  Effect.gen(function* () {
    const token = yield* Token;
    const host = yield* ModalBindingsFunction;
    return { url: host.functionUrl.as<string>(), secretId: token.secretId };
  }),
);

const stack = beforeAll(deploy(Stack));
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

const get = <A>(path: string) =>
  Effect.gen(function* () {
    const { url } = yield* stack;
    const client = HttpClient.filterStatusOk(yield* HttpClient.HttpClient);
    const res = yield* client
      .get(`${url}${path}`)
      .pipe(Effect.retry({ schedule: Schedule.exponential("500 millis"), times: 8 }));
    return (yield* res.json) as A;
  });

const tags = (kind: string) => ["provider:modal", `provider:modal:${kind}`, "provider:aws", "live"];

describe("ReadVolume / WriteVolume / ReadWriteVolume", () => {
  test(
    "write, read, copy, list and remove volume files from a Lambda function",
    Effect.gen(function* () {
      const body = yield* get<{ text: string; files: string[] }>("/volume?key=v1");
      expect(body.text).toBe("hello v1");
      expect(body.files).toContain("v1.txt");
      expect(body.files).toContain("v1-copy.txt");
    }),
    { tags: tags("volume"), timeout: 180_000 },
  );
});

describe("ReadDict / WriteDict / ReadWriteDict", () => {
  test(
    "put, get, contains and pop from a Lambda function",
    Effect.gen(function* () {
      const body = yield* get<Record<string, unknown>>("/dict?key=d1");
      expect(body).toEqual({ value: { n: 1 }, contains: true, popped: { n: 1 }, after: false });
    }),
    { tags: tags("dict"), timeout: 180_000 },
  );
});

describe("ReadQueue / WriteQueue / ReadWriteQueue", () => {
  test(
    "put into a partition and take from it",
    Effect.gen(function* () {
      const body = yield* get<{ len: number; taken: unknown[] }>("/queue?key=q1");
      expect(body.len).toBe(2);
      expect(body.taken).toEqual([{ i: 1 }, { i: 2 }]);
    }),
    { tags: tags("queue"), timeout: 180_000 },
  );
});

describe("WriteSecret", () => {
  test.provider(
    "rotate a secret from a Lambda function",
    () =>
      Effect.gen(function* () {
        yield* get("/secret?key=s1");
        const { secretId } = yield* stack;
        const { metadata } = yield* secret.secretGetInfo({ secretId });
        expect(metadata?.keys).toEqual(["TOKEN"]);
      }),
    { tags: tags("secret"), timeout: 180_000 },
  );
});

describe("CreateSandbox", () => {
  test(
    "create a sandbox from a bound image and exec in it",
    Effect.gen(function* () {
      const body = yield* get<{ exitCode: number; stdout: string }>("/sandbox?key=x1");
      expect(body.exitCode).toBe(0);
      expect(body.stdout.trim()).toBe("ready");
    }),
    { tags: tags("sandbox"), timeout: 300_000 },
  );

  test(
    "write and read sandbox files and snapshot the filesystem",
    Effect.gen(function* () {
      const body = yield* get<{ read: string; snapshot: string }>("/sandbox-files");
      expect(body.read).toBe("written");
      expect(body.snapshot).toMatch(/^im-/);
    }),
    { tags: tags("sandbox"), timeout: 300_000 },
  );

  test(
    "find a sandbox by name",
    Effect.gen(function* () {
      const body = yield* get<{ created: string; found: string }>("/sandbox-named?key=n1");
      expect(body.found).toBe(body.created);
    }),
    { tags: tags("sandbox"), timeout: 300_000 },
  );
});

describe("CreateSandbox persistent", () => {
  // Persistent sandboxes are in beta; set MODAL_TEST_PERSISTENT_SANDBOX=1 on
  // an entitled workspace (PersistentSandbox.test.ts probes the rejection).
  test.skipIf(!process.env.MODAL_TEST_PERSISTENT_SANDBOX)(
    "create a persistent sandbox and reach its session by id",
    Effect.gen(function* () {
      const body = yield* get<{ read: string }>("/sandbox-persistent?key=p1");
      expect(body.read).toBe("kept p1");
    }),
    { tags: tags("sandbox"), timeout: 300_000 },
  );
});

describe("CreateSandbox usage", () => {
  test(
    "report sandbox cost grouped by tag",
    Effect.gen(function* () {
      yield* get("/sandbox?key=u1");
      const body = yield* get<{ groups: number }>("/sandbox-usage");
      expect(body.groups).toBeGreaterThanOrEqual(0);
    }),
    { tags: tags("sandbox"), timeout: 300_000 },
  );
});

describe("CreateImage", () => {
  test(
    "build an image with a context file at runtime",
    Effect.gen(function* () {
      const body = yield* get<{ imageId: string; published: string; unpublished: string | null }>(
        "/image?key=i1",
      );
      expect(body.imageId).toMatch(/^im-/);
      expect(body.published).toBe(body.imageId);
      expect(body.unpublished).toBeNull();
    }),
    { tags: tags("image"), timeout: 600_000 },
  );
});

describe("CreateVolume / CreateDict / CreateQueue / CreateSecret", () => {
  test(
    "get-or-create, list and delete objects by name at runtime",
    Effect.gen(function* () {
      const body = yield* get<{ listed: Record<string, boolean>; keys: string[]; gone: boolean }>(
        "/open?key=o1",
      );
      expect(body.listed).toEqual({ volumes: true, dicts: true, queues: true, secrets: true });
      expect(body.keys).toEqual(["A"]);
      expect(body.gone).toBe(true);
    }),
    { tags: tags("open"), timeout: 180_000 },
  );
});
