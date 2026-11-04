/**
 * An HTTP/1.1 `fetch` over a `cloudflare:sockets` TLS socket, for Modal
 * calls from a Cloudflare Worker.
 *
 * `api.modal.com` is behind Cloudflare, and a Worker's `fetch` to a site on
 * Cloudflare takes Cloudflare's internal Worker-to-site path, which resets
 * Modal's gRPC upstream (`grpc-status: 14`, "upstream connect error ...
 * remote reset"). A plain TLS socket from the same Worker reaches Modal like
 * any other client (alchemy-run/distilled#1055).
 *
 * Each request opens its own socket with `connection: close`: workerd pins a
 * socket to the request that opened it, so sockets are never pooled across
 * events. The response is read in full, chunked bodies are decoded, and
 * chunked trailers (where gRPC puts `grpc-status`) are folded into the
 * response headers, as `http2Fetch` does for HTTP/2.
 */

import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as Layer from "effect/Layer";

/** The part of `cloudflare:sockets` this transport uses. */
type Connect = (
  address: { hostname: string; port: number },
  options: { secureTransport: "on"; allowHalfOpen: boolean },
) => {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  close(): Promise<void>;
};

// A non-literal specifier: hosts other than workerd (Lambda, Node, Bun)
// bundle this module but never load `cloudflare:sockets`, and workerd
// resolves its built-in modules at runtime.
const SOCKETS_MODULE = "cloudflare:sockets";
let sockets: Promise<Connect> | undefined;
const connectFn = () =>
  (sockets ??= (import(SOCKETS_MODULE) as Promise<{ connect: Connect }>).then(
    (module) => module.connect,
  ));

/** Whether this code runs on workerd (a Cloudflare Worker or Durable Object). */
export const isWorkerd = (): boolean =>
  typeof navigator !== "undefined" && navigator.userAgent === "Cloudflare-Workers";

/** The hosts that serve Modal's API (`api.modal.com` and its regional hosts). */
const MODAL_API_HOST = /(^|\.)modal\.com$/;

/** Headers this transport sets itself. */
const FRAMING_HEADERS = new Set([
  "host",
  "connection",
  "keep-alive",
  "content-length",
  "transfer-encoding",
  "upgrade",
]);

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const bodyBytes = async (body: unknown): Promise<Uint8Array | undefined> => {
  if (body === undefined || body === null) return undefined;
  if (body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  if (typeof body === "string") return encoder.encode(body);
  if (body instanceof ReadableStream) {
    return new Uint8Array(await new Response(body).arrayBuffer());
  }
  throw new TypeError("socketFetch sends only byte, ArrayBuffer, string and stream bodies");
};

const concat = (chunks: ReadonlyArray<Uint8Array>, length: number): Uint8Array => {
  const out = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
};

const indexOfCrlfCrlf = (bytes: Uint8Array, from = 0): number => {
  for (let i = from; i + 3 < bytes.length; i++) {
    if (bytes[i] === 13 && bytes[i + 1] === 10 && bytes[i + 2] === 13 && bytes[i + 3] === 10) {
      return i;
    }
  }
  return -1;
};

const indexOfCrlf = (bytes: Uint8Array, from: number): number => {
  for (let i = from; i + 1 < bytes.length; i++) {
    if (bytes[i] === 13 && bytes[i + 1] === 10) return i;
  }
  return -1;
};

const parseHeaderLines = (lines: ReadonlyArray<string>, into: Headers) => {
  for (const line of lines) {
    if (line === "") continue;
    const colon = line.indexOf(":");
    if (colon <= 0) continue;
    into.append(line.slice(0, colon).trim(), line.slice(colon + 1).trim());
  }
};

/**
 * Decode a chunked body, or `undefined` while its last chunk or trailer
 * section hasn't arrived yet.
 */
const decodeChunked = (bytes: Uint8Array): { body: Uint8Array; trailers: string[] } | undefined => {
  const chunks: Uint8Array[] = [];
  let length = 0;
  let offset = 0;
  for (;;) {
    const lineEnd = indexOfCrlf(bytes, offset);
    if (lineEnd < 0) return undefined;
    const size = Number.parseInt(
      decoder.decode(bytes.subarray(offset, lineEnd)).split(";")[0]!.trim(),
      16,
    );
    if (Number.isNaN(size)) throw new TypeError("socketFetch: malformed chunk size");
    if (size === 0) {
      // The trailer section ends with an empty line: the CRLF that ends the
      // `0` line is followed by trailer lines, then CRLF CRLF.
      const end = indexOfCrlfCrlf(bytes, lineEnd);
      if (end < 0) return undefined;
      const trailers = decoder.decode(bytes.subarray(lineEnd + 2, end)).split("\r\n");
      return { body: concat(chunks, length), trailers };
    }
    offset = lineEnd + 2;
    if (offset + size + 2 > bytes.length) return undefined;
    chunks.push(bytes.subarray(offset, offset + size));
    length += size;
    offset += size + 2;
  }
};

interface RawResponse {
  readonly statusLine: string;
  readonly headers: Headers;
  readonly body: Uint8Array;
}

/**
 * Parse the response once it is complete by its own framing (content-length
 * or the last chunk), or `undefined` while more is needed. `ended` means the
 * peer closed, which completes a response framed by the connection.
 */
const parseResponse = (
  bytes: Uint8Array,
  method: string,
  ended: boolean,
): RawResponse | undefined => {
  const headEnd = indexOfCrlfCrlf(bytes);
  if (headEnd < 0) {
    if (ended) throw new TypeError("socketFetch: response ended before its headers");
    return undefined;
  }
  const [statusLine = "", ...headerLines] = decoder
    .decode(bytes.subarray(0, headEnd))
    .split("\r\n");
  const status = Number(statusLine.split(" ")[1]);
  if (!Number.isInteger(status)) {
    throw new TypeError(`socketFetch: malformed status line "${statusLine}"`);
  }
  const headers = new Headers();
  parseHeaderLines(headerLines, headers);
  const payload = bytes.subarray(headEnd + 4);
  if (status === 204 || status === 304 || (status >= 100 && status < 200) || method === "HEAD") {
    return { statusLine, headers, body: new Uint8Array(0) };
  }
  if (headers.get("transfer-encoding")?.toLowerCase().includes("chunked")) {
    const decoded = decodeChunked(payload);
    if (decoded === undefined) {
      if (ended) throw new TypeError("socketFetch: truncated chunked response");
      return undefined;
    }
    parseHeaderLines(decoded.trailers, headers);
    headers.delete("transfer-encoding");
    return { statusLine, headers, body: decoded.body };
  }
  const declared = headers.get("content-length");
  if (declared !== null) {
    const length = Number(declared);
    if (payload.length < length) {
      if (ended) throw new TypeError("socketFetch: truncated response body");
      return undefined;
    }
    return { statusLine, headers, body: payload.subarray(0, length) };
  }
  return ended ? { statusLine, headers, body: payload } : undefined;
};

/** Read until the response is complete, without waiting for the peer to close. */
const readResponse = async (
  readable: ReadableStream<Uint8Array>,
  method: string,
  signal: AbortSignal | null | undefined,
): Promise<RawResponse> => {
  const reader = readable.getReader();
  const onAbort = () => void reader.cancel(signal?.reason).catch(() => {});
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    const chunks: Uint8Array[] = [];
    let length = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (signal?.aborted) throw signal.reason ?? new DOMException("Aborted", "AbortError");
      if (!done) {
        chunks.push(value);
        length += value.length;
      }
      const parsed = parseResponse(concat(chunks, length), method, done);
      if (parsed !== undefined) return parsed;
    }
  } finally {
    signal?.removeEventListener("abort", onAbort);
    reader.releaseLock();
  }
};

/**
 * `fetch` over a TLS socket (HTTP/1.1). Provide it as
 * `FetchHttpClient.Fetch` to send an Effect `HttpClient`'s requests over
 * `cloudflare:sockets`. Requests to hosts other than Modal's API go to the
 * global `fetch`.
 */
export const socketFetch = async (
  input: string | URL | Request,
  init: RequestInit = {},
): Promise<Response> => {
  const request = input instanceof Request ? input : undefined;
  const url = new URL(request ? request.url : input.toString());
  // Only Modal's API is behind the Worker-to-Cloudflare path that resets;
  // presigned blob URLs (`modal-storage.com`) and the user's own endpoints
  // work over `fetch`.
  if (url.protocol !== "https:" || !MODAL_API_HOST.test(url.hostname)) {
    return globalThis.fetch(input, init);
  }
  const signal = init.signal ?? request?.signal;
  if (signal?.aborted) throw signal.reason ?? new DOMException("Aborted", "AbortError");
  const method = init.method ?? request?.method ?? "GET";
  const body = await bodyBytes(init.body ?? request?.body);

  const head = [`${method} ${url.pathname}${url.search} HTTP/1.1`, `host: ${url.host}`];
  new Headers(init.headers ?? request?.headers).forEach((value, key) => {
    if (!FRAMING_HEADERS.has(key)) head.push(`${key}: ${value}`);
  });
  head.push(`content-length: ${body?.length ?? 0}`, "connection: close", "", "");

  const connect = await connectFn();
  const socket = connect(
    { hostname: url.hostname, port: url.port ? Number(url.port) : 443 },
    { secureTransport: "on", allowHalfOpen: false },
  );
  try {
    const writer = socket.writable.getWriter();
    await writer.write(encoder.encode(head.join("\r\n")));
    if (body !== undefined && body.length > 0) await writer.write(body);
    writer.releaseLock();

    const response = await readResponse(socket.readable, method, signal);
    return new Response(
      response.body.length === 0 ? null : (response.body as Uint8Array<ArrayBuffer>),
      {
        status: Number(response.statusLine.split(" ")[1]),
        statusText: response.statusLine.split(" ").slice(2).join(" "),
        headers: response.headers,
      },
    );
  } finally {
    // The response is complete; don't wait for the peer's close.
    socket.close().catch(() => {});
  }
};

/**
 * The HTTP client for Modal calls: `fetch`, except on a Cloudflare Worker,
 * where it sends over {@link socketFetch}. `FetchHttpClient` reads its
 * `Fetch` from the calling fiber on every request, so the socket `Fetch` is
 * pinned onto each request rather than provided to the layer.
 */
export const ModalHttpClient: Layer.Layer<HttpClient.HttpClient> = Layer.effect(
  HttpClient.HttpClient,
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    return isWorkerd()
      ? HttpClient.transform(client, (response) =>
          Effect.provideService(
            response,
            FetchHttpClient.Fetch,
            socketFetch as typeof globalThis.fetch,
          ),
        )
      : client;
  }),
).pipe(Layer.provide(FetchHttpClient.layer));
