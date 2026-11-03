import type * as archil from "@distilled.cloud/archil";
import * as Redacted from "effect/Redacted";

/**
 * An allowed destination, with optional changes to outbound HTTPS requests
 * that match it. Headers set here overwrite whatever the sandbox sends, so
 * a credential injected at the boundary never enters the sandbox.
 */
export interface SandboxEgressTarget {
  /** IPv4 address, CIDR range, exact domain, or `*.`-wildcard domain. */
  target: string;
  /** HTTP headers to set on matching HTTPS requests. */
  headers?: Record<string, string | Redacted.Redacted<string>>;
  /** Forward matching requests to this URL instead. */
  forwardUrl?: string;
}

/**
 * Outbound network policy. Deny entries win when an address or domain
 * matches both lists. Wildcards match subdomains, not the apex domain.
 */
export interface SandboxEgress {
  /** What happens to destinations neither list names. */
  default: "allow" | "deny";
  /** Allowed destinations: addresses, CIDRs, domains, or targets with request changes. */
  allow?: ReadonlyArray<string | SandboxEgressTarget>;
  /** Denied addresses, CIDRs, or domains. */
  deny?: ReadonlyArray<string>;
  /**
   * Hosts whose in-flight HTTP(S) requests finish before the sandbox
   * pauses (ports 80 and 443 only). Doesn't grant access.
   */
  drainOnPause?: ReadonlyArray<string>;
}

/** A disk mounted into a sandbox. */
export interface SandboxMount {
  /** Disk id (`dsk-…`), e.g. `disk.diskId`. */
  disk: string;
  /**
   * Absolute directory to mount at. Required when more than one disk is
   * mounted; a single mount defaults to `/mnt/archil`.
   */
  path?: string;
  /** Mount this subdirectory of the disk instead of its root. */
  subdirectory?: string;
  /** Mount without write access. @default false */
  readOnly?: boolean;
  /**
   * Write without holding the disk's root delegation, so several writers
   * can mount it at once. @default false
   */
  conditional?: boolean;
  /**
   * Milliseconds to wait for the disk's exclusive root delegation before
   * the mount fails. Not allowed with `readOnly` or `conditional`.
   */
  queueMs?: number;
}

/** A port a sandbox serves publicly. */
export interface SandboxEndpoint {
  port: number;
  /** Public hostname that routes to the port. */
  hostname: string;
}

const secretValue = (value: string | Redacted.Redacted<string>) =>
  typeof value === "string" ? value : Redacted.value(value);

/** The wire shape of an egress policy. */
export const toEgressPolicy = (egress: SandboxEgress): archil.SandboxEgressPolicy => ({
  default: egress.default,
  allow: egress.allow?.map((entry) =>
    typeof entry === "string"
      ? entry
      : {
          target: entry.target,
          transform: entry.headers
            ? {
                headers: Object.fromEntries(
                  Object.entries(entry.headers).map(([k, v]) => [k, secretValue(v)]),
                ),
              }
            : undefined,
          forward_url: entry.forwardUrl,
        },
  ),
  deny: egress.deny ? [...egress.deny] : undefined,
  drain_on_pause: egress.drainOnPause ? [...egress.drainOnPause] : undefined,
});

const isArchilEgress = (entry: string | object) =>
  typeof entry === "string" && /^(mount|data)\.\*\.archil\.com$/.test(entry);

/**
 * Split off the allow-list entries Archil adds itself to a sandbox that
 * mounts disks (`mount.*.archil.com`, `data.*.archil.com`). They aren't
 * part of the desired policy, but an update must keep them.
 */
export const splitArchilEgress = (egress: archil.SandboxEgressPolicy | undefined) => ({
  managed: (egress?.allow ?? []).filter(isArchilEgress),
  policy: egress && { ...egress, allow: egress.allow?.filter((entry) => !isArchilEgress(entry)) },
});

/** The wire shape of a mount. */
export const toSandboxMount = (mount: SandboxMount): archil.SandboxMount => ({
  disk_id: mount.disk,
  path: mount.path,
  subdirectory: mount.subdirectory,
  read_only: mount.readOnly ?? false,
  conditional: mount.conditional ?? false,
  queue_ms: mount.queueMs,
});

/** Environment values, unwrapped for the wire. */
export const toEnv = (env: Record<string, string | Redacted.Redacted<string>> | undefined) =>
  env === undefined
    ? undefined
    : Object.fromEntries(Object.entries(env).map(([k, v]) => [k, secretValue(v)]));

/** A comparable form of an egress policy (header values are hashed out). */
export const egressIdentity = (egress: SandboxEgress | undefined) =>
  egress === undefined
    ? undefined
    : {
        ...egress,
        allow: egress.allow?.map((entry) =>
          typeof entry === "string"
            ? entry
            : {
                ...entry,
                headers: entry.headers
                  ? Object.fromEntries(
                      Object.entries(entry.headers).map(([k, v]) => [k, secretValue(v)]),
                    )
                  : undefined,
              },
        ),
      };
