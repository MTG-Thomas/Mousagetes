<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0002. The daemon stays the sole owner of `muse serve`

- Status: Accepted
- Date: 2026-09-19

## Context

MSP (`muse serve`) is stdio-only: the client owns the process's stdin and
stdout and is its only connection. It has a writer lease; a second host
cannot attach. m8s v1 already respects this: the daemon owns one serve
host and exposes a JSON-lines control API on a Unix socket. Adding more
client surfaces (ACP, web) risks a second process reaching for MSP.

## Decision

The daemon is the **only** process that talks to `muse serve`. Every
client surface — the ACP adapter, the v1 web view, the CLI — is a client
of the daemon control API. No adapter spawns its own serve host, and no
second supervisor attaches to a host already owned by the daemon.

## Consequences

- Writer-lease correctness is preserved by construction; there is no
  path by which two surfaces contend for a lane.
- Adapters are stateless and replaceable; all lane state lives in the
  daemon and its file-backed records.
- The daemon is a single point of contention; this matches v1 and is
  acceptable. Throughput is bounded by the control socket, not by the
  number of clients.
- Adapters must handle daemon bounces and reconnection; a dead adapter
  must never affect a running lane.
- A future "many adapters, one daemon" fan-out is an extension of the
  control API, not a change to this decision.
