<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0005. Keep Python stdlib-only; websocket via an external bridge

- Status: Accepted
- Date: 2026-09-19

## Context

m8s states "stdlib only — no dependencies" and CI enforces a dependency-
free install. Remote phone clients need a websocket endpoint, and the
Python standard library has no websocket server. The realistic choices
are to add a Python dependency, hand-roll a websocket implementation, or
use an external bridge process.

## Decision

The adapter exposes only **stdio** (and optionally a Unix socket). Remote
websocket exposure is provided by an **external bridge process** (for
example `stdio-to-ws`), not by a Python dependency. If a native `ws`
transport is later desired, that is a new decision that supersedes this
one.

## Consequences

- The install stays dependency-free; the ACP adapter is plain Python.
- One more process to run and supervise on the host. The runbook
  (`docs/acp-remote.md`) documents it.
- TLS termination and client authentication are the bridge's and the
  private mesh's responsibility, not the adapter's.
- If the bridge becomes a maintenance burden, the escape hatch is a
  native transport behind a superseding ADR; the mapping layer is
  unaffected either way.
