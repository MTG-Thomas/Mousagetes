<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0008. The ACP adapter lives in its own package

- Status: Accepted
- Date: 2026-09-19

## Context

`scripts/muse-msp.py` is a single ~5,900-line, ~240 KB file that owns the
daemon, the CLI, the bus, the board compiler, and the v1 web view. Tests
load it by path with `importlib`. The ACP plan puts several workstreams
(protocol core, command mapping, transport) in flight at once, and a
single file makes them all collide on the same lines. ACP work also does
not belong inside the daemon: by [ADR 0002](0002-daemon-authority.md) the
adapter is a separate client of the control socket.

## Decision

New adapter code lives in a dedicated Python package, `scripts/m8s_acp/`
(`jsonrpc.py`, `stub.py`, `cli.py`, and later `mapping.py` and
`daemon.py`). `muse-msp.py` is not modified for ACP work except, later, a
thin shared daemon-client extraction. Decomposing the monolith itself is
tracked separately and must be behavior-preserving; it is not a
precondition for ACP work.

## Consequences

- Workstreams get distinct files, so parallel lanes stop sharing a
  collision zone; the module boundary is the interface.
- Tests import the package normally instead of loading the monolith by
  path.
- Some daemon-client logic is duplicated until the shared helper is
  extracted from the monolith. This is accepted temporarily; the
  extraction is behavior-preserving and test-covered by the existing
  suite.
- A new entrypoint, `scripts/m8s-acp`, fronts the package; `scripts/m8s`
  keeps pointing at the daemon CLI.
- The monolith refactor remains desirable but optional, so ACP progress
  never blocks on it.
