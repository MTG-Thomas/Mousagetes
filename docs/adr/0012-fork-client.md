<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0012. Adopt and fork the ACP web client as the m8s client base

- Status: Accepted
- Date: 2026-09-19

## Context

The stock client is a good starting point but lacks what m8s needs: no
`session/list` (ADR 0010), no m8s lane/budget/swarm surfaces, it ignores
tool content (R8), and it loads third-party telemetry (R15). It is
MIT-licensed and small.

## Decision

Fork the ACP web client, pinned to a known version, as the m8s client
base. The fork adds a lane-roster method (an ACP extension via a custom
method or `_meta`), m8s branding, telemetry stripping, and later the
swarm view. Phases 1-3 use the stock build; the fork is Phase 4 work.

## Consequences

- m8s owns the client's UI and supply chain (MIT is compatible with
  AGPL-3.0).
- Upstream tracking is a maintenance cost; pin and diff deliberately.
- The roster extension must be namespaced and documented so other ACP
  clients can ignore it.
- Until the fork lands, roster and UX gaps are accepted per ADR 0010.
