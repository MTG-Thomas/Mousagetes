<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# Claim / heartbeat / intent bus (Moonshot P4)

A boring bus between cooperative, mutually-trusting nodes carrying
versioned claim/heartbeat/intent messages with branch-lease expiry into
reassignment.

## Transport choice: NATS-subject-shaped local bus

The bus speaks three NATS-style subjects — `m8s.claims`, `m8s.heartbeat`,
`m8s.intent` — but the carrier is a file-backed local log, not a NATS
server:

- `bus.ndjson` in the daemon runtime dir: one `{"subject", "message"}`
  record per line (same pattern as `events.ndjson`).
- `claims.json` in the same dir: the authoritative lease table, keyed by
  branch.
- Agent-to-agent gossip reuses the P3 SSH peer CLI: a node runs
  `bus read` locally and replays it at the peer (`peer_cli_argv`). No new
  carrier, no TCP listener (MSP stays stdio-only), stdlib only.

Git-as-mailbox was considered and rejected: leases need
second-granularity expiry and cheap heartbeats, and a commit/push per
heartbeat would spam history and require the network.

Graduation path: replace `publish()` / `read_bus()` with NATS (later
gossipsub/A2A) publish/subscribe on the same subjects. Every message
already carries `type` + `version`, so that is a transport change, not a
redesign — `validate_bus_message()` rejects unknown versions outright so
a future v2 is a deliberate upgrade.

## Message types (all `version: 1`)

- `m8s.claim` on `m8s.claims`: `{claimId, host, lane, branch,
  checkout?, sessionId?, issuedAt, expiresAt}` — "host B holds lane X on
  branch Y".
- `m8s.heartbeat` on `m8s.heartbeat`: `{host, agentId, at, claims[]}` —
  the embedded claims are the re-gossip (presence broadcast).
- `m8s.intent` on `m8s.intent`: `{intentId, host, verb, lane?, branch?,
  detail?, sessionId?, at, expiresAt}`.

## Lease / expiry semantics

- One lane per branch, one writer per checkout. Claiming a branch with a
  live foreign lease fails `leaseHeld`; claiming a checkout whose writer
  holds a live lease fails `checkoutBusy`.
- Leases last `LEASE_TTL_SECONDS` (default 300 s,
  `M8S_LEASE_TTL_SECONDS` override). Re-claiming your own
  `(host, lane, branch)` refreshes the lease and keeps the `claimId`.
- A lease is live while unexpired **and** while its host is live per the
  P3 registry feed — a claim whose enrolled host reads dead is treated as
  expired, so a crashed owner cannot squat a branch.
- Heartbeat refreshes a host's leases and re-publishes each claim on
  `m8s.claims` plus one `m8s.heartbeat` gossip record. The P3
  `host heartbeat` action does this automatically for the heartbeaten
  host — there is deliberately no second heartbeat.
- `sweep_claims()` (run by `bus list`) announces each newly-expired lease
  once as a `claim.expired` event with `requeue: true`. Expired claims
  stay in the table as visible requeue state until released or
  reassigned: claiming an expired branch just works (reassignment).

## Operator surface

```bash
scripts/m8s bus claim --host B --lane X --branch lane/x --checkout /repo/x
scripts/m8s bus heartbeat --host B        # refresh + re-gossip B's leases
scripts/m8s bus intent --verb propose-plan --branch lane/x --detail '...'
scripts/m8s bus list                      # lease table with live/requeue
scripts/m8s bus release --branch lane/x
scripts/m8s bus read --subject m8s.intent --limit 50
```

`list` also carries the lease table (`claims`, plus each owned session's
live `lease` when its claim names the session), and every bus action
records an event (`claim.acquired`, `claim.heartbeat`, `claim.expired`,
`claim.released`, `intent.published`), so `events` / `watch` show bus
activity with no extra plumbing.
