<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# Board-to-lane compiler (Moonshot P5)

Desired state (GitHub issues + milestones) compiled into lane specs
(branch, worktree scope, brief, priority), reconciled against actual lane
state on a planning cadence.

## Input shape: file-based board snapshot

The compiler consumes a JSON snapshot file, not the GitHub API directly:

```json
{
  "schemaVersion": 1,
  "repo": "OWNER/REPO",
  "exportedAt": 1700000000.0,
  "milestones": [{"number": 1, "title": "Moonshot", "dueOn": null}],
  "issues": [
    {
      "number": 6, "title": "Board-to-lane compiler",
      "labels": ["priority-high"], "milestone": "Moonshot",
      "state": "open", "updatedAt": 1700000000.0, "body": "...",
      "scope": {"branch": "lane/m8s-p5-compiler",
                "checkout": "/tmp/m8s-lanes/m8s-p5-compiler",
                "files": ["scripts/muse-msp.py"]},
      "dependsOn": [5],
      "decisionBlocked": null
    }
  ]
}
```

Only `issues[].number` is required; everything else has a default
(branch slug derived from the title, priority weight 1, no deps, no
scope). `scope` / `dependsOn` / `decisionBlocked` may be annotated by the
coordinator on the snapshot file — the exporter's conventions are below,
and hand edits are a supported input path.

`scope.checkout` is the lane's worktree root. Lanes on one host share the
parent lanes dir; each lane owns its checkout exclusively (one writer per
checkout, P4 lease rule).

## Exporter: why issues + milestones, not Projects v2

`board export --repo OWNER/REPO --out board.json` builds the snapshot
with read-only `gh` calls (`gh issue list --json
number,title,labels,milestone,updatedAt,body` plus the REST milestones
endpoint). It never writes to GitHub — the coordinator owns board writes.

Projects v2 columns are deliberately out of scope for the exporter: the
v2 API is GraphQL-only with cursor pagination, which fits poorly behind
stdlib/`gh` one-liners. Project-column state travels as issue
labels/milestones instead, and the snapshot file stays the compiler's
input contract. Graduating the exporter to GraphQL later changes the
exporter only, not the compiler.

## Arbitration formula

Per moonshot spec: `score = priority x (1 + dependency depth) x staleness`.

- **Priority** — label weight: critical/p0 = 4, high/p1 = 3, medium/p2 =
  2, low/p3 = 1; highest label wins, unlabeled = 1.
- **Dependency depth** — longest chain of *open* issues transitively
  depending on this one (0 when nothing depends on it). Blockers
  therefore outrank the blocked, so rank order is dependency-safe.
  Closed-issue deps don't count; `dependsOn` loops resolve
  deterministically (back edge scores 0) and are listed in
  `dependencyCycles` for the coordinator to break.
- **Staleness** — factor `1 + idleDays / 30`: a month-old issue at equal
  priority/depth outranks a fresh one without dwarfing the priority term.
  Unparseable timestamps read as fresh rather than failing the plan.

Ranking is by `(score desc, issue number asc)` — fully deterministic.
Every spec carries `priority.why` (weight source, depth, idle days and
factor), so a scheduled lane cites exactly why it was scheduled.

## Collision rules (P4 reuse)

No parallel ownership system: the plan collides against the live P4
lease table (`claim_table()`) plus the plan's own already-scheduled
specs, using the P3 overlap rule (same branch, same checkout, or
equal/nested files):

- A spec whose branch/checkout sits under a **live P4 lease** queues
  with `queuedBehind` naming the holding host/lane.
- A spec overlapping an **already-scheduled spec** queues with
  `queuedBehind` naming the winning issue.
- First schedulable spec in rank order wins each zone; expired leases
  just work (reassignment, P4 semantics) and surface as `requeue`.
- Decision-blocked specs never enter the collision pass.

## Decision-blocked rules (P2 alignment)

Signals, in precedence order: explicit snapshot field
`decisionBlocked: {class, reason}` → decision labels (`needs-product-call`,
`needs-auth`, `needs-spend` families, see `DECISION_LABELS`) → body
markers ("decision required", "waiting on human", …). Classes are
`product | auth | spend`; `spend` reuses the P2 `decisionClass: spend`
value so paging surfaces stay uniform.

Decision-blocked lanes compile to `status: decision-blocked` with
`pageHuman: true`, never schedule, and `reconcile` records a
`board.decisionBlocked` event carrying the `decisionClass` — a page, not
a guess.

## Reconcile loop and cadence

`board reconcile --board board.json` compares the plan against live P4
leases and recent supervision events (the same file-backed state
`list`/`events` show: `lane.stuck`, `budget.exceeded`, `claim.expired`)
and emits one action per spec:

| Action      | Meaning                                              |
| ----------- | ---------------------------------------------------- |
| `propose`   | Free to schedule: operator runs `bus claim` + `launch` |
| `in-sync`   | Branch already leased — nothing to do                |
| `queued`    | Collision — waits, with cited winner                 |
| `page-human`| Decision-blocked — human call required               |

Plus `requeue` (expired leases on desired branches, from P4 expiry
semantics) and `attention` (stuck / over-budget signals needing an
owner). Reconcile never claims, launches, or guesses — it proposes and
pages; the operator (or a later scheduler phase) performs.

**Cadence** (documented, operator-owned): run
`scripts/m8s board reconcile --board <snapshot>` on a planning timer
(cron/systemd, suggested every 15 minutes) after refreshing the snapshot
with `board export`; feed its `actions`/`attention` into the existing
supervision surfaces — the CI watcher (failed checks → requeue/propose)
and the stale-lane reaper (`lane.stuck` → `attention` → page). Between
ticks, `bus list` expiry announcements and `events`/`watch` carry live
state with no extra plumbing.

## Operator surface

```bash
scripts/m8s board export --repo OWNER/REPO --out board.json
scripts/m8s board plan --board board.json
scripts/m8s board reconcile --board board.json
```

All three run daemonless (no controller socket, no second machine
needed); stdlib only.
