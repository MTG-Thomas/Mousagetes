<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# Host capacity planning (Moonshot P3+, issue #19)

A single host holds ~20-32 owned sessions before `session/start` begins
rejecting with loaded-session capacity exhausted (retryable). Bouncing the
controller under live lanes is not a repair path — it wipes the in-memory
roster — so this doc defines the structural fix in three parts: lifecycle
(when idle lanes unload), dispatch gating (stop proposing past the gate),
and pre-ceiling alerting (shed load before launches fail).

## 1. Owned-session lifecycle

```text
active → standby → unloaded → worktree/branch cleanup
```

- **Active.** The lane holds a session and is doing (or owed) work:
  live turns, pending approvals/inputs, uncommitted work, an open PR.
- **Standby.** Duty is complete but the lane stays resident for the
  **standby window** (`UNLOAD_STANDBY_SECONDS`, default 30 minutes,
  `M8S_UNLOAD_STANDBY_SECONDS` override) so review/CI follow-up has a
  live session to land on. Stand-down is supervisor-owned and unchanged
  (see [docs/heartbeat.md](heartbeat.md) §5b): send the wrap-up
  instruction, let the session land or report its work.
- **Unloaded.** Past standby, an idle lane unloads via the explicit
  attributable path below. Its branch lease releases, its alias/budget
  slots free, and its id persists in the retired set so a daemon bounce
  never resurrects it.
- **Cleanup.** Worktree/branch removal stays with the lane/coordinator
  flow (merge or abandon) — unload ends supervision, not repo state.

The standby window doubles as the child-activity tripwire: every serve
notification (turn progress, subagent/task relays, approvals) refreshes
`lastActivity` and re-arms standby, so a lane with live children never
looks idle. A session with no activity signal at all cannot prove
standby and is refused as uncertain.

## 2. Unload path (explicit, attributable, no override)

```bash
scripts/muse-msp.py unload <session> --reason capacity --by <operator>
```

`unload` judges the **shared departure guards** (the retire guards from
issue #21 — one code path, `_departure_checks`, never duplicated), then
its own unload-only refusals. There is deliberately **no `--force`**:
guarded lanes never unload; the supervisor override stays `retire
--force`, which ends supervision while work is still unlanded.

Never unload a lane with:

- **uncommitted work** — dirty worktree (`uncommittedWork`);
- **active children** — a live turn (`activeTurn` marker, set on
  `turn/start`, cleared on `turn/completed` or explicit turn
  cancel/unqueue), pending approvals/inputs, or a dead turn awaiting
  owner action (`sessionBusy`);
- **uncertain external effects** — an open PR on the lane branch
  (`openPR`), transcript activity inside the standby window, or no
  activity signal yet (`standby`).

Success records a `lane.unloaded` event with the operator (`by`), the
reason, and the roster count before/after (`loadedBefore`/`loadedAfter`)
— attributable, and replayable from `events`. No timer auto-unloads: the
stale-lane reaper pages via `attention`; a supervisor (human or loop)
runs `unload`.

## 3. Dispatch gating (board compiler)

Admission posture is computed from loaded owned sessions vs the ceiling:

| Posture | Condition | Dispatch behavior |
|---|---|---|
| `open` | loaded < warn threshold | propose freely |
| `limited` | warn ≤ loaded < ceiling | admit at most one new lane per pass |
| `closed` | loaded ≥ ceiling | refuse every new lane until a slot frees |

Defaults: ceiling 20 (`M8S_SESSION_CEILING`), warn at 75% of ceiling
(`M8S_SESSION_WARN_AT`) — under the observed 20–32 failure band, and
overridable per host without a code change. `board plan`/`reconcile`
take `--loaded`, `--ceiling`, `--warn-at`; `loaded` defaults to the
live P4 lease count (a conservative daemonless floor — pass the true
roster count when the supervisor knows it).

Past the gate no new lane proposes: gated specs park as `queued` with
the loaded-vs-ceiling evidence cited in `queuedBehind`, and reconcile
maps them to `queued` actions with the same reason. `in-sync`,
collision-`queued`, `requeue`, and `page-human` are unaffected — the
gate only stops *new* admissions. Every plan carries a `capacity`
block (`loaded`, `ceiling`, `warnAt`, `posture`, `evidence`) so a
refusal always says why and what frees it. See
[docs/compile.md](compile.md) for the compiler contract.

## 4. Pre-ceiling alert

`board reconcile` emits a `host.capacity.warning` event whenever loaded
is at/above the warn threshold — a signal, not a rejection. It fires
below the point where the gate starts refusing (verified: warning at
`limited` while the top lane still proposes; silence when `open`), so
supervisors shed load (stand down + `unload` idle lanes) before
launches start failing.

## Operator surface

```bash
# Where the host stands (counts are cited, not guessed):
scripts/muse-msp.py list | jq '.result.sessions | length'

# Plan / reconcile against the true roster count:
scripts/m8s board plan --board board.json --loaded 19
scripts/m8s board reconcile --board board.json --loaded 19

# Watch the signal:
scripts/muse-msp.py events --limit 50 | grep host.capacity.warning

# Shed load, one attributable unload at a time:
scripts/muse-msp.py unload <idle-session> --reason capacity --by <operator>
```
