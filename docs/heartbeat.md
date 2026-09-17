<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# m8s supervision heartbeat (forked from Wrangnarok heartbeat v2)

Periodic supervisor loop that drives the Moonshot milestone toward green:
reconcile actual swarm state (lanes, PRs, CI, issues) toward declared
desired state (the Moonshot milestone issues). One pass = roster ->
blockers -> git/PRs -> dispatch -> intervene -> report. See
[docs/moonshot.md](moonshot.md) for the control-plane concept and
[docs/lanes.md](lanes.md) for the lane-visibility protocol this loop
enforces — lanes.md is authoritative on pickup, board moves, heartbeats,
and PR linkage; nothing here overrides it.

## m8s fork adaptations (where this differs from Wrangnarok)

- **Paths.** The repo path is the Mousagetes checkout; lane worktrees live
  under `/tmp/m8s-lanes` — never `/tmp/muse-lanes`. One writer per
  checkout, one lane per branch (`lane/<issue>-<slug>`).
- **Merge authority is direct-merge-when-green.** There is no merge queue
  on this repo. The coordinator owns the merge gate: only the coordinator
  merges, clears `lane:active`, and moves the board to `Merged`.
- **Dispatch queue is the Moonshot milestone issues P1-P5, in milestone
  order.** Unclaimed Ready issues dispatch before new work; expired leases
  requeue. Rank by milestone order, then dependency depth, then staleness
  (see [docs/compile.md](compile.md)).

## The loop

Run one pass per cycle. Polling minimums: queue/CI ~5 min, lane turns
10+ min; no tight loops. Do not equate a quiet lane with failure while it
waits on an active CI run — check the external dependency first.

### 1. Roster (with post-bounce re-resolution)

Session aliases can change across daemon bounces — re-resolve roster
identity after every bounce. Never trust a cached session name.

```bash
scripts/muse-msp.py list
scripts/muse-msp.py events --limit 50
```

1. List live sessions; note any disappeared or replaced identities.
2. After any controller/daemon bounce, re-resolve: match sessions by
   durable identity (worktree, branch, claimed issue), not by alias.
3. Flag `down`/`stuck`/`blocked` from `list` + `health` output.
4. Correlate each live session to its claimed issue, branch, and worktree.
   A session with no claim, or two sessions on one checkout/branch, is a
   stall — handle under step 5.

### 2. Blockers

Scan all four signals every pass:

- `session/statusChanged` with `approvalPending` or `inputPending`;
  server `approval/request` and `userInput/request` (recorded as `blocker`
  events).
- `blocker` events in `events` output.
- Failed or cancelled `turn/completed` events.
- `view/gap` and `session/viewHealthChanged` events.

The controller acknowledges display of server requests but never approves
or answers automatically — apply the normal authorization rules first.
Stuck lanes (no transcript/repo activity past `M8S_STUCK_AFTER_SECONDS`,
default 30 minutes, or a failed/cancelled turn with no owner action)
appear as `stuck` in `list` with one `lane.stuck` event each; over-budget
lanes refuse new turns until the budget is raised (`decisionClass:
spend` — page the human, do not raise silently).

### 3. Git / PRs (untruncated)

Never `| head` or shortlog-truncate consequential git output; use full
hashes for merge decisions.

```bash
git log --format='%H %s' -n 20
git status -sb
gh pr list --repo MTG-Thomas/Mousagetes
gh pr checks <PR> --repo MTG-Thomas/Mousagetes
```

Per open lane PR: CI state, unresolved review threads, reported
conflicts, and whether the lane already ran `gh pr ready`. A lane that
says it is done while its PR is still open is a stall. Only the
coordinator merges, and only when CI is green and threads are resolved.

### 4. Dispatch (Moonshot milestone, P1-P5 order)

```bash
gh issue list --repo MTG-Thomas/Mousagetes --milestone Moonshot \
  --json number,title,labels,state
```

1. Every lane traces to a filed issue — file-the-anchor-issue duty: if a
   lane's work has no issue, file it before dispatching further.
2. Dispatch unclaimed Ready issues in milestone order (P1-P5); requeue
   expired leases before opening new work.
3. Lane names start with a letter. Launch via the controller:

```bash
scripts/muse-msp.py launch --name <lane> \
  --workspace /tmp/m8s-lanes/<lane> --prompt '<brief>'
```

4. Worktree retry discipline: bounded retries with backoff on worktree
   create/push transient failures; report persistent ones, do not loop
   forever.
5. New lanes follow [docs/lanes.md](lanes.md) pickup (label
   `lane:active`, assignee, claim comment, board-move attempt) and open a
   draft PR at first push.

### 5. Intervene (smallest restoring send only)

Intervene solely on concrete stall, with the minimal restoring
instruction. Stall signals: routine-question ending despite standing
authority; no transcript/repo activity after claimed immediate work; red
CI / unresolved threads / conflict with no owner action; "done" with PR
still open; two writers on one checkout; TUI awaiting absent approval.

For each stall, send the smallest instruction that restores progress,
then verify pickup after every consequential send: `accepted` is not
`running` — confirm the session actually picked up the turn (follow-up
`list`/`events` showing the turn running); recover or bypass stuck
sessions (re-send once, then restart via an authorized path or reassign
the lane — never force-migrate a live TUI-owned session).

```bash
scripts/muse-msp.py send <session> '<minimal instruction>'
scripts/muse-msp.py list   # confirm the turn is actually running
```

### 5b. Stand-down and retire (issue #21)

A served session whose duty is complete must not linger on the roster:
after 30+ minutes idle it accrues a `stuck` flag and masks genuinely
live lanes on the health screen. End it in two steps, supervisor-owned:

1. **Stand down by message.** Send the session a stand-down instruction
   (wrap up, land or report any uncommitted work, confirm idle). This is
   still just a `send` — the session does the wrapping up, not the
   supervisor.
2. **Retire.** Once the session confirms idle:

```bash
scripts/muse-msp.py retire <session>
scripts/muse-msp.py list   # session gone; no stuck accrual afterwards
```

`retire` confirms idle (no pending approvals/inputs, no dead turn
awaiting owner action), refuses sessions with uncommitted work or an
open PR, releases the lane's branch lease, and drops the session from
the roster, health, and stuck accounting. The retired id persists across
daemon restarts — a bounced daemon never resurrects it. The only
override is explicit:

```bash
scripts/muse-msp.py retire <session> --force
```

Never force-retire a session with uncommitted work or an open PR
without supervisor judgment: `--force` ends supervision while the work
is still unlanded. Retire records a `lane.retired` event; refusals are
typed (`sessionBusy`, `uncommittedWork`, `openPR`).

### 6. Report (material changes only)

No "still running" noise. Report to the user only: dispatches,
recoveries, merges, escalations (spend, auth boundaries, product calls),
and heartbeat comments required by [docs/lanes.md](lanes.md) (lane posts
start + CI-green; coordinator posts queue/merge + completion). Back every
status claim with inspected evidence — open the body cited, quote what it
shows; grep output alone never counts. Anything checked but unresolved
goes in the report as an explicit unresolved item; nothing is silently
dropped.
