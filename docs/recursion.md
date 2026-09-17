<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# Recursive backlog generation (Moonshot recursion)

Any m8s node with an empty lane queue and usage headroom can pad out its
own journey: partition the roadmap into non-overlapping areas, send out
read-only scouts to draft one lane brief per area, verify the briefs are
pairwise collision-free, and hand the verified set to lane dispatch.
Recursion is the [board-to-lane compiler](compile.md) made operational —
the compiler ranks and collision-checks known issues, while recursion
discovers the *next* issues worth filing and scheduling.

Nothing here overrides [docs/lanes.md](lanes.md): pickup (label
`lane:active` + assignee + claim comment + board-move attempt), draft PR
at first push, and start + CI-green heartbeats stay authoritative.
Dispatch order follows [docs/heartbeat.md](heartbeat.md); ranking and
collision semantics follow [docs/compile.md](compile.md).

## 1. When to recurse

Recurse when **both** hold:

- **Empty lane queue.** No unclaimed Ready issue in the Moonshot
  milestone, no expired lease awaiting requeue, no `attention` item from
  the last reconcile. Check with (flags verified via `--help`):

  ```bash
  gh issue list --repo OWNER/REPO --milestone Moonshot \
    --json number,title,labels,state
  scripts/m8s board reconcile --board board.json
  ```

  Recurse only when the reconcile output has no `propose`, `requeue`,
  or `attention` actions left — i.e. there is genuinely nothing to
  dispatch.

- **Usage headroom.** The node (and the provider subscription window)
  has budget to spend on speculative planning work: no lane
  `overBudget`, no `budget.exceeded` event unacknowledged, no human-set
  spend freeze. Scouts are cheap but not free — each one is a served
  session with its own token cost.

Do **NOT** recurse when any of these hold:

- **Lanes still staffed.** Live sessions are working, or claimed issues
  are awaiting CI/review. Recursion never preempts real work; re-check
  the roster (`scripts/muse-msp.py list`) first.
- **Budget tight.** Any `overBudget` flag, any recent
  `budget.exceeded` event, or an explicit human spend cap. Paging the
  human about spend is the compiler's `page-human` path
  ([docs/compile.md](compile.md)); recursion must not route around it.
- **Human decisions pending.** Any `decision-blocked` spec
  (`needs-product-call`, `needs-auth`, `needs-spend`, or body markers
  per [docs/compile.md](compile.md)) is unresolved. Scouts investigate;
  they never answer product/auth/spend questions on the human's behalf.
  Clear the page first, then recurse.

## 2. Area-partitioning rules

Before launching scouts, the recursing node partitions the roadmap
(source tree + open-issue backlog) into **non-overlapping areas** so
scouts cannot propose colliding lanes. Rules:

1. **Partition by directory, ownership, or collision zone** — e.g.
   `scripts/` vs `docs/` vs `SKILL.md` surfaces, or one area per
   top-level directory. Follow the same overlap rule the compiler uses
   ([docs/compile.md](compile.md), P3 overlap): same branch, same
   checkout, or equal/nested files count as overlap.
2. **No shared files between areas.** Each file path (or path prefix)
   belongs to exactly one area. Shared infrastructure both areas would
   touch (e.g. a common test harness) goes in exactly one area, named
   explicitly, and the other area's brief must cite it as a
   cross-area dependency rather than claiming it.
3. **No shared branches between areas.** Each area maps to a distinct
   future `lane/<issue>-<slug>` branch family; two areas never propose
   lanes on the same branch.
4. **No shared open issues between areas.** Each open issue is assigned
   to at most one area for citation. If an issue genuinely spans areas,
   assign it to one and note the spillover as a cross-area dependency.
5. **Write the partition down first.** List areas with their path
   prefixes and assigned issue numbers in the recursion kickoff note
   (an issue comment on the tracking issue), so the synthesis check in
   section 4 has something auditable to verify against.

A partition with fewer than two areas is not recursion — it is just
planning. Re-partition whenever the synthesis check fails (section 4).

## 3. Scout brief template

Scouts are **read-only**: no commits, no PRs, no board writes (no
`gh issue edit`, no `gh pr create`, no `bus claim`). They read code,
issues, and CI state, then each writes exactly one brief file,
`briefs/<area>-lanes.md` (committed by the recursing node, not the
scout — or collected as lane output and committed once, so scout
sessions themselves never touch git history).

Each brief **must** contain:

- **Open-issue citations** — every candidate lane traces to a filed
  issue (`gh issue view N` body quoted, not just the number), per the
  file-the-anchor-issue duty in [docs/heartbeat.md](heartbeat.md). A
  candidate with no issue gets one filed before synthesis.
- **Acceptance criteria per candidate lane** — checkable boxes, in the
  style of the issue that spawned the recursion.
- **Collision zones** — files, branches, and checkouts each candidate
  would touch, stated as paths so synthesis can pairwise-compare.
- **Gate flags** — `decisionBlocked` class if any (`product | auth |
  spend`, same vocabulary as [docs/compile.md](compile.md)),
  plus any budget sensitivity.
- **Evaluated-and-excluded list** — ideas the scout considered and
  rejected, each with a one-line reason (duplicate of #N, blocked on
  decision X, too small to be a lane, collides with area Y). An empty
  excluded list means the scout did not look hard enough — send it back.

Launch one scout per area (flags verified via `--help`):

```bash
scripts/m8s launch --name scout-<area> \
  --workspace /tmp/m8s-lanes/scout-<area> \
  --prompt 'Read-only scout for area <area> (paths: <prefixes>, issues: <numbers>). No commits, no PRs, no board writes. Write briefs/<area>-lanes.md per the template in docs/recursion.md section 3.'
```

Copy-paste brief template:

```markdown
# Scout brief: <area>

- Area paths: <path prefixes owned by this area>
- Assigned open issues: <#a, #b, ...>
- Scout session: <session name>, <date>

## Candidate lanes

### 1. <short title> (cites #<N>)

- Issue: #<N> — "<issue title>" (quote the relevant body lines)
- Acceptance criteria:
  - [ ] <checkable criterion 1>
  - [ ] <checkable criterion 2>
- Collision zone: files <paths>, branch `lane/<N>-<slug>`
- Gate flags: none | decisionBlocked: <product|auth|spend> — <reason>

## Cross-area dependencies

- <what this area needs from area Y, without claiming its files>

## Evaluated and excluded

- <idea> — excluded because <reason>
```

## 4. Collection and synthesis procedure

1. **Collect.** Gather every `briefs/<area>-lanes.md`. A missing brief
   blocks synthesis — re-send the scout once, then reassign the area;
   never synthesize around a silent area (a stall is a stall, per
   [docs/heartbeat.md](heartbeat.md) step 5).
2. **Completeness check.** Every brief has all five content blocks
   (citations, acceptance criteria, collision zones, gate flags,
   excluded list). Reject thin briefs back to the scout.
3. **Pairwise collision-freedom check.** For every pair of areas,
   compare collision zones using the compiler's overlap rule
   ([docs/compile.md](compile.md)): shared files (equal or nested),
   shared branches, shared checkouts, or the same open issue cited by
   two areas. Any overlap **fails the check** — the offending
   candidates do not get quietly dropped; instead the node
   **re-partitions** (section 2) and re-briefs the affected areas.
4. **Decision-blocked triage.** Candidates flagged `product | auth |
   spend` compile to `status: decision-blocked` with `pageHuman: true`
   per [docs/compile.md](compile.md) — they page the human, they never
   enter the dispatch set.
5. **Synthesize the dispatch set.** The surviving candidates become
   lane specs: `{issue, branch lane/<N>-<slug>, checkout
   /tmp/m8s-lanes/<lane>, brief, priority}` ranked by the arbitration
   formula (`score = priority x (1 + dependency depth) x staleness`,
   ties by issue number asc) from [docs/compile.md](compile.md). Record
   the ranking rationale (`priority.why`) per spec so each dispatched
   lane cites why it was scheduled.

The synthesis output is a single dispatch list (an issue comment or a
`dispatch-<date>.md` file) naming each lane, its brief source, and its
rank — the input to section 5.

## 5. Handoff to lane dispatch

Each verified brief becomes one or more implementation lanes, dispatched
in the heartbeat order ([docs/heartbeat.md](heartbeat.md) step 4):
unclaimed Ready issues first in milestone order (ranked per the
synthesis in section 4), expired-lease requeues before new work, and
every lane tracing to a filed issue.

New lanes follow [docs/lanes.md](lanes.md) pickup exactly:

```bash
gh issue edit N --add-label lane:active --add-assignee <owner>
gh issue comment N --body "Claim: lane <lane-name> picks up #N. ..."
git push -u origin lane/<N>-<slug>
gh pr create --draft --title "..." --body "Addresses #N ..."
```

(flags verified via `--help`; `gh pr ready` when CI is green and
threads are resolved). The recursing node does not claim the lanes
itself — it publishes the dispatch set and lets lanes (or the
heartbeat loop) pick them up, so ownership stays one-writer-per-branch
from the first commit.

Stopping condition for a recursion pass: every synthesized lane either
claimed, or explicitly deferred with a reason recorded on the tracking
issue. Unresolved items (a failed collision check, a paged human, a
brief never delivered) are recorded explicitly — nothing silently
dropped, per the evidence rule in [docs/lanes.md](lanes.md).
