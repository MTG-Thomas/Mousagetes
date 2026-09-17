<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# Lane-visibility protocol (repo standard)

Every lane working a GitHub issue follows this protocol so the human can
see swarm state from GitHub alone: issue labels/assignees/comments plus
the Moonshot project board plus the lane's PR. Until the board moves work
(see "Known blocker" below), label + assignee + comments are the source
of truth.

## 1. Pickup: claim the issue

As the first action after reading the issue, run:

```bash
gh issue edit N --add-label lane:active --add-assignee <owner>
```

`<owner>` is the GitHub user whose credentials the lane operates under
(today: `MTG-Thomas`). Agent session names are not GitHub users and
cannot be assignees — the lane session name goes in the claim comment
and in the board Lane field instead.

Then post a claim comment on the issue with all four fields:

```bash
gh issue comment N --body "Claim: lane <lane-name> picks up #N.

- Lane: <lane-name>
- Branch: lane/<N>-<slug>
- Worktree: <absolute worktree path>
- Host: <hostname>
- Owner (GitHub user): <owner>"
```

Steady-state rule: the `lane:active` label is cleared on merge or
abandon — by the **coordinator at merge** (the coordinator owns the
merge gate), by the **lane itself on abandon** (post an abandon comment
and remove the label with `gh issue edit N --remove-label lane:active`).

## 2. Branch and PR linkage

Branch name (one lane per branch, one writer per checkout):

```text
lane/<issue>-<slug>
```

Example: `lane/13-lanes-doc` for issue #13.

The PR body links the issue so the board and timeline stay connected:

- `Addresses #N` when the PR advances but does not complete the issue.
- `Closes #N` when merging the PR completes the issue.

```bash
gh pr create --draft --title "..." --body "Addresses #N

..."
```

## 3. Board moves: Status, Lane, Host

Intended board: the repo-level Projects v2 board **"Moonshot"** with a
single-select **Status** field (`Ready`, `Claimed`, `In review`,
`Merged`) and text fields **Lane** and **Host**.

Move Status at each transition and keep Lane/Host current:

| Transition | Status | Command |
|---|---|---|
| Pickup (claim) | Ready -> Claimed | `gh project item-edit --id <item-id> --project-id <board-id> --field-id <status-field-id> --single-select-option-id <claimed-option-id>` |
| First push (draft PR open) | Claimed (Lane/Host set) | `gh project item-edit --id <item-id> --project-id <board-id> --field-id <lane-field-id> --text <lane-name>` and the same for `--field-id <host-field-id> --text <hostname>` |
| Ready for review | Claimed -> In review | `gh project item-edit ... --field-id <status-field-id> --single-select-option-id <in-review-option-id>` |
| Merge (coordinator) | In review -> Merged | coordinator runs the `item-edit` to `Merged` and clears `lane:active` |

Resolve `<board-id>`, `<item-id>`, and field/option IDs at runtime with
`gh project view`, `gh project item-list`, and
`gh project field-list` — never hardcode or invent IDs.

### Known blocker (honest status)

Board moves currently fail on lane hosts: the `gh` token lacks the
`project` scope.

```text
error: your authentication token is missing required scopes [read:project]
To request it, run:  gh auth refresh -s read:project
```

Until the supervisor runs the refresh below (and the board/fields
exist), board moves cannot run; label + assignee + comments are the
source of truth. Lanes must still attempt the board move at pickup and
report its exact failure text in the start heartbeat — do not stall on
it.

### One-time board setup (supervisor/coordinator, after scope lands)

Do not invent a board URL or IDs — create them and read back the IDs:

```bash
# 1. Grant scope on the lane host (supervisor):
gh auth refresh -s read:project -s project

# 2. Create the board (coordinator/supervisor, once):
gh project create --owner MTG-Thomas --title "Moonshot"

# 3. Create the fields (once; Status is single-select):
gh project field-create --owner MTG-Thomas --number <board-number> \
  --name Status --data-type SINGLE_SELECT \
  --single-select-options "Ready,Claimed,In review,Merged"
gh project field-create --owner MTG-Thomas --number <board-number> \
  --name Lane --data-type TEXT
gh project field-create --owner MTG-Thomas --number <board-number> \
  --name Host --data-type TEXT

# 4. Add an issue to the board (per issue):
gh project item-add --owner MTG-Thomas --number <board-number> \
  --url https://github.com/MTG-Thomas/Mousagetes/issues/N

# 5. Move Status / set Lane/Host (per transition; see table above):
gh project item-edit --id <item-id> --project-id <board-id> \
  --field-id <status-field-id> --single-select-option-id <option-id>
gh project item-edit --id <item-id> --project-id <board-id> \
  --field-id <lane-field-id> --text <lane-name>
gh project item-edit --id <item-id> --project-id <board-id> \
  --field-id <host-field-id> --text <hostname>
```

Verify scope with `gh auth status` (look for `read:project`, `project`)
and list the board with `gh project list --owner MTG-Thomas`.

## 4. Heartbeats on the issue

Post issue comments at four points. The lane posts start + CI-green;
the coordinator posts queue/merge + completion because it owns the
merge gate:

1. **Start** (lane): claimed, label/assignee set, board move attempted
   (with exact failure text if blocked), plan in one or two lines.
2. **CI-green** (lane): CI green on the lane's PR, threads owned to
   resolution, `gh pr ready` run.
3. **Queue/merge** (coordinator): PR queued/merged, `lane:active`
   cleared.
4. **Completion** (coordinator): issue closed or next step stated.

```bash
gh issue comment N --body "Heartbeat (<stage>): ..."
```

Evidence rule: back every status claim with inspected evidence. Open
the implementation or test body cited and quote what it shows — grep or
search output alone never counts as verification. Anything checked but
not resolved (a failing test outside scope, a thread left open, a board
move still blocked) is recorded explicitly in the heartbeat as an
unresolved item; nothing is silently dropped.

## 5. Draft PR at first push

Open the PR as a draft at the first push, not at completion, so
Files-changed is a live progress view:

```bash
git push -u origin lane/<N>-<slug>
gh pr create --draft --title "..." --body "Addresses #N ..."
```

When CI is green and review threads are resolved, mark it ready:

```bash
gh pr ready <PR-number-or-url>
```

Only the coordinator merges. The lane's stopping condition is: PR
green, threads resolved, `ready-for-merge` reported on the issue.
