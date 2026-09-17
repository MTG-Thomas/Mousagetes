# Changelog

## Unreleased

- `m8s health` fused swarm screen (issue #11): per member, liveness
  (`list` status + last-event age), recent turn activity, and progress
  (branch-ahead commits, read-only `gh` open-PR checks, pending
  approvals/inputs). Exactly three flags — `down` (listed but
  unresponsive past `M8S_DOWN_AFTER_SECONDS`, default 2h), `stuck`
  (P2 idle on events and commits, or a dead turn awaiting owner
  action), `blocked` (approval/user-input wait, wins over silence) —
  with P4 lease state riding along on each row.

## v0.2.0

- Per-lane budgets (`budget` command, `launch --max-tokens/--max-context-tokens/--models`):
  visible in `list`, enforced (over-budget lanes refuse new turns, disallowed
  models refused), breaches recorded as `budget.exceeded` with
  `decisionClass: spend`.
- Stuck-lane detection: idle past `M8S_STUCK_AFTER_SECONDS` (default 30 min,
  transcript + worktree activity) or failed/cancelled turns with no owner
  action surface as `stuck` in `list` plus `lane.stuck` / `lane.attention` events.
- Validated `call` passthrough: unknown MSP methods and missing-but-required
  `commandId` fail client-side with a typed `errorKind`, no daemon round-trip.

## v0.1.0

- Initial public release (AGPL-3.0-or-later).
- `muse-msp.py` (`m8s`): daemon owning one `muse serve` host with a Unix-socket control API.
- Curated lane commands (`launch`, `send`, `list`, `events`, `watch`, `pending`,
  `read`, `view`, `goal`, `turn`, `workflow`, `subagent`, `task`, session lifecycle).
- Generic `call` passthrough covering all 51 MSP schema methods with automatic
  `commandId` minting.
- `muse-remote.sh` for direct local session messaging without the daemon.
- Stdlib-only test suite (`python3 -m unittest discover -s scripts/tests`).
