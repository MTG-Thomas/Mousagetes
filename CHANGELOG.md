# Changelog

## v0.6.0
- ACP client surface (m8s v2): a daemon-backed Agent Client Protocol
  adapter (`scripts/m8s-acp`) so ACP clients and a phone browser can
  drive m8s-owned Muse lanes. One lane maps to one ACP session; prompts
  stream `session/update`; approvals and user input relay as
  `session/request_permission` and `elicitation/create`; model, effort,
  and approval mode are reachable as slash commands and as mode/model
  pickers. No filesystem or terminal capabilities are advertised to
  clients.
- `scripts/m8s_acp/`: stdlib-only package (JSON-RPC framing, transport,
  daemon control client, command mapping) plus a transport test stub,
  with a frozen `LaneMapping` seam between the transport and the mapping.
- Plan and decisions: `docs/acp.md`, `docs/adr/` (0001-0012), the remote
  transport runbook `docs/acp-remote.md`, and the client fidelity
  evaluation `docs/acp-client-eval.md`.

## v0.5.0
- `m8s reload` hot upgrade: the daemon finishes its response, shuts
  down cleanly, and execs the script file fresh, so edited code takes
  effect without a manual down/up cycle. Watch streams drop and the
  owned serve host restarts across the bounce; the roster re-resolves
  per the heartbeat loop.
- `m8s adopt` roster repair: re-registers a live server-side session as
  owned (by id or server-side name, optional `--name` alias) without
  starting a turn, restoring post-bounce supervision when the in-memory
  ownership table is lost but sessions persist.
- Roster snapshot/restore across reloads: launch/adopt/reload persist
  ownership to `sessions.json`; the fresh daemon re-registers only
  sessions the new serve host still knows (dead ids dropped, never
  resurrected) and records `controller.rosterRestored`. In-flight turns
  still die with the old serve host — sessions pick up on demand, but a
  running turn needs a re-send.

- `m8s web` v1 local web client (stdlib only, no new dependencies):
  localhost-only bridge with a token-gated single page — lane list,
  fused health, live event tail over SSE, transcript/pending views, and
  a send box for dispatching advice to the coordinator lane. Token is
  stored `0600` under the runtime dir; loopback-only by default, with
  `--allow-remote` permitting an explicit non-loopback bind (e.g. a
  Tailscale IP, still token-gated).
- Web live refresh: each SSE event routes to its panel (status/usage →
  lanes, turns → lanes+health+open transcript, approvals/inputs/blockers
  → lanes+health+pending, stuck/budget → fast health), debounced per
  panel with blockers jumping the queue; per-panel "updated Xs ago"
  stamps, alias-aware selected-session tracking, and a server-side
  `?kinds=` filter on events/stream.
- Coordinator single-select: the send panel has a coordinator dropdown
  persisted server-side (`GET/POST /api/coordinator`); the send box
  defaults to it unless overridden.
- Coordinator advice via intent bus: coordinators live outside `muse
  serve`, so the roster comes from enrolled hosts + bus lanes
  (`GET /api/coordinators`) and advice publishes a versioned
  `m8s.intent` (verb `advise`, `POST /api/advise`) — never a turn
  submit; the smoke intent is visible to coordinators via `bus read`.
- Roster honors intent TTL: expired intents (including smoke tests) drop
  out of the coordinator list instead of lingering past expiry.
- Phase 1 Inbox: daemon-side `inbox` aggregate (per-lane pending with
  inline per-lane errors), `GET /api/inbox`, `POST /api/approve`
  (choice + requirementId race guard + optional feedback) and
  `POST /api/clarify` bridge endpoints, and a full-width inbox panel
  with choice buttons, denial feedback, and clarification boxes, live-
  refreshed on approval/blocker events.
- Phase 2 Fleet kanban: lanes render as cards grouped blocked/stuck/
  running/idle with per-lane budget meters (used vs max tokens, warn at
  80%, over state on breach) driven by existing list data.
- Phase 3 Session view: transcript renders typed rows (user/tool/subagent/
  turn markers, usage collapsed) from the materialized view with cursor
  paging, inline file diffs via `item/readOutput` (`GET /api/patch`),
  and run controls (cancel/interrupt/steer-active-turn via
  `POST /api/turn`).
- Phase 4 Board view: `GET /api/board?repo=` runs a read-only gh export
  plus reconcile in the bridge (no daemon change) and renders needs-you
  (page-human + attention), propose, requeue, queued, and in-sync groups
  with repo persisted per browser.

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
