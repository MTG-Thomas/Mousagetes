<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# MCP adapter for Codex (m8s-mcp)

A thin, stdlib-only Model Context Protocol front end for the m8s daemon,
consolidating the installed Node bridge (`muse-remote` on pve-t340) onto
Mousagetes. Codex reaches it over SSH stdio; it reaches Muse lanes only
through the existing m8s Unix control API into m8s-owned `muse serve`
hosts — one trusted host for `worktree` (YOLO) lanes, one enforced
read-only host for `read_only` lanes.

```
Codex --ssh stdio--> m8s-mcp (task pointers) --unix socket--> m8s daemon (trusted) --> lanes
                                          \--unix socket--> m8s daemon (read-only) --> lanes
```

Invariants (ADR 0002, ADR 0003, ADR 0005, ADR 0008):

- The adapter never spawns `muse serve` and never speaks MSP. The daemon
  is the only process that owns the serve host.
- All lane state lives in daemon records; adapter task records
  (`~/.local/state/m8s-mcp/tasks/`) hold only ownership pointers
  (task id, session id, admitted turn id, mode, commit).
- Adapter death or stdio disconnect performs no daemon call: running
  turns survive by construction and are never auto-replayed.
- No filesystem or terminal proxying: the server advertises
  `capabilities: {"tools": {}}` only — no resources, prompts, or roots —
  and the twelve tools below are the whole surface.
- Stdlib only, no dependencies.

## Tools

| Tool | Behavior |
| --- | --- |
| `health`, `models` | Daemon health plus adapter task counts; host model catalog |
| `sessions`, `session_read` | Lane metadata / point-in-time snapshot; never acquire the writer lease (no `session/resume`, `send`, or `adopt` on these paths) |
| `start` | Asynchronous lane task (`requestId` dedup, 4-active cap) |
| `status`, `result` | Bounded preview with exact pending approvals; terminal evidence bound to the admitted turn |
| `approve` | Relay of the exact offered approval triple; no persistent grants |
| `resume` | Explicit follow-up in the adapter-owned idle lane |
| `cancel` | Cancel of the adapter-owned admitted turn; terminal proof via `status`/`result` |
| `changes` | Git status and bounded diff; never commits, pushes, merges, or applies |

`read_only` (default) launches on the dedicated enforced host with
approval mode `onRequest` (defense in depth). Without a verified
read-only socket it fails closed with `unsupportedReadOnly` — there is
no approval-gated fallback on the trusted host. `worktree` is the
explicit YOLO selection: a detached worktree at the exact verified
committed ref with approval mode `allowAll` on the trusted host.
Always supply a fresh UUID `requestId` per new task: repeating it with
identical inputs replays the existing task, changed inputs reject, and
it is what makes an uncertain retry safe (the lane alias derives from
it, so a lost receipt adopts its orphan instead of launching twice).
Starts hold an exclusive registry lock across dedup, cap, worktree,
launch, and save, so concurrent identical starts yield one lane.

Reconnects reconcile against daemon evidence, not client death: a new
adapter process settles tasks with a terminal event for their admitted
turn, keeps supervised lanes live, and only marks lane-gone-without-
terminal tasks `interrupted` (never auto-replayed).

## Read-only host setup (daemon-owned, for Codex to review and execute)

`--disable-write` / `--disable-shell` are `muse serve` argv, fixed per
host for its lifetime ("neither is negotiable over the wire" —
`muse serve --help`). The adapter therefore never asks the shared
trusted daemon for read-only work. Instead, run a second daemon with
its own runtime dir (hence its own socket, pid, and event log) and an
overriding serve argv (`M8S_SERVE_ARGV`, advertised back via the
`health` control command as `serveArgv` so the adapter verifies rather
than assumes):

```bash
# As the daemon user on the host that owns the daemons:
export XDG_RUNTIME_DIR=/run/m8s-mcp-ro   # any private 0700 dir works
export M8S_SERVE_ARGV='muse serve --disable-write --disable-shell'
scripts/muse-msp.py up
scripts/muse-msp.py --help >/dev/null  # sanity: CLI still parses
# Verify the posture from the control socket before trusting it:
# health must list serveArgv containing --disable-write and --disable-shell.
```

Then point the adapter at both sockets (`--socket` defaults to the main
daemon socket, so only the read-only one needs naming):

```bash
scripts/m8s-mcp serve \
  --read-only-socket /run/m8s-mcp-ro/muse-msp-supervisor/control.sock
```

`health` reports the verified posture under `readOnly`
(`configured` / `enforced` / `serveArgv`). If the read-only daemon is
down or misconfigured, `read_only` starts fail with `daemonDown` /
`readOnlyMisconfigured` and `health` shows `enforced: false` — the
trusted host is never substituted silently.

## Intentional differences from the Node bridge

1. **Enforced read-only, fail-closed.** The old `read_only` flag mapped
   to CLI sandbox flags on bridge-owned sessions; here enforcement is
   per-host serve argv on a dedicated daemon, verified per use. Until
   that socket is supplied, `read_only` refuses rather than downgrades.
2. **Turn evidence, not SDK items.** `session/read` excludes history
   (`history.mode: none`), so previews and `result` evidence come from
   the `view/page` materialized view: `agentMessage` item text whose
   `item.turnId` equals the admitted turn (highest revision wins),
   bounded to 4,000 chars. `session_read(includeItems=True)` returns
   those plain item dicts, not notification frames. Full logs stay on
   the host like before.
3. **Bounded concurrency, evidence reconcile.** The 4-active-task cap
   is preserved and now lock-guarded; restart reconciles against daemon
   terminal/live evidence instead of blanket interruption. Per-lane
   budgets remain daemon-side (`budget` command).
4. **No Codex app-server emulation.** This is a twelve-tool MCP server,
   not the harness contract; it does not vendor restricted upstream
   code (the Grok investigation informed protocol choices only).

## Deployment (for Codex to review and execute)

On the host that owns the daemons (today pve-t340), as the daemon user:

```bash
# 1. Update to a commit containing scripts/m8s_mcp, then smoke it (below).
# 2. Start the main daemon if needed, then the read-only daemon (above):
scripts/muse-msp.py up
XDG_RUNTIME_DIR=/run/m8s-mcp-ro \
  M8S_SERVE_ARGV='muse serve --disable-write --disable-shell' \
  scripts/muse-msp.py up
# 3. Register the MCP server on the Codex machine (Windows), over SSH stdio:
codex mcp add m8s -- ssh pve-t340 python3 /path/to/Mousagetes/scripts/m8s-mcp serve --read-only-socket /run/m8s-mcp-ro/muse-msp-supervisor/control.sock
# 4. Verify from Codex: initialize, tools/list shows the twelve tools,
#    health shows readOnly.enforced true with both flags in serveArgv.
# 5. Keep the Node bridge registered until the cutover check below passes.
```

Cutover check: `health` (enforced read-only verified) + one `read_only`
start/status/result round-trip through `m8s-mcp`, with the Node bridge
untouched. Only then remove the old registration
(`codex mcp remove muse-remote` on Windows).

## Rollback

```bash
codex mcp remove m8s            # on the Codex machine
# Re-add the Node bridge registration if it was removed:
codex mcp add muse-remote -- <previous ssh invocation>
```

Rollback stops only adapter processes. Daemon lanes (both hosts) keep
running under m8s supervision; adapter task records under
`~/.local/state/m8s-mcp` are inert JSON and can be kept for inspection
or deleted. The read-only daemon can be stopped independently once no
adapter references its socket. No session migration is needed because
no lane ever belonged to the adapter.

## Migration of bridge-owned saved sessions/worktrees

No takeover, by design:

- Bridge SDK sessions (including tmux-owned ones) are never attached,
  resumed, or steered: `sessions`/`session_read` see only m8s lanes
  (tagged by owning host: `rw` or `ro`).
- To continue bridge work under m8s, `start` a new lane in the same
  source checkout (or a fresh worktree at the recorded commit) with the
  prior result pasted as context; leave the bridge's state directory
  and worktrees in place until reviewed.
- Preserve `/root/.local/state/muse-bridge` until the cutover check
  passes; its fixture worktree stays inspectable.

Migration limitations:

- Pre-enforcement adapter tasks (PR45 head: `read_only` launched
  approval-gated on the trusted host, records without a `daemon`
  pointer) are **not** upgraded: they keep routing to the trusted host
  and report `enforcement: approval-gated-legacy` in `status`. Re-start
  them to get enforced read-only; retire the legacy lanes by hand.
- `interrupted` tasks from any restart are never auto-replayed; an
  explicit `resume` re-drives the same lane after verifying it is idle.
- A lane deleted from the daemon with no terminal for the admitted
  turn reports `interrupted` with `lanePresent: false` — the work may
  still exist in the worktree; inspect before restarting.

## Smoke harness (no live tenant writes)

```bash
# Contract gates (fake sockets + temp git repos, no daemon, no network):
python3 -m unittest discover -s scripts/tests -p 'test_mcp_adapter.py' -v
python3 -m unittest discover -s scripts/tests -p 'test_serve_argv.py' -v
# live-shape smoke (daemons must be up; uses only list/health/models):
printf '%s\n' \
 '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
 '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
 '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"health","arguments":{}}}' \
 | scripts/m8s-mcp serve --read-only-socket /run/m8s-mcp-ro/muse-msp-supervisor/control.sock
```

The unit commands are the gates (33 tests). The last is read-only
against live daemons. None launches a lane.

## Limitations

- `read_only` requires the dedicated enforced daemon; without it the
  tool refuses (`unsupportedReadOnly`). Approval-gating alone is never
  presented as enforcement.
- Status previews and diffs are bounded; large patches exceed preview
  limits — use `changes` plus direct worktree inspection.
- Starts serialize on a registry lock (one at a time per
  state directory); concurrent Codex clients share daemon lanes but not
  adapter task records.
- Model selection passes through daemon `launch`; unknown models fail
  at the daemon with its own error.
- The read-only host is local-only: remote (`ssh`) hosts wrap the
  default trusted argv (`build_ssh_serve_argv` is unchanged).
