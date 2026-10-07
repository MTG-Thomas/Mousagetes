<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# MCP adapter for Codex (m8s-mcp)

A thin, stdlib-only Model Context Protocol front end for the m8s daemon,
consolidating the installed Node bridge (`muse-remote` on pve-t340) onto
Mousagetes. Codex reaches it over SSH stdio; it reaches Muse lanes only
through the existing m8s Unix control API into the m8s-owned `muse serve`
host.

```
Codex --ssh stdio--> m8s-mcp (stateless tasks) --unix socket--> m8s daemon --stdio--> muse serve --> lanes
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

`read_only` (default) launches with approval mode `onRequest` in the
source checkout. `worktree` is the explicit YOLO selection: a detached
worktree at the exact verified committed ref with approval mode
`allowAll`. Supply a fresh UUID `requestId` per new task; repeating it
with identical inputs replays the existing task, changed inputs reject.

## Intentional differences from the Node bridge

1. **No OS-level read-only.** `--disable-write` / `--disable-shell` are
   `muse serve` argv, fixed per host for its lifetime ("neither is
   negotiable over the wire" — `muse serve --help`), and the shared
   daemon serves `--trust-workspace --disable-sandbox`. The adapter
   therefore never claims tool removal: `read_only` is an
   approval-gated posture (`onRequest`, no worktree, no auto-answer —
   nothing applies without an explicit `approve` call). Hard tool
   removal needs a dedicated read-only daemon host; the adapter already
   accepts `--socket`, so it can point at one once it exists. Calling
   anything "shell-disabled" on the shared host would be false.
2. **Turn evidence, not SDK items.** Progress previews come from
   daemon `session/read` / `view/page` tails, bounded to 4,000 chars;
   full logs stay on the host like before.
3. **No concurrency beyond the daemon.** The 4-active-task cap and
   interruption-on-restart semantics are preserved; per-lane budgets
   remain daemon-side (`budget` command) rather than adapter-side.
4. **No Codex app-server emulation.** This is a twelve-tool MCP server,
   not the harness contract; it does not vendor restricted upstream
   code (the Grok investigation informed protocol choices only).

## Deployment (for Codex to review and execute)

On the host that owns the daemon (today pve-t340), as the daemon user:

```bash
# 1. Update to a commit containing scripts/m8s_mcp, then smoke it (below).
# 2. Register the MCP server on the Codex machine (Windows), over SSH stdio:
codex mcp add m8s -- ssh pve-t340 python3 /path/to/Mousagetes/scripts/m8s-mcp serve
# 3. Verify from Codex: initialize, tools/list shows the twelve tools, health works.
# 4. Keep the Node bridge registered until the cutover check below passes.
```

Cutover check: `health` + one `read_only` start/status/result round-trip
through `m8s-mcp`, with the Node bridge untouched. Only then remove the
old registration (`codex mcp remove muse-remote` on Windows).

## Rollback

```bash
codex mcp remove m8s            # on the Codex machine
# Re-add the Node bridge registration if it was removed:
codex mcp add muse-remote -- <previous ssh invocation>
```

Rollback stops only adapter processes. Daemon lanes keep running under
m8s supervision; adapter task records under `~/.local/state/m8s-mcp`
are inert JSON and can be kept for inspection or deleted. No session
migration is needed because no lane ever belonged to the adapter.

## Migration of bridge-owned saved sessions/worktrees

No takeover, by design:

- Bridge SDK sessions (including tmux-owned ones) are never attached,
  resumed, or steered: `sessions`/`session_read` see only m8s lanes.
- To continue bridge work under m8s, `start` a new lane in the same
  source checkout (or a fresh worktree at the recorded commit) with the
  prior result pasted as context; leave the bridge's state directory
  and worktrees in place until reviewed.
- Preserve `/root/.local/state/muse-bridge` until the cutover check
  passes; its fixture worktree stays inspectable.

## Smoke harness (no live tenant writes)

```bash
# stdio smoke against the real framing with a fake daemon socket:
python3 -m unittest discover -s scripts/tests -p 'test_mcp_adapter.py' -v
# live-shape smoke (daemon must be up; uses only list/health/models):
printf '%s\n' \
 '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
 '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
 '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"health","arguments":{}}}' \
 | scripts/m8s-mcp serve
```

The first command is the gate (19 tests, fake socket + temp git repos).
The second is read-only against a live daemon. Neither launches a lane.

## Limitations

- `read_only` is approval-gated, not OS-enforced (see difference 1).
- Status previews and diffs are bounded; large patches exceed preview
  limits — use `changes` plus direct worktree inspection.
- One adapter process per stdio connection; concurrent Codex clients
  share daemon lanes but not adapter task records (task ids are
  per-state-directory).
- Model selection passes through daemon `launch`; unknown models fail
  at the daemon with its own error.
