---
name: muse-remote-supervisor
description: Discover, message, coordinate, and periodically supervise live Muse coding sessions on a local or remote host through Muse MSP and external-agent ingress. Use for remote session control, multi-session coordination, idle detection, unattended prompts, and CI or merge-queue follow-through.
---

# Muse remote supervisor

Use Muse's supported interfaces to inspect and steer sessions. Do not inject keystrokes into PTYs or write directly to Muse session databases, journals, registries, or sockets.

## Pick the interface

- Use `muse session-message` to discover and send text to already-running interactive sessions.
- Use `muse serve` when building an MSP client that owns a host process over stdin/stdout. It is JSON-RPC and supports session, turn, view, approval, goal, and user-input methods.
- For new unattended sessions, prefer the bundled `scripts/muse-msp.py` controller. It owns one `muse serve` host, receives live MSP events, and exposes a local Unix-socket command interface.
- Use persisted session logs and ordinary process or repository inspection for passive diagnosis when live ingress is unavailable.

`muse serve` is not an attachment shortcut for a TUI that already owns a session. Expect writer leases or `sessionInUse` when another host has it loaded.

## Served sessions

Start the local controller and launch new sessions through it:

```bash
scripts/muse-msp.py up
scripts/muse-msp.py launch \
  --name lane-name \
  --workspace /absolute/path/to/worktree \
  --prompt 'Concrete lane brief'
```

Use `scripts/muse-msp.py list`, `events`, `send`, and `pending` for supervision, and `scripts/muse-msp.py health` for the fused one-screen swarm view (liveness, recent turns, progress; flags are exactly `down`/`stuck`/`blocked`). The controller records operational event summaries under its private runtime directory. It does not copy model reasoning or full transcript output into its event log.

The controller reacts to MSP notifications rather than scanning session logs. Its
`events`/`watch` output includes `session/tokenUsage` events, whose
`promptTokens` and `totalTokens` fields are the protocol's counted-once values;
do not re-derive totals from provider-specific cache counters. The latest
cumulative total is also included as `tokenUsage` in `list` for sessions that
have emitted usage since the controller started. `usage/changed` concerns the
provider subscription window, not a session's token count.

Per-lane budgets (`budget SESSION [--max-tokens N] [--max-context-tokens N]
[--models a,b]`, or `launch` with the same flags) cap cumulative tokens,
context occupancy, and allowed models. `list` shows each lane's `budget`,
`tokenUsage`, `overBudget`, `needsOwnerAction`, and `stuck` flags; breaching
lanes refuse new turns with a typed `overBudget` error until the budget is
raised, and every breach records a `budget.exceeded` event marked
`decisionClass: spend`. Stuck lanes (no transcript/repo activity past
`M8S_STUCK_AFTER_SECONDS`, default 30 minutes, or a failed/cancelled turn with
no owner action) appear as `stuck` in `list` with one `lane.stuck` event each.
The generic `call` passthrough validates the method against the exported MSP
schema before hitting the wire: unknown methods and missing-but-required
`commandId` values fail client-side with a typed `errorKind`.

Treat these as actionable:

- `session/statusChanged` with `approvalPending` or `inputPending`;
- server requests `approval/request` and `userInput/request`, recorded as `blocker` events;
- failed or cancelled `turn/completed` events;
- `view/gap` and `session/viewHealthChanged` events.

The controller acknowledges that it displayed server requests, but never decides approvals or answers user questions automatically. Use the typed MSP commands only after applying the normal authorization rules.

Do not migrate a live TUI-owned session by force. Let it finish, verify its worktree and PR ownership are settled, stop that TUI, then launch its next lane through the served controller. A `muse serve` host cannot take the writer lease from an active TUI.

## Host discovery

Run commands on the host that owns the Muse processes. For another host, use the user's existing remote-execution path, such as SSH, and perform all discovery there. Never assume a username, home directory, Muse installation path, session root, workspace path, or session name.

1. Confirm `muse` is on `PATH` with `command -v muse` and read `muse --version` when useful.
2. Discover live sessions with the supplied helper:

   ```bash
   scripts/muse-remote.sh list
   ```

3. Correlate session IDs with processes and workspaces only when needed. Prefer the product listing over guessing from PIDs or filenames.

The helper enables the caller-side gates:

```text
MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=on
MUSE_EXPERIMENTAL_LOCAL_SESSION_MESSAGING=1
```

These gates must also have been present when each target Muse process started. A successful list does not prove a target accepts messages.

## Send and verify

Sending a message changes a live agent's work. Do it only when the user requested steering, coordination, or intervention.

```bash
printf '%s\n' 'Instruction' | scripts/muse-remote.sh send SESSION_ID_OR_NAME
```

Treat delivery as successful only when the command exits zero and its JSON result reports success. `target_resolved` or `transport_accepted` receipts followed by `external_agent_ingress_closed` are a failed delivery.

`unverified_target_receipt`, `unverified_kernel_peer`, or `causal_metadata_invalid` means the target is open but does not authenticate this standalone shell process as an external agent. Do not retry or spoof sender metadata. Use an authenticated MSP controller when available. If the user authorized restarting or driving the interactive TUI, put the session under a real terminal multiplexer such as `tmux` and submit the bootstrap instruction through that TUI. Confirm the prompt was actually submitted, because bracketed-paste handling may require a second Enter. After bootstrap, prefer authenticated peer messages sent by the Muse sessions themselves.

Do not retry a closed target repeatedly. Report that it must be restarted or resumed with both gates in its environment:

```bash
MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=on \
MUSE_EXPERIMENTAL_LOCAL_SESSION_MESSAGING=1 \
muse resume SESSION_ID_OR_NAME
```

Preserve the target's original safety flags and workspace posture when restarting. Do not terminate or restart an interactive session without authorization when it has uncommitted work or owns an active task. If restart is authorized, verify durable session identity and repository state before and after it.

## Coordination message content

Give agents concrete ownership boundaries and an observable stopping condition. Include relevant repository rules rather than assuming peers share context. For development swarms, state:

- coordinate claims before editing;
- one writer per checkout and one lane per dedicated worktree;
- avoid the shared main checkout;
- identify the claimed issue, branch, worktree, and owner to peers;
- own CI, review threads, conflicts, and merge-queue progress through `MERGED`;
- ask only when missing authority or a material product decision blocks safe progress.

Tailor instructions to existing ownership. Do not tell several sessions to "pick an issue" without a claim protocol.

The controller owns a claim/heartbeat/intent bus for that protocol
(`scripts/muse-msp.py bus ...`, see `docs/bus.md`): claim the branch
before editing (`bus claim --host H --lane X --branch lane/x --checkout
/path`), keep the lease alive with heartbeats (`bus heartbeat --host H`
— the `host heartbeat` action also re-gossips leases automatically),
publish intentions (`bus intent --verb propose-plan --branch lane/x`),
and check `bus list` for live/requeue lease state. One lane per branch,
one writer per checkout; expired leases requeue for reassignment.

## Supervision loop

Use a recurring goal or monitor when the product provides one. Otherwise poll at a proportionate interval while work remains. Avoid tight loops.

At each check:

1. List live sessions and detect disappeared or replaced identities.
2. Read each session's latest committed user and assistant messages, or use MSP view methods when an owned MSP connection is available.
3. Check whether it is actively running, idle after declaring unfinished work, waiting for approval or user input, or blocked on a peer.
4. Inspect relevant repository state, PR checks, unresolved review threads, conflicts, and merge-queue status.
5. Intervene only on a concrete stall. Give the smallest instruction that restores progress.
6. Report material changes to the user. Do not create noisy "still running" updates.

Common stall signals include:

- the latest assistant message ends with a routine question despite standing authority;
- no transcript or repository activity after the session claimed immediate work;
- an open PR has red CI, unresolved threads, or a reported conflict with no owner action;
- a session says it is done while its PR is still open;
- multiple sessions edit one checkout or claim the same lane;
- a TUI awaits approval or structured input that nobody is present to answer.

Do not equate a quiet session with failure when it is waiting on an active CI or merge-queue run. Check the external dependency first.

## MSP client work

Before implementing a client, export the exact schema from the installed binary:

```bash
schema_dir="$(mktemp -d)"
muse schema generate-json-schema --out "$schema_dir" --experimental
```

Use that bundle as the authority for methods and wire shapes. Initialize the connection before other JSON-RPC methods. MSP is stdio-bound unless the installed Muse version documents another transport; do not expose it on a network listener without an authenticated transport and explicit authorization.

## Safety boundaries

- Never print auth files, API keys, environment contents, encrypted reasoning, or raw unredacted exports in status reports.
- Never infer permission to approve dangerous tools, merge red PRs, overwrite dirty worktrees, or terminate sessions.
- Do not edit session registries or spoof the ingress wire protocol. Use the Muse CLI or a schema-conformant MSP client.
- Do not use a bare pseudo-terminal keeper for a TUI that expects terminal-emulator capability replies. Use a real terminal emulator or multiplexer.
- Keep host-specific discoveries in runtime state, not in this skill.
