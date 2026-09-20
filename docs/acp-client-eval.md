<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# ACP client evaluation (WS-E)

Target: iPhone. Reference client: the cross-platform ACP web build at
<https://acp-ui.github.io/> (Vite SPA, `clientInfo.name = "acp-ui"`,
observed version `0.1.15`). Agent under test: the Wave 0 transport stub
`scripts/m8s-acp stub`, reached through the external WebSocket bridge
`@rebornix/stdio-to-ws` on port 3001. Transport, token, and
the mesh TLS proxy are WS-C's workstream and are not changed here.

This is a fidelity report, not an adapter. Every gap below is recorded as
a requirement for WS-A (protocol core), WS-B (command mapping), or WS-C
(transport/access). Nothing under `scripts/` was modified.

## Verdict

The reference web client drives the stub end to end over a WebSocket
bridge: handshake, session creation, streamed `agent_message_chunk`,
server-initiated `session/request_permission` with a real modal and a
typed reply, `tool_call`/`tool_call_update`, `session/load` replay, and
foreground/manual reconnect. Capability advertising is compatible: the
stub sends no `fs`/`terminal`, the web client declares
`fs.readTextFile:false`/`writeTextFile:false`, and no `fs/*` or
`terminal/*` request is ever issued.

Three things block a naive phone deployment and are not protocol bugs:

1. The **public HTTPS client cannot open `ws://localhost`** in stock
   Chrome: Private Network Access fails with
   `net::ERR_BLOCKED_BY_LOCAL_NETWORK_ACCESS_CHECKS`. On iOS the fix is
   not a flag; self-host the bundle on the trusted mesh origin and use
   `wss://` (see ADR 0007).
2. `stdio-to-ws` **does not enforce the bearer token** encoded in the
   `bearer.<token>` WebSocket subprotocol. It opens and initializes for a
   missing, bogus, or absent token. Auth must move to the adapter or a
   fronting proxy.
3. The client has **no session-enumeration RPC** (no `session/list`). The
   "session list is the lane roster" line in `docs/acp.md` is not
   reachable through this client's UI without a fork.

## Method and exact steps

All "verified" rows below were run here on a Linux host with Chromium
(from Playwright, `chromium-1243`) driven headlessly. iOS rows are
instructed from the client source and platform rules, not measured.

Reproduce:

```bash
# 1. Bridge the stub onto a WebSocket (port 3001, never WS-C's 3000).
npx @rebornix/stdio-to-ws "scripts/m8s-acp stub" \
  --port 3001 --persist --grace-period -1

# 2. Open the client and add an agent.
#    URL:  https://acp-ui.github.io/
#    Agent name: Stub
#    Transport: websocket
#    URL: ws://localhost:3001/
#    Headers: Authorization: Bearer <token>
#    (the client rewrites this to the `bearer.<token>` subprotocol)
# 3. Select Stub, set Working Directory (e.g. /tmp/stub-work),
#    click New Session.
```

Chromium automation had to be launched with
`--disable-features=LocalNetworkAccessChecks` for the public origin to be
allowed to reach `ws://localhost:3001`. This is an evaluation harness
workaround only; the self-hosted path below needs no flag.

The self-hosted control was verified by serving the same bundle at
`http://localhost:3009/` (index plus `assets/`) and connecting to
`ws://localhost:3001/` with **no flag**. For the phone, the equivalent is
the mesh HTTPS origin (`https://<mesh-ip>/`) with `wss://` to the same
host; see [docs/acp-remote.md](acp-remote.md).

## Verification matrix

| # | Item | Result | Evidence / observed result |
| --- | --- | --- | --- |
| 1 | `initialize` handshake | works | Verified. Client sent `{"protocolVersion":1,"clientCapabilities":{"fs":{"readTextFile":false,"writeTextFile":false}},"clientInfo":{"name":"acp-ui","title":"ACP UI","version":"0.1.15"}}`; stub replied `protocolVersion:1`, `agentCapabilities`, `agentInfo`, `authMethods:[]`. Client logged `Agent initialized`. |
| 2 | `session/new` | works (transport) / partial (controls) | Verified. Client sent `{"cwd":"/tmp/stub-work","mcpServers":[]}`; stub returned `{"sessionId":"stub-0001"}`. Session opened and appeared in the sidebar. Mode/model pickers stayed absent because the stub returns no `modes`/`models` (see R3). |
| 3 | `session/prompt` + streamed `agent_message_chunk` | works | Verified. Client sent `{"sessionId":"stub-0001","prompt":[{"type":"text","text":"hello from eval"}]}`; stub emitted `session/update` with `update.sessionUpdate="agent_message_chunk"`, `content.type="text"`; client rendered it as the Assistant message and resolved the request with `stopReason:"end_turn"`. Only `content.type=="text"` is rendered. |
| 4 | server-initiated `session/request_permission` appears and is answered | works (modal) / partial (content) | Verified. Stub sent a server request with `toolCall:{toolCallId}` and `options:[{optionId,name,kind}]`. Client showed a "🔐 Permission Required" modal with option buttons "Allow once"/"Reject" and a Cancel. Clicking Allow once returned `{"outcome":{"outcome":"selected","optionId":"allow-once"}}`; stub then emitted a completed `tool_call_update` and the turn ended. The modal rendered the kind as `Other` and an **empty title** because the stub sent only `toolCallId` (see R5). |
| 5 | `tool_call` / `tool_call_update` rendering | partial | Verified. `tool_call` rendered with 🔧, its `title`, and a pending ⏳ marker; `tool_call_update` flipped the status to completed ✓. The client **ignores `tool_call_update.content`, `tool_call.content`, `rawInput`/`rawOutput`** — only `title` and `status` (plus `locations`) update. Rich diffs/output therefore do not render (see R8). |
| 6 | `session/load` replay | works | Verified. After a page reload the client sent `session/load` `{sessionId,cwd,mcpServers:[]}`; the stub replayed `user_message_chunk` then `agent_message_chunk` **before** the `{}` result; the client rendered both replayed messages. |
| 7 | reconnect | partial | Verified. Killing the bridge surfaced `Connection lost: websocket closed (code=1006, reason=unknown)` with a **Reconnect** button; it did **not** auto-reconnect while the tab stayed visible. Clicking Reconnect re-ran `initialize`, then `session/load` for the open session and replayed history. The client also calls its reconnect path on `visibilitychange`/`online` (relevant to phone wake; see matrix row 7 and runbook step 6). |
| 8 | advertised capabilities (`fs`/`terminal` absent) | works | Verified. `initialize` result contained no `fs`/`terminal` keys. The web client advertised `fs.readTextFile`/`writeTextFile` as `false` and issued no `fs/*` or `terminal/*` request (the bundle contains an `fs/*` handler but gates it behind `fsAvailable`, false on web). Matches ADR 0003. |
| 9 | negotiated ACP protocol version | works (v1) | Verified. Client constant `protocolVersion = 1`, stub `PROTOCOL_VERSION = 1`; handshake agreed on `1`. WebSocket subprotocol is `acp.v1`; the bearer token is offered as `bearer.<token>`. |
| 10 | bridge token enforcement | **blocked** | Verified with a raw WebSocket probe: connections with `["acp.v1","bearer.eval-token"]`, with **no** subprotocol, and with only `["bearer.bogus-token"]` all opened and completed `initialize`. `stdio-to-ws` echoes a requested subprotocol but validates nothing. See R12. |
| 11 | public client → `ws://localhost` | **blocked** (stock Chrome) | Verified: `WebSocket connection to 'ws://localhost:3001/' failed: net::ERR_BLOCKED_BY_LOCAL_NETWORK_ACCESS_CHECKS`. Works only with the harness flag or when the page is self-hosted on the same (localhost) origin. See R13. |
| 12 | session enumeration (lane roster) | **blocked** by client | Verified from the bundle: no `session/list`, `listSessions`, or `sessions/list` method exists. The sidebar lists only sessions persisted locally by the client (`Saved Sessions`). See R14. |
| 13 | iPhone Safari / Home Screen | instructed | Not run here (no iOS device). Steps in the iOS section; marked instructed. |

## Protocol details worth pinning

- **Framing.** Over WebSocket the payload must be **newline-terminated
  JSON**, one ACP object per line. The client's WebSocket transport does
  `n = t.endsWith("\n") ? t : t + "\n"`. A raw client that sends JSON
  without a trailing newline leaves the stub blocked on stdin; adding the
  newline made responses flow. The bridge relays this framing to the
  stdio child.
- **Bridge control frame.** On connect, `stdio-to-ws` emits a non-ACP
  `{"type":"connected","clientId":"..."}` frame. The reference client
  ignores it; a stricter ACP server should not emit it.
- **Per-connection process.** With `--persist --grace-period -1`, each new
  WebSocket connection spawned a fresh stub child and `session/new`
  returned `stub-0001` on every connection; stale children accumulated
  (three after three connections). In-process session state does **not**
  survive reconnect. That is fine under the daemon-authority design (all
  lane state lives in the daemon) but must not be assumed away.
- **Controls the client can call.** `session/set_mode`
  (`{sessionId, modeId}`) and the unstable `session/set_model`
  (`{sessionId, modelId}`), driven by the mode/model pickers. It does
  **not** understand `config_option_update` (0 occurrences), so reasoning
  effort cannot be a config option — slash command only (ADR 0004).
- **Commands.** `available_commands_update` is supported and rendered as
  command hints (`name`, `description`, `input.hint`).
- **Session close.** The client never sends `session/close` (0
  occurrences); deleting a saved session is local only. The stub's
  advertised `sessionCapabilities.close` is unused.
- **Auth.** `authMethods: []` is fine. If an agent advertises methods and
  a load/new fails with `authentication required` or `-32000`, the client
  shows an auth dialog and calls `authenticate({methodId})`.

## iOS Safari runbook (instructed, not verified here)

Marked **instructed** because no iPhone was available in this
environment. Only the desktop Chromium and self-hosted rows above are
measured.

1. On the host, run the bridge (port 3001 or another free port) and serve
   the web client from the **same** mesh HTTPS origin so the page and the
   socket share a host. On the Defined Networking mesh this is a local
   reverse proxy bound to the mesh IP with an internal CA; exact commands
   are in [docs/acp-remote.md](acp-remote.md).
2. Serve the client bundle from that origin (do not rely on
   `acp-ui.github.io`): it avoids the PNA block, avoids mixed content,
   and removes the third-party dependency (ADR 0007, ADR 0011).
3. On the iPhone: open `https://<mesh-ip>/` in Safari.
   **Share → Add to Home Screen**, then launch from the Home Screen.
4. Open Settings (⚙) and add a remote agent: transport **websocket**,
   URL **`wss://<mesh-ip>/<path>`**, and an
   `Authorization: Bearer <token>` header. An HTTPS page may only open
   `wss://`; plain `ws://` is blocked (except `localhost`, which is not
   the host on a phone). Verify the token is actually enforced (R12)
   before trusting it.
5. Select the agent, set the working directory, tap New Session, send a
   prompt, answer the permission modal, and change a mode/model if the
   adapter advertises them.
6. Expect suspension when backgrounded; on return the app fires
   `visibilitychange` and attempts a foreground reconnect + `session/load`
   (matrix row 7). Background execution and push are unavailable
   (ADR 0007).

## Prioritized adapter requirements

Priorities: **P0** blocks a usable session; **P1** fidelity/UX;
**P2** deployment/security.

### WS-A / WS-B (adapter)

- **R1 (P0) — Newline framing is part of the transport contract.** Emit
  exactly one newline-terminated JSON object per line/frame; never send
  two objects in one frame. The reference client appends `\n`; the
  adapter must not strip or double it.
- **R2 (P0) — Be stateless and daemon-backed.** Do not hold session state
  in the adapter process. Resolve `session/load` from the daemon, replay
  history, and then reply. The bridge may respawn the agent per
  connection (observed), and ADR 0002/0006 make the daemon authoritative.
- **R3 (P0) — Return `modes` and `models` from `session/new`.** The
  client populates its pickers only if the result contains
  `modes:{currentModeId, availableModes:[{id,name,description}]}` and
  `models:{currentModelId, availableModels:[{modelId,name,description}]}`.
  Return them on `session/load` as well (the picker refresh on resume was
  not observable with the stub). Without these, no mode/model UI exists.
- **R4 (P0) — Implement `session/set_mode` `{sessionId,modeId}` and
  `session/set_model` `{sessionId,modelId}`.** Map to
  `set-approval-mode`/`set-model`/`set-effort`. Do not rely on
  `config_option_update`; the client does not implement it — effort must
  be a slash command.
- **R5 (P0) — Populate the permission request.** Include
  `toolCall.title`, `toolCall.kind`, `toolCall.locations`, and
  `options[].{optionId,name,kind}`. Accept
  `{outcome:{outcome:"selected",optionId}}` and
  `{outcome:{outcome:"cancelled"}}`; dedupe repeated answers. The stub's
  `toolCallId`-only shape renders an empty title.
- **R6 (P0) — Implement `initialize` shape.** Return
  `agentCapabilities.loadSession:true`, `authMethods:[]` (or real
  methods), and omit all `fs`/`terminal` keys. If auth methods are
  advertised, implement `authenticate` (ADR 0003).
- **R7 (P1) — Order replay before the `session/load` response.**
  `session/update` history must precede the load result; the client
  renders what arrives before it flips state.
- **R8 (P1) — Encode observable state in `title`/`status`.** The client
  ignores tool content. Put the human-facing result in tool `title` and
  status; any diff/output that must be seen should be sent as message
  text (ADR 0003 already reports edits as output, not as RPC).
- **R9 (P1) — Emit `available_commands_update`** with
  `availableCommands:[{name,description,input:{hint}}]` for the
  slash-first controls (ADR 0004).
- **R10 (P1) — Implement `session/cancel` and `stopReason`.** Support the
  cancel notification during a turn and return `stopReason` on
  `session/prompt`. Not deterministically observable with the instant
  stub; needs a slow/streaming peer (WS-D fixtures).
- **R11 (P1) — Send only renderable update kinds.** `agent_message_chunk`
  (text), `agent_thought_chunk`, `tool_call`, `tool_call_update`,
  `available_commands_update`, `current_mode_update` render. `plan` and
  `config_option_update` are unhandled and only logged.

### WS-C (transport/access)

- **R12 (P0) — Enforce the bearer token outside `stdio-to-ws`.**
  The bridge accepts any/no token and any subprotocol. Validate the
  `bearer.<token>` subprotocol in the adapter or a fronting proxy, bind
  to the mesh interface only, and never log the token.
- **R13 (P0) — Self-host the client on the mesh HTTPS origin.** The
  public client from `https://` is blocked from `ws://localhost` by
  Private Network Access; the phone needs `wss://` on a page served from
  the same host. Self-hosting (verified on localhost) removes the PNA and
  mixed-content barriers and the third-party dependency (ADR 0007).
- **R14 (P1) — Decide how the lane roster appears.** No `session/list`
  exists; the client can only resume sessions it created and stored
  locally. Either accept client-local session history for Phases 1-3, or
  treat a roster UI as a client fork/extension and record a new decision.
- **R15 (P2) — Re-evaluate bridge flags and telemetry.** `--persist
  --grace-period -1` accumulated agent children per connection in our
  runs; confirm the lifecycle you want. The reference bundle loads Azure
  Application Insights (`dc.services.visualstudio.com`, logs
  `Telemetry initialized`); decide whether to strip telemetry in the
  self-hosted build.
- **R16 (P2) — Tolerate the bridge's control frame.** Do not fail on
  `{"type":"connected",...}` between the socket open and the first ACP
  message.

## Limitations

- iOS was not exercised; all iOS behavior is **instructed**.
- Mode/model selection, `session/cancel`, and `session/close` could not
  be wire-verified because the stub returns no `modes`/`models` and turns
  complete instantly; R3/R4/R10/R11 come from the client bundle, not from
  observed frames.
- The reconnect observation is from a visible desktop tab; phone
  background/foreground behavior is inferred from the client's
  `visibilitychange`/`online` reconnect path.
- Evidence was captured on 2026-09-19 against stub `0.1.0` and client
  `0.1.15`; the client is a hosted third-party build and may change
  without notice, which is itself an argument for self-hosting (R13).
