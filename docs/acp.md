<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# ACP client surface (m8s v2)

Muse is the brain, m8s is the supervisor, and neither has a body. m8s v2
gives the swarm a body by speaking the **Agent Client Protocol (ACP)** —
the open, Apache-2.0 standard (originally from Zed) that agent clients
already speak. Any ACP client then drives m8s-owned lanes: desktop editors
on the host, and a browser on a phone.

This document is the Phase 0 deliverable (the frozen mapping) plus the plan
for Phases 1-4. The decisions behind it are codified in
[docs/adr/](adr/README.md).

## One idea

Teach m8s to speak one plug standard, and let existing apps be the screen.
Do not build a web framework, do not fork an editor, and do not emulate
another agent's private API. ACP is to agent UIs what LSP is to editors.

## Non-goals

- **No OpenCode-API emulation.** OpenChamber is a fine app, but emulating
  OpenCode's HTTP+SSE surface (60+ routes and a fine-grained event
  taxonomy) is the most expensive option with the least reuse.
- **No Codex app-server emulation.** Open and codegen-friendly, but it is
  Codex's own harness contract (~130 methods), not an interop standard.
- **No new daemon state.** The adapter translates; the daemon stays the
  only source of truth.
- **No filesystem or terminal proxying to remote clients.** See
  [ADR 0003](adr/0003-remote-client-capability-boundary.md).
- **No Python dependency.** WebSocket exposure rides an external bridge;
  see [ADR 0005](adr/0005-stdlib-only-external-ws-bridge.md).

## Architecture

```
  phone / tablet / browser / editor
        |  ACP (stdio locally, wss:// remotely)
  m8s acp adapter                 <- new: stateless translator
        |  unix socket: the existing daemon control API
  m8s daemon  --stdio(MSP)-->  muse serve  -->  lanes
   (roster, budgets, bus, board, health)
        |
  m8s web (v1)  ->  swarm / board view (unchanged)
```

Two invariants:

1. **The daemon remains the only process that talks to `muse serve`**
   ([ADR 0002](adr/0002-daemon-authority.md)). The adapter never spawns a
   serve host, so there is no writer-lease conflict.
2. **The adapter is stateless**; all lane state lives in the daemon and its
   file-backed records (`events.ndjson`, roster, claims).

## Translation model

### One lane = one ACP session

Each m8s lane maps to exactly one ACP session whose id is the lane's
stable id. `session/new` launches a lane; `session/load` resumes one; the
lane worktree is the session working directory. A connection may host many
sessions. See [ADR 0006](adr/0006-lane-session-identity.md).

### Action mapping

| ACP message | m8s call |
| --- | --- |
| (session list) | `list`, then per-lane roster |
| `session/new` (cwd) | `launch` |
| `session/load` | `resume-session`, then `read` / `view/page` |
| `session/prompt` | `send`; on a live turn, `turn --steer` |
| `session/cancel` | `turn --cancel` / `--interrupt` |
| `session/update` (agent message, thought, tool call, plan) | `watch` events + `view/page` + `item/readOutput` |
| `session/request_permission` | `pending` approval -> `approve` |
| `elicitation/create` | pending user input -> `clarify` |
| `session/set_mode` / config option / slash command | `set-approval-mode`, `set-model`, `set-effort` |
| slash commands `/goal /fork /compact /retire` | `goal`, `fork`, `compact`, `retire` |

### Controls: slash-command first, picker second

ACP v1 standardizes session modes but not model or reasoning effort, and
client picker support varies. Every control is therefore reachable as a
slash command intercepted by the adapter, and *additionally* advertised as
a mode or config option where the client supports it. See
[ADR 0004](adr/0004-slash-first-controls.md).

Command set: `/model <id>`, `/effort <level>`, `/approval <mode>`,
`/goal <text>`, `/fork`, `/compact`, `/retire`, `/budget`.

### Capability advertisement

Advertise `fs.readTextFile: false`, `fs.writeTextFile: false`, and no
terminal capability. Never issue `fs/*` or `terminal/*` to the client.
Muse edits in its own sandbox and runs its own shell, so the client never
needs host filesystem access. See
[ADR 0003](adr/0003-remote-client-capability-boundary.md).

## The `m8s acp` command

```
m8s acp [--transport stdio|unix] [--socket PATH]
        [--workspace-root DIR] [--token-file FILE]
```

- **stdin/stdout** carries ACP JSON-RPC 2.0, newline-delimited (the
  standard ACP stdio transport). Editors and the websocket bridge use it.
- `--transport unix` binds a local Unix socket instead.
- The adapter connects to the running daemon via its control socket and
  refuses to start if the daemon is down (or can `up` it on request).
- The adapter never writes to MSP directly and never spawns `muse serve`.

## Phase plans

### Phase 0 — Freeze the mapping

**Goal.** A signed-off spec and recorded fixtures, no behavior.

**Deliverables.**
- This document plus the [ADRs](adr/README.md) accepted.
- Recorded MSP transcript fixtures (see Phase 4 test harness) covering:
  a turn's event stream, an approval request, a user-input request, and
  a model/effort/approval-mode change.

**Tasks.**
- Confirm update granularity: does `watch`/`view` emit token-level deltas,
  or whole messages? Record the answer; it sets expectation for streaming.
- Confirm approval and user-input payload shapes and the reply verbs.
- Confirm the model and reasoning-effort value sets.
- Confirm lane id stability across `resume-session` and daemon restart.
- Choose the remote token mechanism (see Phase 2).

**Exit criteria.** ADRs accepted; fixtures checked in; granularity answer
written down.

**Risks.** Coarse streaming (chunky replies rather than typewriter) is
cosmetic but must be known before promising "OpenChamber-like".

### Phase 1 — stdio adapter with all controls

**Goal.** Drive a real lane end-to-end from a desktop ACP client.

**Deliverables.**
- `m8s acp` stdio transport: JSON-RPC framing, initialize, session
  registry, event pump, reconnect-safe teardown.
- Translation for `list`, `launch`, `read`, `view`, `send`, `turn`,
  `pending`, `approve`, `clarify`, `resume-session`.
- Slash commands `/model`, `/effort`, `/approval`, `/goal`, `/fork`,
  `/compact`, `/retire`, `/budget`.
- `modes`/`models` on `session/new` and `session/load`, plus
  `session/set_mode` and `session/set_model` (R3, R4). The reference
  client renders pickers only from these; slash commands remain the
  compatibility floor (ADR 0004).

**Tasks.**
- JSON-RPC 2.0 read/write loop with newline framing and request ids.
- Session map: ACP session id -> lane id -> daemon session handle.
- Re-read the lane's materialized view from the prompt turn's start
  cursor (the view inserts and revises records, so a naive forward cursor
  misses both), then fan that turn's records into ordered, deduplicated
  `session/update` notifications. Scope terminals and updates to the
  submitted `turnId`; a queued `send` returns a command id, not a turn
  id, so adopt the turn once its records appear.
- Bridge `pending` approvals/inputs to permission and elicitation
  requests; serialize decisions back through `approve`/`clarify`.
- Handle `overBudget` and `stuck` as typed ACP errors/messages.

**Exit criteria.** A scripted ACP client (test fixture) and a real desktop
client both complete: open lane, read history, send prompt, observe
streamed output and a tool call, answer an approval, change model and
approval mode.

**Risks.** Approval races (double answer); session-mapping drift across
daemon bounce; event ordering.

### Phase 2 — Remote transport for the phone

**Goal.** Steer a lane from an iPhone over the private mesh.

**Deliverables.**
- A websocket endpoint in front of `m8s acp` via an external bridge
  (`stdio-to-ws`).
- Bearer-token authentication enforced by m8s, not the bridge
  ([ADR 0009](adr/0009-token-enforcement.md)).
- TLS termination on the mesh address. The mesh is **Defined
  Networking** (`dnclient`, Nebula); it encrypts transport but does not
  terminate TLS, so a local reverse proxy bound to the mesh IP with an
  internal CA provides `wss://` — see [docs/acp-remote.md](acp-remote.md).
- A setup runbook (`docs/acp-remote.md`).
- Self-host the ACP web client on the mesh HTTPS origin to clear the
  browser Private Network Access and mixed-content barriers
  ([ADR 0011](adr/0011-self-host-client.md)).

**Tasks.**
- Enforce a bearer token in the adapter or a fronting proxy (R12): the
  `stdio-to-ws` bridge accepts any subprotocol, so it is a byte pipe.
- Add the Defined Networking inbound firewall rule for the TLS port.
  Host-local tests bypassed the firewall; the phone will not.
- Verify the HTTPS-page/`wss://` rule and Add-to-Home-Screen behavior on
  iOS Safari.
- Confirm reconnect and `session/load` resume after the phone sleeps.

**Exit criteria.** From the iPhone: open the app, read a lane it created,
send a prompt, answer an approval, and change a mode/model.

**Risks.** Mixed content (must be `wss://`); token propagation via
websocket subprotocol; the mesh firewall must allow the TLS port; one
active connection at a time in the first cut; no lane roster on the stock
client ([ADR 0010](adr/0010-roster-client-local.md)).

### Phase 3 — Robustness / daily driver

**Goal.** Reliable unattended use.

**Deliverables.**
- Reconnect with backoff; session resume after daemon restart.
- Multi-device policy: single-active initially, or daemon fan-out (decide
  in an ADR if we change models).
- Approval dedupe and expiry messaging.
- Richer tool/diff rendering from `item/readOutput`.
- Budget/stuck/health surfaced in session metadata and slash commands.

**Exit criteria.** Survives a daemon bounce, a network flap, and phone
sleep without corrupting lane state or losing an unanswered approval.

**Risks.** Event replay gaps after reconnect; duplicate decisions.

### Phase 4 — Optional branded PWA / merged console

**Goal.** One installable app with swarm and session together.

**Deliverables.** An m8s PWA (manifest + service worker) that merges the
`m8s web` v1 swarm view with session interaction, either by extending the
ACP web client or by building a thin client (possibly on AG-UI). Requires
a new ADR before work starts.

**Exit criteria.** Installed to the iOS Home Screen, offline shell, board
and chat in one surface.

**Risks.** Scope and framework churn. Explicitly deferred; Phases 1-3 do
not depend on it.

## Adapter requirements (from client evaluation)

Measured against the real `acp-ui` client; evidence and detail in
[docs/acp-client-eval.md](acp-client-eval.md). `P0` blocks a usable
session, `P1` is fidelity, `P2` is deployment/security. "WS-A/B" means
the transport or mapping workstream.

| Req | P | Requirement | Owner |
| --- | --- | --- | --- |
| R1 | P0 | One newline-terminated JSON object per frame; never two per frame | WS-A |
| R2 | P0 | Adapter is stateless and daemon-backed | WS-A |
| R3 | P0 | `session/new` and `session/load` return `modes` and `models` | WS-B |
| R4 | P0 | Implement `session/set_mode` and `session/set_model` | WS-B |
| R5 | P0 | Permission requests carry `toolCall` title/kind/locations and options; dedupe answers | WS-B |
| R6 | P0 | `initialize` shape: `loadSession:true`, `authMethods:[]`, no `fs`/`terminal` | WS-A |
| R7 | P1 | Replay history before the `session/load` response | WS-A |
| R8 | P1 | Put observable results in tool `title`/`status`; content is ignored | WS-B |
| R9 | P1 | Emit `available_commands_update` for the slash commands | WS-B |
| R10 | P1 | `session/cancel` mid-turn and `stopReason` on `session/prompt` | WS-A |
| R11 | P1 | Send only renderable update kinds | WS-A |
| R12 | P0 | Enforce the bearer token outside `stdio-to-ws` | WS-C |
| R13 | P0 | Serve the client from the mesh HTTPS origin (PNA/mixed content) | WS-C |
| R14 | P1 | No `session/list`; roster is client-local or a fork decision | WS-C |
| R15 | P2 | Re-evaluate bridge flags; strip client telemetry | WS-C |
| R16 | P2 | Tolerate the bridge `{"type":"connected"}` control frame | WS-A |

## Cross-cutting concerns

- **Auth.** Local stdio needs none; remote needs a bearer token, and the
  mesh provides network identity. The daemon control socket stays `0600`.
- **Observability.** Reuse `events.ndjson`; add `acp.*` records
  (connect, disconnect, session map, decision relayed) so `m8s events`
  shows adapter activity with no new plumbing.
- **Security.** No fs/terminal to clients; no public listener; MSP stays
  stdio-only; the token is never logged.
- **Failure posture.** Adapter death must not affect lanes; the daemon
  keeps running and the client reconnects.

## Test strategy

- `scripts/tests/test_acp.py`: framing and the transport stub.
- `scripts/tests/test_acp_agent.py`: transport against a fake mapping.
- `scripts/tests/test_acp_mapping.py`: mapping against a fake daemon.
- `scripts/tests/test_acp_integration.py`: the real transport plus the real
  mapping against a fake daemon (no model turns).
- **Replay harness**: feed recorded daemon `watch`/`events` records into
  the translator and assert the emitted ACP notifications, in order.
- **Conformance**: manual runs against a desktop ACP client, then iOS.
- **CI**: existing `ruff check --select F,E9` and
  `python3 -m unittest discover -s scripts/tests`.

## Workstreams and parallelization

These are candidate lanes for the swarm. Each is chosen so its write
surface does not overlap another's.

| Workstream | Deliverable | Depends on | Write surface |
| --- | --- | --- | --- |
| WS-A Protocol core | JSON-RPC framing, session registry, event pump skeleton | Phase 0 | new adapter module |
| WS-B Command mapping | per-verb translation and slash commands | WS-A interface | adapter mapping module + `test_acp.py` |
| WS-C Transport & access | websocket bridge, token enforcement, mesh TLS proxy, runbook | WS-A stdio | `scripts/`, `docs/acp-remote.md` |
| WS-D Fixtures & harness | recorded MSP transcripts, fake ACP client | Phase 0 | `scripts/tests/fixtures/` |
| WS-E Client evaluation | self-host web client, iOS Home Screen, fidelity report | WS-C | `docs/`, packaging |
| WS-F Docs & ADRs | this plan, ADRs, README links | none | `docs/` |

**Parallel start.** WS-D and WS-F need nothing and can run alongside
Phase 0. Once Phase 0 freezes the mapping and WS-A freezes its interface,
WS-B can fan out per verb and WS-C can proceed against the stdio contract.
WS-E waits on WS-C.

**Collision note.** `scripts/muse-msp.py` is a single 5,900-line file and
is therefore a collision zone. The adapter is a **separate package**
(`scripts/m8s_acp/`) so WS-A/WS-B/WS-C can proceed in parallel without
editing one file — see [ADR 0008](adr/0008-adapter-module-boundary.md).

**Scaffold available now.** `scripts/m8s_acp/` ships a JSON-RPC framing
module and a minimal ACP **stub agent** (`scripts/m8s-acp stub`) that
implements initialize, session/new, session/load, session/prompt with
streamed updates, and a server-initiated permission request. It exists so
WS-C (transport/token/mesh TLS proxy) and WS-E (client evaluation, iOS
Home Screen) can be exercised end-to-end before the real adapter lands.
The stub is not the adapter; WS-A replaces its session handling with
daemon-backed mapping.

## Open questions

1. **Module layout — resolved.** The adapter lives in `scripts/m8s_acp/`
   with a `scripts/m8s-acp` entrypoint; the monolith refactor is tracked
   separately. See [ADR 0008](adr/0008-adapter-module-boundary.md).
2. **Streaming fidelity.** Token-level or message-level updates? Affects
   only polish, decided in Phase 0.
3. **Multi-device.** Single active client first, or daemon fan-out now?
4. **Token mechanism — resolved.** A static bearer file enforced by m8s
   ([ADR 0009](adr/0009-token-enforcement.md)); per-device pairing is a
   Phase 3+ upgrade.
5. **Client choice — resolved.** Adopt and fork the ACP web client
   ([ADR 0012](adr/0012-fork-client.md)); Phases 1-3 use the stock build.
6. **Lane creation without an initial prompt.** ACP `session/new` carries
   no prompt, but m8s `launch` takes one; confirm the mapping in Phase 1.

## References

- ACP overview and transports: <https://agentclientprotocol.com/>
- ACP clients and registry: <https://agentclientprotocol.com/get-started/clients>
- Remote transport runbook: [docs/acp-remote.md](acp-remote.md)
- Client fidelity evaluation and R1-R16: [docs/acp-client-eval.md](acp-client-eval.md)
- Decision records: [docs/adr/](adr/README.md)
- Muse/MSP supervision: [SKILL.md](../SKILL.md), [docs/lanes.md](lanes.md)
- Networking (peer federation): [docs/multihost.md](multihost.md)
