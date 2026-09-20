<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# Remote ACP over the Defined Networking mesh

This is the Phase 2 transport runbook from [docs/acp.md](acp.md)
(workstream **WS-C "Transport & access"**). It puts an ACP agent behind a
websocket bridge and publishes it to a phone over the **Defined
Networking (Nebula) mesh** with TLS, so a browser PWA or a desktop ACP
client can drive a lane with `wss://`.

The mesh here is Defined Networking's `dnclient` (the Nebula-based agent),
**not Tailscale**. `dnclient` carries encrypted peer-to-peer traffic and
enforces a per-host firewall, but it does **not** provide a `serve`-style
TLS terminator or a mesh DNS name. TLS is therefore terminated locally by
a reverse proxy bound to the mesh address, using an internal CA the phone
trusts. See [ADR 0005](adr/0005-stdlib-only-external-ws-bridge.md)
(external bridge, stdlib-only Python),
[ADR 0007](adr/0007-ios-pwa-private-mesh.md) (iOS PWA over the mesh with
TLS; a local reverse proxy with an internal certificate is its stated
fallback), and [ADR 0003](adr/0003-remote-client-capability-boundary.md)
(no filesystem or terminal to remote clients).

```
  iPhone / laptop browser            ACP client (desktop)
        |  wss://100.100.0.3/   (or ws://100.100.0.3:3002)
        v
  Defined Networking mesh (dnclient / Nebula, defined1)   [ADR 0007]
        |  encrypted WireGuard-style tunnel + per-host firewall
        v
  local TLS proxy  (socat/openssl, internal CA, bind 100.100.0.3:443)
        |  http://127.0.0.1:3002
        v
  @rebornix/stdio-to-ws  (websocket -> child stdin/stdout)  [ADR 0005]
        |  ACP JSON-RPC over stdio, newline framed
        v
  scripts/m8s-acp stub   (WS-A replaces with daemon-backed adapter)
```

The bridge owns no protocol logic: it spawns the agent as a child and
forwards each websocket frame to the child's stdin and each stdout chunk
back to the socket. All ACP framing lives in the Python process.

## Prerequisites

- A checkout of this repo and Python 3 (`scripts/m8s-acp` is stdlib-only;
  verified with Python 3.13.5). Run commands from the repo root so the
  relative `scripts/m8s-acp` path resolves.
- Node.js and `npx` for the external bridge (verified with Node v22.23.2 /
  npm 10.9.8). The bridge is pinned below to `@rebornix/stdio-to-ws@0.2.0`.
- **The Defined Networking client (`dnclient`) installed, enrolled, and
  running** on the host. Confirm with:

  ```sh
  systemctl status dnclient
  /opt/defined-networking/dnclient info
  ip -4 -brief addr show defined1
  ```

  The mesh interface is `defined1`; this host (`pve-t340`) has mesh
  address **`100.100.0.3/22`**. `dnclient` has no DNS entry for the mesh,
  so clients use the IP. Substitute your own address.
- **A Defined Networking inbound firewall rule for the TLS port.** The
  mesh firewall is default-deny (`default_local_cidr_any: false`), so a
  port is unreachable from peers until it is allowed. This host currently
  allows only:

  | Port | Proto | Source group | Purpose |
  | --- | --- | --- | --- |
  | 22 | tcp | `t:needs:infra-routes` | SSH |
  | icmp | icmp | `t:needs:infra-routes` | ping |
  | 8006 | any | `role:Operator Device` | Proxmox web UI |
  | 3000 | any | `role:Operator Device` | OpenChamber |

  Add an inbound rule for **443/tcp from `role:Operator Device`** (or your
  phone's device group) in the Defined Networking console before expecting
  a phone to connect. This is configured centrally by the network, not on
  the host, and is the one step that cannot be done from this repo.
- `socat` and `openssl` for the TLS terminator and internal certificate
  (both present on this host).
- The bridge binds **all interfaces** (`0.0.0.0`/`::`) on its port — see
  [Security](#security-notes). Keep its port out of the mesh firewall and
  off any untrusted interface.

## Start, stop, disable

All commands assume the repo root. `PORT` defaults to `3000`; keep it
distinct per workstream (WS-E uses `3001`). In this environment `3000` is
held by OpenChamber, so the verified run used `3002`.

### 1. Start the websocket bridge

Foreground (useful for logs while testing):

```sh
npx --yes @rebornix/stdio-to-ws@0.2.0 "scripts/m8s-acp stub" \
  --port 3002 --persist --grace-period -1
```

Detached (leave it running on the host). A transient systemd unit is the
cleanest way to supervise it:

```sh
systemd-run --unit=m8s-acp-bridge --collect \
  --working-directory="$PWD" \
  --setenv=PATH=/usr/bin:/bin:/usr/local/bin --setenv=HOME=/root \
  /usr/bin/npx --yes @rebornix/stdio-to-ws@0.2.0 "scripts/m8s-acp stub" \
  --port 3002 --persist --grace-period -1
```

- `--persist` keeps the agent child alive across websocket disconnects so
  the phone can reconnect.
- `--grace-period -1` never reaps that child. A finite value (for example
  `--grace-period 60`) frees it after 60s of no client.
- The command string is parsed by `string-argv`, so quote the path if it
  contains spaces.

Confirm it is listening:

```sh
ss -ltnp | grep ':3002'
```

### 2. Terminate TLS on the mesh address

`dnclient` does not terminate TLS, so run a loopback-to-mesh reverse proxy
with an internal CA. The certificate's SAN must be the **mesh IP**, since
there is no DNS name.

One-time internal CA and server certificate:

```sh
mkdir -p /etc/m8s-acp/tls && cd /etc/m8s-acp/tls

# Internal CA (install ca-cert.pem on the phone once; see below):
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout ca-key.pem -out ca-cert.pem -days 825 \
  -subj "/CN=m8s internal CA"

# Server key + CSR + leaf certificate with the mesh IP as SAN:
openssl req -newkey rsa:2048 -nodes \
  -keyout srv-key.pem -out srv.csr -subj "/CN=100.100.0.3"
printf 'subjectAltName=IP:100.100.0.3\n' > san.ext
openssl x509 -req -in srv.csr -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out srv-cert.pem -days 397 -extfile san.ext

chmod 600 ca-key.pem srv-key.pem
```

Run the TLS terminator, bound to the mesh address on 443:

```sh
systemd-run --unit=m8s-acp-tls --collect \
  /usr/bin/socat \
  OPENSSL-LISTEN:443,bind=100.100.0.3,reuseaddr,fork,verify=0,cert=/etc/m8s-acp/tls/srv-cert.pem,key=/etc/m8s-acp/tls/srv-key.pem \
  TCP:127.0.0.1:3002
```

Verify the chain and the endpoint from the host:

```sh
openssl s_client -connect 100.100.0.3:443 -CAfile /etc/m8s-acp/tls/ca-cert.pem \
  -verify_return_error </dev/null 2>&1 | grep 'Verify return code'
# expected: Verify return code: 0 (ok)
```

The phone and other mesh devices can now reach the agent at
`wss://100.100.0.3/` (443 implied) once the Defined Networking firewall
allows 443 from their group.

### 3. Stop or disable

```sh
systemctl stop m8s-acp-tls.service m8s-acp-bridge.service
systemctl reset-failed m8s-acp-tls.service m8s-acp-bridge.service 2>/dev/null || true

# Or, if started as plain background processes, match the port:
pkill -f 'stdio-to-ws.*--port 3002'
ss -ltnp | grep -E ':3002|:443'   # should print nothing for these
```

Disabling remote access permanently means stopping the two units above
**and** removing the 443 inbound rule from the Defined Networking policy.

## Add the remote agent in an ACP client

Client UIs differ, but the fields are the same. Add a custom/remote agent
with:

- **Transport:** WebSocket
- **URL:** `wss://100.100.0.3/` (or `wss://100.100.0.3:443/`)
- **Authentication:** none in this first cut (see
  [Security](#security-notes)); leave headers/token empty.

For a desktop client on the host itself, point it at the bridge directly
with `ws://127.0.0.1:3002` (no TLS needed on loopback).

A client that can speak plain `ws://` and does not need a browser-trusted
certificate can also use `ws://100.100.0.3:3002` directly over the
encrypted mesh — provided the Defined Networking firewall allows 3002 from
its group. This skips the TLS proxy but is not usable from an HTTPS page.

On iOS, trust the internal CA before opening the client: transfer
`ca-cert.pem` to the phone, install the profile (Settings -> General ->
VPN & Device Management), then enable it under Settings -> General ->
About -> **Certificate Trust Settings**. Then open the ACP web client in
Safari, confirm it connects, and use Share -> **Add to Home Screen**
([ADR 0007](adr/0007-ios-pwa-private-mesh.md)). The page must be served
over HTTPS and must connect with `wss://`; Safari blocks a `ws://` socket
opened from an HTTPS page (mixed content).

## Security notes

- **The mesh plus its firewall is the boundary.** There is no bearer token
  yet: the Phase 2 token task (open question 4 in [docs/acp.md](acp.md))
  has not landed. Until it does, any device allowed through the Defined
  Networking firewall can steer sessions and answer approvals. Keep the
  inbound rule scoped to the operator device group and nothing wider.
- **The bridge listens on all interfaces.** `stdio-to-ws` creates its
  server with only a port (no host), so it binds `0.0.0.0`/`::`, not just
  loopback. The TLS proxy binds only `100.100.0.3`, and the Defined
  Networking firewall default-denies the bridge port from peers, but the
  port is still reachable on the host's other interfaces (LAN, containers).
  Add a host firewall rule allowing the bridge port only from loopback, or
  accept that the mesh firewall is the only gate.
- **Nebula encrypts transport, TLS authenticates the endpoint.** The
  tunnel is already encrypted peer-to-peer, but iOS requires a
  browser-trusted `wss://`; the internal CA provides that. Treat
  `ca-key.pem` as a secret and keep it off the phone (install only
  `ca-cert.pem`).
- **No filesystem or terminal to clients** ([ADR 0003](adr/0003-remote-client-capability-boundary.md)).
  The stub advertises no `fs/*` and no `terminal/*` capabilities and never
  issues those requests; the real adapter must keep that posture.
- **Never log the token.** When bearer-token auth is added, keep it out of
  `events.ndjson` and the bridge log, exactly as the daemon control socket
  stays `0600`.
- The agent's own sandbox is unchanged: remote clients get prompts,
  approvals, controls, and rendered output only.

## Troubleshooting

- **Phone cannot connect, host can.** The Defined Networking firewall is
  default-deny. Confirm the 443 inbound rule exists for the phone's device
  group (`/opt/defined-networking/dnclient info` shows the applied
  `config.firewall.inbound` list; it is pushed from the console).
- **HTTPS page cannot open `ws://`.** Browsers block mixed content. Always
  use `wss://` from an HTTPS page; use `ws://127.0.0.1:3002` only for
  desktop clients running on the host.
- **Certificate errors on iOS.** The leaf must carry `IP:100.100.0.3` in
  its SAN (a `CN` alone is ignored), and the internal CA must be installed
  *and* enabled under Certificate Trust Settings. Re-check with
  `openssl s_client ... -verify_return_error`.
- **`--persist` keeps the session after a disconnect.** With
  `--grace-period -1` the agent child stays alive. To reattach to the
  *same* child, a client must reconnect with the same `X-Client-Id`
  header; the bridge replies with `{"type":"reconnect","clientId":...}`
  and flushes any output buffered while the socket was down. A client that
  reconnects without that header gets a **new** child.
- **One active connection at a time in the first cut.** The bridge itself
  will accept concurrent sockets and spawn a separate child per distinct
  client, but the first-cut policy (and the daemon-backed adapter) assumes
  a single active controller. Two clients can race on the same lane's
  approvals; treat it as single-active until Phase 3 decides the
  multi-device model.
- **`EADDRINUSE` on start.** The bridge binds all interfaces, so any other
  listener on that port blocks it — including a service bound to one
  address (for example OpenChamber on `100.100.0.3:3000`). Choose another
  port and update the TLS proxy target to match.
- **The client connects but never gets a reply.** ACP stdio is
  newline-delimited. A client that writes bare JSON without a trailing
  newline will leave the Python reader waiting. Each JSON-RPC message must
  end with `\n`.

## What was verified, and what was not

Verified in the WS-C environment on 2026-09-19 (Defined Networking client
0.9.8 / Nebula 1.11.1, mesh `defined1` = `100.100.0.3/22`):

- **stdio exchange.** Piping `initialize`, `session/new`, and
  `session/prompt` into `python3 scripts/m8s-acp stub` returned the
  expected results plus a streamed `session/update`
  (`"stub received: ..."`) and `{"stopReason":"end_turn"}`.
- **Bridge on the mesh address.** With the bridge on port 3002, an ACP
  client completed `initialize` -> `session/new` -> `session/prompt` over
  **`ws://100.100.0.3:3002`**, including the server-initiated
  `session/request_permission` round trip (client answered `allow-once`,
  stub emitted the completed tool-call update). Four `session/update`
  notifications across two prompts.
- **TLS on the mesh address.** A `socat` terminator bound to
  `100.100.0.3:443` with an internal CA, and
  `openssl s_client -verify_return_error` returned
  `Verify return code: 0 (ok)`. A client then completed a prompt over
  **`wss://100.100.0.3/`**. (The same was also verified on port 3443 with a
  self-signed cert before the CA flow.)
- **`--persist` reconnect.** A second connection carrying the same
  `X-Client-Id` received `{"type":"reconnect",...}` and reused the same
  child (the session counter continued from `stub-0001` to `stub-0002`).
- `python3 -m unittest discover -s scripts/tests` (282 tests) and
  `ruff check scripts/ --select F,E9` both pass.

**Port deviation.** The runbook's default bridge port is 3000, but in the
WS-C environment port 3000 was already bound by the OpenChamber web UI
(`@openchamber/web ... --port 3000 --host 100.100.0.3`) and 3001 is WS-E's.
Rather than disturb another process, verification ran on **3002**. The
commands above are parameterized by port; substitute `3002` throughout to
reproduce exactly what was tested.

Not verified, and why:

- **Remote reachability from another mesh device (the phone).** All tests
  above were host-local to `100.100.0.3`, which bypasses the Defined
  Networking firewall. The current applied firewall allows only 22, icmp,
  8006, and 3000; **no 443 or 3002 rule exists yet**, so a phone will be
  blocked until the inbound rule is added in the Defined Networking
  console. This is the one external action the runbook depends on.
- **iOS Safari and Add to Home Screen.** Requires a physical iPhone on the
  mesh; out of reach in this environment.
- **The real daemon-backed adapter.** Only the scaffold stub exists so far
  (WS-A owns the adapter). The transport is agent-agnostic and unchanged by
  that swap.
- **Bearer-token authentication.** Deferred to a later phase; the mesh
  firewall is the only access control today.
- **Concurrent multi-client behavior under the real adapter.** Only the
  bridge's per-connection child behavior was observed with the stub.

## References

- Phase 2 plan: [docs/acp.md](acp.md#phase-2--remote-transport-for-the-phone)
- [ADR 0003 — no fs/terminal to remote clients](adr/0003-remote-client-capability-boundary.md)
- [ADR 0005 — stdlib-only, external websocket bridge](adr/0005-stdlib-only-external-ws-bridge.md)
- [ADR 0007 — iOS PWA over the private mesh with TLS](adr/0007-ios-pwa-private-mesh.md)
- Bridge: `@rebornix/stdio-to-ws` (pinned `0.2.0`)
- Defined Networking: <https://defined.net/>
- ACP transports: <https://agentclientprotocol.com/>
