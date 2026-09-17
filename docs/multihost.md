<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# Multihost enrollment (Moonshot P3)

MSP (`muse serve`) is stdio-only: the client owns the process's stdin/stdout
and is its only connection. There is no TCP/port/bind option, and m8s adds
none — **SSH (OpenSSH CLI) is the only sanctioned carrier** for a second
host. Each enrolled host runs exactly one m8s agent owning exactly one serve
process; federation is agent-to-agent, never by sharing a host.

## Concepts

- **Host registry** (`hosts.json` in the daemon runtime dir): enrolled hosts
  with SSH target, lane capacity (`maxLanes`), owning `agentId`
  (`hostname:control-socket`), and heartbeat liveness (TTL 120 s,
  `M8S_HOST_TTL_SECONDS` override).
- **Namespaced lane identity**: `alias` addresses this host's lane;
  `host/alias` addresses a peer host's lane. The same alias on two hosts
  never collides.
- **Placement** (`m8s place`): ranks hosts by collision zone (shared branch
  or overlapping files lose), checkout affinity (host already holding the
  checkout wins), and capacity (full or stale hosts are out).
- **Carrier** (`RemoteMspHost`): spawns `ssh target -- muse serve …` and
  supervises it with the same budgets/stuck-detection/call-validation as a
  local host. A peer agent's CLI is reached agent-to-agent via
  `ssh target -- python3 <remote-msp> …`, never by touching its serve stdio.

## Operator steps: enroll a real second host

On the new host, install Muse and copy this repo's `scripts/muse-msp.py`
(or the whole checkout) to a known path, e.g. `/opt/m8s/muse-msp.py`, then
start its agent (its own daemon, its own runtime dir):

```bash
ssh peer 'python3 /opt/m8s/muse-msp.py up'
```

On the enrolling host (passwordless SSH required — enroll uses
`BatchMode=yes` and fails fast otherwise):

```bash
# 1. key access (once)
ssh-copy-id peer

# 2. enroll (probes `command -v muse && muse --version` over SSH first)
scripts/m8s host enroll --name peer --target peer --max-lanes 4 \
  --remote-msp /opt/m8s/muse-msp.py

# 3. verify
scripts/m8s host list
scripts/m8s host heartbeat --name peer
scripts/m8s place --branch lane/x --checkout /repo/x --files a,b
```

Keep it alive with a periodic heartbeat (cron/systemd timer), e.g.
`scripts/m8s host heartbeat --name peer`. A host whose heartbeat is older
than the TTL reads `live: false` and is excluded from placement; a host
owned by another live agent is refused with `hostOwned` — federate
agent-to-agent instead of sharing it.

## Verified without a second machine

Loopback (`ssh localhost`) is a real SSH enrollment target and was verified
live: probe, enroll, heartbeat, namespaced identity, placement ranking, and
a full `muse serve` initialize handshake carried over `ssh` stdio all pass
against an isolated runtime dir (`XDG_RUNTIME_DIR=/tmp/m8s-p3-live-rt`),
leaving the shared controller daemon untouched. The unit suite
(`python3 -m unittest discover -s scripts/tests`) covers registry,
carrier argv (including a no-TCP-listener guard), identity, and placement
with fake transports.
