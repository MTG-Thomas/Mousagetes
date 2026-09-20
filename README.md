# Mousagetes (m8s)

Apollo Mousagetes — leader of the Muses. A thin local supervisor for live
Muse coding sessions over MSP (`muse serve`) and external-agent ingress.

## What it does

- Owns one `muse serve` host and exposes a JSON-lines control API on a Unix socket.
- Discovers, messages, and coordinates served sessions: launch lanes, queue
  prompts, watch events, track approvals and user-input blockers.
- Covers all 51 MSP schema methods: curated supervision commands (`list`,
  `events`, `watch`, `send`, `pending`, `read`, `view`, `goal`, `turn`,
  `workflow`, `subagent`, `task`, …) plus a generic `call` passthrough with
  automatic `commandId` minting.
- Claim/heartbeat/intent bus (`bus claim|release|list|heartbeat|intent|read`):
  versioned `m8s.claims` / `m8s.heartbeat` / `m8s.intent` messages with
  branch-level leases, heartbeat re-gossip, and expiry into reassignment —
  see [docs/bus.md](docs/bus.md).
- Board-to-lane compiler (`board export|plan|reconcile`): file-based board
  snapshot compiled into ranked lane specs with deterministic priority
  arbitration, P4-lease collision queuing, decision-blocked paging, and a
  reconcile loop — see [docs/compile.md](docs/compile.md).
- ACP client surface (planned, m8s v2): drive lanes from ACP clients and
  phones over a private mesh — see [docs/acp.md](docs/acp.md) and
  [docs/adr/](docs/adr/README.md).

## Use

```bash
scripts/muse-msp.py up
scripts/muse-msp.py launch --name lane --workspace /path/to/worktree --prompt 'Brief'
scripts/muse-msp.py list
scripts/muse-msp.py events --limit 50
# or via the short name:
scripts/m8s usage
```

The companion `scripts/muse-remote.sh` covers direct local session messaging
without the daemon.

## Test

```bash
python3 -m unittest discover -s scripts/tests
```

Stdlib only — no dependencies.

## License

GNU Affero General Public License v3.0 — see [LICENSE](LICENSE).
