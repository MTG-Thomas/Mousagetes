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

## MSP wire types

Method, notification, and error tables come from Meta's generated
`muse-code-msp` package (zero runtime dependencies), not from
hand-maintained lists in `scripts/muse-msp.py`. At daemon start the
controller exports the installed `muse` binary's schema bundles
(`muse schema generate-json-schema`, stable plus `--experimental`) and
compares each fingerprint against the bundle constants and records any
drift as a `schema.drift` event: a mismatch is a warning, never an
error (sdk-cookbook `fingerprint-mismatch` posture — additive-optional
evolution means an older bundle keeps working against a newer host).
Only an export that cannot run at all stops startup (`schemaDrift`).

`muse-code-msp` is not on PyPI, so `pyproject.toml` pins it to an SDK
mirror commit (`git+https://github.com/meta-models/muse-code-sdk@<sha>`
with `#subdirectory=python/clients/msp-py`). Install it before running
the controller or the tests:

```bash
pip install .
```

Update procedure: pick a newer mirror commit whose
`python/clients/msp-py` rendering covers the installed `muse` binary
(`muse --version` vs the bundle's `REQUIRED_HOST_VERSION`), re-pin the
`dependencies` URL, run the suite plus `ruff`, and confirm `up` records
no `schema.drift`. Never hand-edit the generated files.

## License

GNU Affero General Public License v3.0 — see [LICENSE](LICENSE).
