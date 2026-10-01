# Mousagetes agent guidance

Mousagetes (`m8s`) supervises Muse coding sessions over MSP and exposes CLI, web, and ACP client surfaces. [README.md](README.md) is the entry point; read relevant [architecture decisions](docs/adr/README.md) before changing a protocol or authority boundary.

## Map

`scripts/muse-msp.py` implements the supervisor; `scripts/m8s` is its short launcher. `scripts/m8s_acp/` contains the ACP adapter, mapping, JSON-RPC, and contract code. `scripts/tests/` contains regression and integration coverage. Use [docs/bus.md](docs/bus.md), [docs/compile.md](docs/compile.md), and [docs/multihost.md](docs/multihost.md) for leases, board reconciliation, and host behavior.

## Invariants

The daemon alone owns `muse serve`; adapters use its control API and never attach a second supervisor or launch an independent serve host. Lane state belongs to daemon records. Adapter disconnection must not terminate running lanes. Remote ACP clients advertise no filesystem or terminal capabilities and must never receive `fs/*` or `terminal/*` RPC requests; rendered output is a separate disclosure surface. Preserve lane/session identity, leases, token enforcement, and pending approval/user-input boundaries.

## Checks and releases

Use Python 3.10+; the runtime is standard-library-only. CI uses Python 3.12/3.13 and runs `python3 -m unittest discover -s scripts/tests -v`, `python3 scripts/bump-version.py --check`, and `ruff check scripts/ --select F,E9` after installing Ruff. Embedded web JavaScript also has a Node syntax gate in `.github/workflows/ci.yml`; changes to that UI must retain it.

Consult [docs/release.md](docs/release.md) and release workflows before version/tag changes. Local tests do not authorize launch, messaging, steering, stopping sessions, or answering approvals on a live host; use only the user-authorized lane and verify its resulting state.
