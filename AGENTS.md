# Mousagetes agent guidance

Mousagetes (`m8s`) supervises Muse coding sessions over MSP and exposes CLI, web, and ACP client surfaces. [README.md](README.md) is the entry point; read relevant [architecture decisions](docs/adr/README.md) before changing a protocol or authority boundary.

## Map

`scripts/muse-msp.py` implements the supervisor; `scripts/m8s` is its short launcher. `scripts/m8s_acp/` contains the ACP adapter, mapping, JSON-RPC, and contract code. `scripts/tests/` contains regression and integration coverage. Use [docs/bus.md](docs/bus.md), [docs/compile.md](docs/compile.md), and [docs/multihost.md](docs/multihost.md) for leases, board reconciliation, and host behavior.

## Invariants

The daemon alone owns `muse serve`; adapters use its control API and never attach a second supervisor or launch an independent serve host. Lane state belongs to daemon records. Adapter disconnection must not terminate running lanes. Remote ACP clients advertise no filesystem or terminal capabilities and must never receive `fs/*` or `terminal/*` RPC requests; rendered output is a separate disclosure surface. Preserve lane/session identity, leases, token enforcement, and pending approval/user-input boundaries.

## Checks and releases

Use Python 3.10+; the runtime is standard-library-only. CI uses Python 3.12/3.13 and runs `python3 -m unittest discover -s scripts/tests -v`, `python3 scripts/bump-version.py --check`, and `ruff check scripts/ --select F,E9` after installing Ruff. Embedded web JavaScript also has a Node syntax gate in `.github/workflows/ci.yml`; changes to that UI must retain it.

Consult [docs/release.md](docs/release.md) and release workflows before version/tag changes. Local tests do not authorize launch, messaging, steering, stopping sessions, or answering approvals on a live host; use only the user-authorized lane and verify its resulting state.

## Secret gate before publication

Before publishing the current PR branch, commit the complete candidate and run
`python3 -I scripts/secret_gate.py --base origin/main` (native Windows: `python -I`). Require exit 0;
missing tooling/authentication, scan errors and skipped scans block publication.
Then run the existing repository gates. Repeat after changing outgoing commits.
Push exactly the scanned HEAD. This PR scan covers the current branch against its
fetched intended base; other refs, multi-ref pushes and rewritten history require
`--pre-push` with Git's complete ref-update stream. Stop if any pushed ref is
uncovered. See [secret scanning](SECRET-SCANNING.md) for base selection, new branches,
pre-push input and prerequisites. Preserve GitGuardian App, CI and Gitleaks checks.

For a non-default PR target, fetch its destination and pass that exact ref to
`--base`; the remote default is not a substitute for a release/maintenance base.

The secret gate requires Python 3.11+ and approved ggshield >=1.56.1,<2.
