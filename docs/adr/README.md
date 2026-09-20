<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# Architecture decision records (m8s)

One file per decision. Each record is immutable once accepted; a later
decision supersedes an earlier one rather than editing it.

The plan these support is [docs/acp.md](../acp.md).

## Status values

- **Proposed** — drafted, awaiting the maintainer's accept/reject.
- **Accepted** — in force.
- **Superseded** — replaced by a later record, which is linked.

## Index

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-acp-as-client-protocol.md) | Use ACP as the m8s client protocol | Accepted |
| [0002](0002-daemon-authority.md) | The daemon stays the sole owner of `muse serve` | Accepted |
| [0003](0003-remote-client-capability-boundary.md) | Remote clients get no filesystem or terminal capabilities | Accepted |
| [0004](0004-slash-first-controls.md) | Controls are slash-commands first, modes/config second | Accepted |
| [0005](0005-stdlib-only-external-ws-bridge.md) | Keep Python stdlib-only; websocket via an external bridge | Accepted |
| [0006](0006-lane-session-identity.md) | One lane equals one ACP session | Accepted |
| [0007](0007-ios-pwa-private-mesh.md) | iOS delivery is a PWA over the private mesh with TLS | Accepted |
| [0008](0008-adapter-module-boundary.md) | The ACP adapter lives in its own package | Accepted |
| [0009](0009-token-enforcement.md) | Remote auth is enforced by m8s, not the bridge | Accepted |
| [0010](0010-roster-client-local.md) | The lane roster is client-local until the client is forked | Accepted |
| [0011](0011-self-host-client.md) | Self-host the ACP client on the mesh HTTPS origin | Accepted |
| [0012](0012-fork-client.md) | Adopt and fork the ACP web client as the m8s base | Accepted |

## Template

```markdown
<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# NNNN. Title

- Status: Proposed
- Date: YYYY-MM-DD

## Context
Why a decision is needed.

## Decision
What we will do.

## Consequences
What becomes easier, what becomes harder, and what we accept.
```
