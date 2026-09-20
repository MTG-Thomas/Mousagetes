<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0004. Controls are slash-commands first, modes/config second

- Status: Accepted
- Date: 2026-09-19

## Context

Phase 1 must expose model selection, reasoning effort, and approval mode,
plus goal/fork/compact/retire. ACP v1 standardizes session modes but not
model or reasoning effort, and client support for pickers (modes,
config options, model pickers) varies and is sometimes marked unstable.
Relying on pickers alone would make required controls unreachable in some
clients.

## Decision

Every mutating control is reachable as a **slash command** intercepted by
the adapter, and *additionally* advertised as a mode or config option
where the client supports it. Slash commands are the compatibility floor;
pickers are progressive enhancement.

Command set: `/model`, `/effort`, `/approval`, `/goal`, `/fork`,
`/compact`, `/retire`, `/budget`.

## Consequences

- All required Phase 1 controls work in any ACP client, including minimal
  ones, because a slash command is just a prompt the adapter intercepts.
- Some duplication: each control has a command path and (where supported)
  a picker path. The command path is authoritative.
- Command names become a small public contract; changing them is a
  compatibility event.
- Clients that render modes/config get a native experience; clients that
  do not still function.
