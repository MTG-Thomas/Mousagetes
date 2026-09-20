<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0006. One lane equals one ACP session

- Status: Accepted
- Date: 2026-09-19

## Context

ACP clients present sessions (threads) to the user and resume them by id.
m8s already has a stable unit of work — the lane — with an alias, a
session id, a worktree, and an event log. The mapping between the two
must survive reconnect, phone sleep, and daemon restart.

## Decision

Each m8s lane maps to exactly one ACP session whose id is the lane's
stable id. `session/new` creates a lane (`launch`) and `session/load`
resumes one (`resume-session`). The lane worktree is the session working
directory. A single ACP connection may host many sessions, one per lane.

## Consequences

- The client's session list is the lane roster, and resume uses m8s's
  existing session persistence rather than adapter state.
- Lane lifecycle maps cleanly: retire/unload are session close/archival,
  and a session that no longer exists returns a typed error.
- If a lane's alias changes, the session id must remain the stable
  underlying id, not the alias, or resume breaks. The adapter resolves
  aliases to the stable id on every call.
- Multi-device fan-out, if added, is a property of the daemon control API
  and does not change this one-to-one mapping.
