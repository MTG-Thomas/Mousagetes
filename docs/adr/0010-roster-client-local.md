<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0010. The lane roster is client-local until the client is forked

- Status: Accepted
- Date: 2026-09-19

## Context

ACP v1 has no `session/list`, and the reference client exposes only the
sessions it created and stored locally. ADR 0006 maps each lane to an ACP
session and assumed the client's session list would be the lane roster;
WS-E found that list cannot be enumerated by the stock client.

## Decision

For Phases 1-3, the adapter does not promise lane-roster enumeration, and
we accept client-local session history: a phone can resume lanes it
created, not arbitrary pre-existing lanes. The authoritative roster stays
in `m8s web` v1 and the CLI. A roster in the client requires the fork
(ADR 0012).

## Consequences

- ADR 0006's "session list = lane roster" is aspirational until the fork.
- Opening an existing lane from the phone needs a workaround (a slash
  command or deep link) until the fork adds a roster method.
- The daemon remains the source of truth; nothing about lane identity
  changes, only discoverability from the phone.
