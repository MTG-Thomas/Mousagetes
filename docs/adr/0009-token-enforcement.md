<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0009. Remote authentication is enforced by m8s, not the bridge

- Status: Accepted
- Date: 2026-09-19

## Context

WS-E measured the reference bridge (`stdio-to-ws`): it accepts any
subprotocol and enforces no token, so a bearer header set by the client
is cosmetic. ADR 0007 puts the endpoint on the private mesh, and the mesh
encrypts transport and gates by firewall — but transport encryption is
not application authentication, and the firewall is per-host, not
per-client.

## Decision

Bearer-token authentication is enforced by m8s (the adapter, or a
fronting proxy m8s controls) before `initialize`. The token is read from
the `Authorization: Bearer` header or the `bearer.<token>` websocket
subprotocol the client sends, compared in constant time, and never
logged. The adapter binds to the mesh interface only. The bridge stays a
dumb byte pipe.

## Consequences

- The mesh firewall becomes defense in depth, not the only control.
- Token storage is a local file with `0600`; rotation is a restart in
  Phases 1-3.
- There is no per-device identity yet; per-device pairing codes are a
  Phase 3+ upgrade.
- The adapter owns auth, so it must reject pre-initialize traffic and
  avoid timing or length leaks in the comparison.
