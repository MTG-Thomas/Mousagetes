<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0007. iOS delivery is a PWA over the private mesh with TLS

- Status: Accepted
- Date: 2026-09-19

## Context

The target phone is an iPhone. The reference ACP client ships a web build
and an Android build but no iOS binary (iOS would require Xcode signing),
and an App Store release is out of scope. A browser page served over
HTTPS may only open `wss://`, so plain `ws://` is blocked. A private mesh
already exists for host-to-host federation: **Defined Networking**
(`dnclient`, Nebula-based). It encrypts transport and enforces a per-host
firewall, but it terminates no TLS.

## Decision

Deliver to iOS as a **browser PWA**: the ACP web client opened in Safari
and added to the Home Screen, connecting to the host over the private
mesh with TLS. Because Defined Networking terminates no TLS, a local
reverse proxy bound to the mesh address provides `wss://` using an
internal CA the phone trusts; the concrete commands are in
[docs/acp-remote.md](../acp-remote.md). No App Store or Xcode build in
Phases 1-3. A branded installable PWA (manifest plus service worker) is
Phase 4, behind [ADR 0012](0012-fork-client.md).

## Consequences

- No App Store review, no signing, and no per-device builds; "install" is
  a browser action.
- Depends on the mesh and on a TLS endpoint that terminates websocket
  upgrades. The mesh encrypts but terminates no TLS, so a local reverse
  proxy with an internal certificate is required, and the phone must
  trust that CA.
- The mesh firewall must allow the TLS port inbound to this host; the
  host-local verification in [docs/acp-remote.md](../acp-remote.md)
  bypassed it.
- The browser tab is the runtime: background execution and push
  notifications are unavailable until a native or Phase 4 PWA path is
  chosen. Steering is foreground-initiated, which matches supervision.
- The same web client also serves desktop browsers, so one client covers
  laptop and phone for Phases 1-3.
