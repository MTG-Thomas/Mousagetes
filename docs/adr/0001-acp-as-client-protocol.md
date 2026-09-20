<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0001. Use ACP as the m8s client protocol

- Status: Accepted
- Date: 2026-09-19

## Context

m8s supervises Muse lanes but has no rich client surface; the v1 web view
shows swarm state only. We want OpenChamber-like session interaction from
a phone. Three candidate client contracts were researched:

- **OpenCode's HTTP+SSE API**, to reuse OpenChamber. Richest app, but the
  largest shim: 60+ routes, a fine-grained event taxonomy, and
  `~/.config/opencode` filesystem expectations. Muse speaks stdio
  JSON-RPC, so the transport and session models do not line up.
- **Codex app-server**, also JSON-RPC over stdio/ws. Open and
  codegen-friendly, but it is Codex's own harness contract (~130 client
  methods) rather than an interoperability standard, and its clients
  assume Codex semantics.
- **Agent Client Protocol (ACP)**, an open Apache-2.0 standard for
  agent/client interoperability. Small surface, stdio transport that
  matches MSP, a broad client ecosystem (Zed, JetBrains, VS Code, mobile
  and web clients), and existing adapters for other harnesses, including
  one for Muse.

## Decision

m8s ships an **ACP adapter** as its external client surface. We do not
emulate OpenCode's API and we do not emulate Codex app-server.

## Consequences

- The client app is replaceable; the durable asset is the m8s adapter,
  which works with any ACP client.
- Desktop editors work for free, and mobile/web delivery is a PWA over a
  websocket transport rather than a bespoke app.
- m8s does not get OpenChamber's full feature set (first-class diff
  review, git/PR workflow, embedded terminal) in Phases 1-3. Those
  require a later custom PWA, decided separately.
- ACP's remote transport is still a draft, so remote access uses the
  standard stdio transport behind an external bridge.
- Because ACP is editor-centric, a swarm/board view remains a separate
  surface (the v1 web view) until Phase 4.
