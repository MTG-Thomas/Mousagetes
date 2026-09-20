<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0011. Self-host the ACP client on the mesh HTTPS origin

- Status: Accepted
- Date: 2026-09-19

## Context

WS-E found that Chrome's Private Network Access rule blocks the public
HTTPS client (`acp-ui.github.io`) from opening `ws://localhost`, and
mixed-content rules block `ws://` from any HTTPS page. Serving the client
from a third-party origin also adds a supply-chain dependency and
unwanted telemetry.

## Decision

Serve the ACP client bundle from the same mesh HTTPS origin as the
websocket endpoint (internal CA), not from a public site. The page and
the socket share a host, which clears both the PNA and mixed-content
barriers and removes the third-party dependency.

## Consequences

- The phone must trust the internal CA once.
- m8s owns hosting the bundle, so it must pin a version and track
  upstream (ADR 0012).
- ADR 0007's "browser PWA" becomes a self-hosted bundle rather than a
  public web app.
