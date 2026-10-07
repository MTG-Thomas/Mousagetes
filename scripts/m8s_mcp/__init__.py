# SPDX-License-Identifier: AGPL-3.0-or-later
"""m8s MCP adapter (Codex bridge).

A thin, stdlib-only Model Context Protocol front end for the m8s daemon.
Codex reaches it over SSH stdio; it reaches Muse lanes only through the
existing m8s Unix control API. It never spawns ``muse serve``, owns no
lane state, and never auto-answers approvals. See ``docs/mcp.md``.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
