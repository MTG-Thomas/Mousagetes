# SPDX-License-Identifier: AGPL-3.0-or-later
"""Frozen interface between the ACP transport (WS-A) and daemon mapping.

WS-A implements the transport against :class:`LaneMapping`; WS-B implements
the daemon-backed mapping. Neither edits this module without amending the
seam. The mapping yields :class:`MappingEvent` values, which the transport
turns into ACP messages:

- ``("update", {"update": {...}})`` -> a ``session/update`` notification
- ``("permission", {"requestId", "toolCall", "options"})`` -> the
  server-initiated request ``session/request_permission``
- ``("question", {"requestId", "message"})`` -> ``elicitation/create``
- ``("stop", {"stopReason": "end_turn" | "cancelled"})`` -> the
  ``session/prompt`` response

See ``docs/acp.md`` (Phase 1) and
``docs/adr/0008-adapter-module-boundary.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Protocol

# MappingEvent.kind values.
UPDATE = "update"
PERMISSION = "permission"
QUESTION = "question"
STOP = "stop"


@dataclass
class MappingEvent:
    """One event from the daemon mapping to the ACP transport."""

    kind: str
    data: dict[str, Any] = field(default_factory=dict)


class LaneMapping(Protocol):
    """Daemon-backed operations the ACP transport needs, per lane."""

    def list_lanes(self) -> list[dict[str, Any]]:
        """Return known lanes as ``{"laneId", "title", ...}`` records."""
        ...

    def launch_lane(self, cwd: str, title: str) -> str:
        """Create a lane rooted at ``cwd``; return its stable lane id."""
        ...

    def resume_lane(self, lane_id: str, cwd: str) -> list[dict[str, Any]]:
        """Return ACP ``update`` dicts to replay before the load response."""
        ...

    def prompt(self, lane_id: str, text: str) -> Iterator[MappingEvent]:
        """Run one turn, yielding events; end with a ``stop`` event."""
        ...

    def cancel(self, lane_id: str) -> None:
        """Request cancellation of the lane's active turn."""
        ...

    def answer_permission(self, lane_id: str, request_id: str, option_id: str) -> None:
        """Relay a permission decision to the daemon (idempotent)."""
        ...

    def answer_question(self, lane_id: str, request_id: str, text: str) -> None:
        """Relay a user-input answer to the daemon (idempotent)."""
        ...

    def set_control(self, lane_id: str, name: str, value: str) -> None:
        """Set a control: ``name`` in ``{"model", "effort", "approval"}``."""
        ...

    def commands(self) -> list[dict[str, Any]]:
        """Slash-command descriptors for ``available_commands_update``."""
        ...

    def modes(self, lane_id: str) -> dict[str, Any]:
        """ACP ``modes`` payload: ``{currentModeId, availableModes: [...]}``."""
        ...

    def models(self, lane_id: str) -> dict[str, Any]:
        """ACP ``models`` payload: ``{currentModelId, availableModels: [...]}``."""
        ...
