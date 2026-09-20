# SPDX-License-Identifier: AGPL-3.0-or-later
"""Daemon-backed command mapping for the ACP adapter (WS-B).

:class:`DaemonMapping` implements :class:`m8s_acp.contract.LaneMapping` by
translating each ACP operation into one or more requests on the m8s daemon
control socket. It is the only place that knows daemon verbs; the transport
(WS-A) never speaks MSP. See ``docs/acp.md`` (Phase 1, "Action mapping") and
``docs/adr/0008-adapter-module-boundary.md``.

Lane creation without a prompt (open question 6)
------------------------------------------------

ACP ``session/new`` carries only a working directory, but m8s ``launch``
starts the session's first turn and therefore requires a prompt. The
mapping keeps the ``launch`` verb (rather than bypassing the daemon's
roster/alias/budget bookkeeping with ``call session/start``) and submits a
fixed :data:`NEW_SESSION_BRIEF` as that first turn. The brief tells the
lane it was opened by an ACP client with no task yet and to await
instructions; the client's first ``session/prompt`` is queued behind it by
the daemon (``ifBusy: queue``). The brief is overridable via the
``new_session_brief`` constructor argument. One lane-creation path
(daemon-authoritative, ADR 0002/0006) is worth one extra harmless turn.

Control surface
---------------

Every control is reachable as a slash command intercepted by :meth:`prompt`
(``/model``, ``/effort``, ``/approval``, ``/goal``, ``/fork``,
``/compact``, ``/retire``, ``/budget``); :meth:`commands` advertises them
for ``available_commands_update`` (R9) and :meth:`run_command` executes
one. The ACP mode/model pickers are served from :meth:`modes` and
:meth:`models` (R3) and write through :meth:`set_control` (R4).
"""

from __future__ import annotations

import json
import re
import secrets
import time
from typing import Any, Callable, Iterator

from . import contract
from .daemon import ControlClient, DaemonError

# ACP ``session/new`` has no prompt. This is the first-turn brief the lane
# receives so that ``launch`` (which owns roster/alias/budget setup) stays
# the single creation path. It must stay harmless and non-actionable.
NEW_SESSION_BRIEF = (
    "An ACP client opened this session and has not issued a task yet. "
    "Do not start work; acknowledge briefly and wait for the next instruction."
)

# m8s approval modes (the ``--approval-mode`` choices in muse-msp.py).
APPROVAL_MODES: tuple[dict[str, str], ...] = (
    {
        "id": "allowAll",
        "name": "Allow all",
        "description": "Run every tool without asking.",
    },
    {
        "id": "promptUnmatched",
        "name": "Ask on unmatched",
        "description": "Ask only for tools without an explicit rule.",
    },
    {
        "id": "onRequest",
        "name": "Ask on request",
        "description": "Ask whenever the lane requests approval.",
    },
    {
        "id": "denyUnmatched",
        "name": "Deny unmatched",
        "description": "Deny tools without an explicit allow rule.",
    },
)
DEFAULT_APPROVAL_MODE = "onRequest"

# Materialized-view item kinds that carry assistant prose or reasoning. The
# view emits whole items (``item/started`` for work in progress,
# ``item/completed`` when settled); the observed daemon schema names the
# assistant item ``agentMessage``.
_MESSAGE_ITEM_KINDS = frozenset({"agentMessage", "assistantMessage", "message"})
_THOUGHT_ITEM_KINDS = frozenset(
    {"reasoning", "agentThought", "agentThoughtMessage", "thinking"}
)
_TOOL_ITEM_KINDS = frozenset(
    {
        "toolCall",
        "tool",
        "commandExecution",
        "fileChange",
        "mcpToolCall",
        "shell",
        "bash",
    }
)


class TurnTimeoutError(DaemonError):
    """The lane's turn produced no terminal event before the deadline.

    Raised instead of fabricating an ``end_turn``: the transport surfaces it
    as a typed JSON-RPC error, so a stalled turn is never reported as a
    successful completion.
    """


class TurnFailedError(DaemonError):
    """The lane's turn reached a terminal ``failed`` state."""


def _command(name: str, description: str, hint: str = "") -> dict[str, Any]:
    descriptor: dict[str, Any] = {"name": name, "description": description}
    if hint:
        descriptor["input"] = {"hint": hint}
    return descriptor


def command_descriptors() -> list[dict[str, Any]]:
    """Fresh ``available_commands_update`` descriptors for the controls."""
    return [
        _command("model", "Switch the lane's model.", "<model-id>"),
        _command("effort", "Set reasoning effort.", "low|medium|high"),
        _command(
            "approval",
            "Set the approval mode.",
            "allowAll|promptUnmatched|onRequest|denyUnmatched",
        ),
        _command("goal", "Set or replace the lane goal.", "<objective>"),
        _command("fork", "Fork the lane into a new session."),
        _command("compact", "Compact the lane transcript."),
        _command("retire", "Retire the lane once its work is done."),
        _command("budget", "Read or set the lane token budget.", "[max-tokens]"),
    ]


def parse_command(text: str) -> tuple[str, str] | None:
    """Split ``/name argument`` into ``("name", "argument")``, else ``None``."""
    stripped = (text or "").strip()
    if not stripped.startswith("/"):
        return None
    parts = stripped[1:].split(None, 1)
    if not parts or not parts[0]:
        return None
    argument = parts[1].strip() if len(parts) > 1 else ""
    return parts[0], argument


class DaemonMapping(contract.LaneMapping):
    """A :class:`~m8s_acp.contract.LaneMapping` backed by the daemon socket.

    The ``client`` only needs a ``request(dict) -> result`` method; the
    default is :class:`~m8s_acp.daemon.ControlClient`. Tests inject an
    in-memory fake control server the same way ``test_muse_msp.py`` injects
    a fake host.
    """

    def __init__(
        self,
        client: Any | None = None,
        *,
        socket_path: str | None = None,
        sleep: Callable[[float], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        poll_interval: float = 0.1,
        prompt_timeout: float = 120.0,
        page_limit: int = 50,
        prime_pages: int = 20,
        replay_pages: int = 20,
        max_polls: int = 1200,
        new_session_brief: str = NEW_SESSION_BRIEF,
    ) -> None:
        self._client = client if client is not None else ControlClient(socket_path)
        self._sleep = sleep if sleep is not None else time.sleep
        self._clock = clock
        self._poll_interval = poll_interval
        self._prompt_timeout = prompt_timeout
        self._page_limit = page_limit
        self._prime_pages = prime_pages
        self._replay_pages = replay_pages
        self._max_polls = max_polls
        self._new_session_brief = new_session_brief
        # Per-lane view cursors and seen-event keys for prompt/resume.
        self._cursors: dict[str, str] = {}
        self._seen: dict[str, set[str]] = {}
        # Tool-call ids already introduced as ``tool_call`` (R8), per lane.
        self._seen_tool_calls: dict[str, set[str]] = {}
        # Assistant/thought text already streamed, per (lane, itemId), so a
        # started item followed by its completed revision emits the delta
        # once and keeps a stable ``messageId``.
        self._message_text: dict[tuple[str, str], str] = {}
        # Permission/question request ids already emitted or answered.
        self._emitted_permissions: set[tuple[str, str]] = set()
        self._emitted_questions: set[tuple[str, str]] = set()
        self._answered_permissions: set[tuple[str, str]] = set()
        self._answered_questions: set[tuple[str, str]] = set()
        self._pending_context: dict[tuple[str, str], dict[str, Any]] = {}

    # -- transport helpers ------------------------------------------------

    def _call(self, request: dict[str, Any]) -> Any:
        return self._client.request(request)

    def _call_method(
        self,
        method: str,
        lane_id: str | None = None,
        params: dict[str, Any] | None = None,
        command_id: str = "auto",
    ) -> Any:
        request: dict[str, Any] = {
            "command": "call",
            "method": method,
            "commandId": command_id,
        }
        if lane_id is not None:
            request["session"] = lane_id
        if params is not None:
            request["params"] = params
        return self._call(request)

    def _active_turn(self, lane_id: str) -> str | None:
        """The lane's currently active turn id, if any.

        Used to recognise the *next* turn that starts after a queued
        prompt, since a queued ``send`` does not return the new turn id.
        """
        try:
            result = self._call({"command": "list"}) or {}
        except DaemonError:
            return None
        for item in result.get("sessions", []) or []:
            if (item.get("sessionId") or item.get("id")) == lane_id:
                active = item.get("activeTurnId")
                return str(active) if active else None
        return None

    # -- LaneMapping: roster and lifecycle --------------------------------

    def list_lanes(self) -> list[dict[str, Any]]:
        result = self._call({"command": "list"}) or {}
        lanes: list[dict[str, Any]] = []
        for item in result.get("sessions", []) or []:
            lane_id = item.get("sessionId") or item.get("id")
            if not lane_id:
                continue
            lanes.append(
                {
                    "laneId": str(lane_id),
                    "title": str(
                        item.get("alias") or item.get("name") or lane_id
                    ),
                    "cwd": item.get("workspace"),
                    "status": item.get("status"),
                    "modelId": item.get("modelId"),
                    "approvalMode": item.get("approvalMode") or item.get("mode"),
                }
            )
        return lanes

    def launch_lane(self, cwd: str, title: str) -> str:
        # Muse's session-name authority is global and rejects duplicates,
        # and a launch that fails its internal rename can leave a session
        # behind -- so mint a unique alias up front instead of retrying.
        # The ACP client owns the display title; this alias is m8s-internal.
        base = (title or "").strip() or "ACP session"
        name = f"{base}-{secrets.token_hex(3)}"
        result = self._call(
            {
                "command": "launch",
                "name": name,
                "workspace": cwd,
                "prompt": self._new_session_brief,
            }
        ) or {}
        session = result.get("session") or {}
        lane_id = session.get("sessionId") or session.get("id")
        if not lane_id:
            raise DaemonError("launch returned no session id")
        return str(lane_id)

    def resume_lane(self, lane_id: str, cwd: str) -> list[dict[str, Any]]:
        self._call_method("session/resume", lane_id)
        # The header reply carries the lane's active turn/model/status; the
        # transcript itself comes from the paged view.
        self._call_method("session/read", lane_id)
        self._cursors.pop(lane_id, None)
        self._seen.pop(lane_id, None)
        updates: list[dict[str, Any]] = []
        for _ in range(self._replay_pages):
            page = self._view_page(lane_id)
            events = page.get("events") or []
            for event in events:
                updates.extend(self._updates_from_event(event, lane_id))
            if not page.get("nextCursor"):
                break
        return updates

    def prompt(self, lane_id: str, text: str) -> Iterator[contract.MappingEvent]:
        command = parse_command(text)
        if command is not None:
            yield from self._run_slash(lane_id, *command)
            return
        self._prime_lane(lane_id)
        # Everything at or before this cursor belongs to earlier turns; the
        # prompt's turn is read forward from here on every poll.
        turn_start_cursor = self._cursors.get(lane_id)
        # The daemon returns the id of the turn it accepted for this prompt
        # (``{"turnId": ..., "startedNewTurn": ...}``). It assigns the id
        # when the prompt is accepted, even when it is queued behind a
        # running turn (``startedNewTurn: false``), and the started turn later
        # carries that same id — so scope every terminal and every streamed
        # update to it. Without this the loop could latch the session brief's
        # terminal event and return before this turn ran.
        prior_turn = self._active_turn(lane_id)
        result = self._call(
            {"command": "send", "session": lane_id, "prompt": text}
        ) or {}
        turn_id: str | None = None
        pending_command: str | None = None
        if isinstance(result, dict):
            candidate = result.get("turnId")
            if candidate is not None:
                turn_id = str(candidate)
            else:
                # Defensive fallback for a daemon that only returns the
                # submission id: adopt the turn id once its records appear.
                pending_command = str(result.get("commandId") or "") or None
        deadline = self._clock() + self._prompt_timeout
        polls = 0
        while True:
            yield from self._drain_pending(lane_id)
            # The materialized view is not append-only: as a turn progresses
            # the daemon inserts item records at earlier cursors and revises
            # existing ones in place (a ``turn/completed`` can move from an
            # interim ``failed``/``reason: incomplete`` to its real terminal).
            # A forward cursor therefore misses records; re-read the region
            # from just before the sent turn every poll and let the stable
            # seen-keys suppress repeats.
            events = self._read_from(lane_id, turn_start_cursor)
            terminal: str | None = None
            for event in events:
                if turn_id is None:
                    adopted: str | None = None
                    if (
                        pending_command is not None
                        and _command_id_of(event) == pending_command
                    ):
                        adopted = _turn_id_of(event)
                    if (
                        adopted is None
                        and str(event.get("method") or "") == "turn/started"
                    ):
                        candidate_turn = _turn_id_of(event)
                        if candidate_turn and candidate_turn != prior_turn:
                            adopted = candidate_turn
                    if adopted:
                        turn_id = adopted
                        pending_command = None
                    if turn_id is None:
                        # The queued turn has not started yet. Never stream
                        # or terminate on the still-running prior turn (the
                        # synthetic session brief in the common case).
                        continue
                event_turn = _turn_id_of(event)
                if turn_id and event_turn and event_turn != turn_id:
                    # An earlier turn's records (most often the synthetic
                    # session brief): never stream or terminate on them.
                    self._seen.setdefault(lane_id, set()).add(_event_key(event))
                    continue
                for update in self._new_updates(lane_id, event):
                    # The client already rendered the prompt it just sent;
                    # only the lane's own output streams back.
                    if update.get("sessionUpdate") == "user_message_chunk":
                        continue
                    yield contract.MappingEvent(
                        contract.UPDATE, {"update": update}
                    )
                terminal = self._terminal_of(event, turn_id) or terminal
            if terminal == "failed":
                raise TurnFailedError(
                    f"turn {turn_id or '?'} ended in failure"
                )
            if terminal is not None:
                yield contract.MappingEvent(
                    contract.STOP, {"stopReason": terminal}
                )
                return
            polls += 1
            if polls >= self._max_polls or self._clock() >= deadline:
                # Never pretend a stalled turn succeeded. The transport
                # turns this typed error into a JSON-RPC error response.
                raise TurnTimeoutError(
                    f"turn {turn_id or '?'} produced no terminal event "
                    f"within {self._prompt_timeout:g}s"
                )
            self._sleep(self._poll_interval)

    def cancel(self, lane_id: str) -> None:
        self._call_method("turn/cancel", lane_id)

    # -- LaneMapping: permissions and questions ---------------------------

    def answer_permission(
        self, lane_id: str, request_id: str, option_id: str
    ) -> None:
        key = (lane_id, str(request_id))
        if key in self._answered_permissions:
            return
        context = self._pending_context.get(key, {})
        approval_id = str(context.get("approvalId") or request_id)
        requirement_id = context.get("requirementId")
        if requirement_id is None:
            requirement_id = self._lookup_requirement(lane_id, approval_id)
        params: dict[str, Any] = {
            "approvalId": approval_id,
            "choiceId": option_id,
        }
        if requirement_id is not None:
            params["requirementId"] = requirement_id
        self._call_method("approval/decide", lane_id, params)
        self._answered_permissions.add(key)

    def answer_question(self, lane_id: str, request_id: str, text: str) -> None:
        key = (lane_id, str(request_id))
        if key in self._answered_questions:
            return
        self._call_method(
            "userInput/clarify",
            lane_id,
            {
                "userInputId": str(request_id),
                "clarification": {"format": "text", "content": text},
            },
        )
        self._answered_questions.add(key)

    # -- LaneMapping: controls and advertisements -------------------------

    def set_control(self, lane_id: str, name: str, value: str) -> None:
        if name == "model":
            self._call_method("session/setModel", lane_id, {"model": value})
        elif name == "effort":
            self._call_method(
                "session/setReasoningEffort",
                lane_id,
                {"reasoningEffort": value},
            )
        elif name == "approval":
            self._call_method(
                "session/setApprovalMode", lane_id, {"mode": value}
            )
        else:
            raise ValueError(f"unknown control: {name!r}")

    def commands(self) -> list[dict[str, Any]]:
        return command_descriptors()

    def modes(self, lane_id: str) -> dict[str, Any]:
        lane = self._lane(lane_id)
        current = lane.get("approvalMode") or DEFAULT_APPROVAL_MODE
        if current not in {mode["id"] for mode in APPROVAL_MODES}:
            current = DEFAULT_APPROVAL_MODE
        return {
            "currentModeId": current,
            "availableModes": [dict(mode) for mode in APPROVAL_MODES],
        }

    def models(self, lane_id: str) -> dict[str, Any]:
        lane = self._lane(lane_id)
        current = lane.get("modelId")
        available = self._available_models()
        ids = {model["modelId"] for model in available}
        if current and current not in ids:
            available.insert(
                0,
                {"modelId": current, "name": current, "description": ""},
            )
        current_id = current or (available[0]["modelId"] if available else "")
        return {
            "currentModelId": str(current_id or ""),
            "availableModels": available,
        }

    # -- slash commands ---------------------------------------------------

    def run_command(self, lane_id: str, name: str, argument: str = "") -> None:
        """Execute one slash command against the daemon."""
        name = name.lstrip("/")
        if name in ("model", "effort", "approval"):
            self.set_control(lane_id, name, argument)
            return
        if name == "goal":
            self._call_method("goal/set", lane_id, {"objective": argument})
            return
        if name == "fork":
            self._call_method("session/fork", lane_id)
            return
        if name == "compact":
            self._call_method("session/compact", lane_id)
            return
        if name == "retire":
            self._call({"command": "retire", "session": lane_id})
            return
        if name == "budget":
            request: dict[str, Any] = {
                "command": "budget",
                "session": lane_id,
            }
            if argument.strip().isdigit():
                request["maxTokens"] = int(argument.strip())
            self._call(request)
            return
        raise ValueError(f"unknown command: /{name}")

    def _run_slash(
        self, lane_id: str, name: str, argument: str
    ) -> Iterator[contract.MappingEvent]:
        self.run_command(lane_id, name, argument)
        yield contract.MappingEvent(
            contract.UPDATE,
            {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {
                        "type": "text",
                        "text": f"/{name} applied.",
                    },
                }
            },
        )
        yield contract.MappingEvent(contract.STOP, {"stopReason": "end_turn"})

    # -- view paging and event translation --------------------------------

    def _lane(self, lane_id: str) -> dict[str, Any]:
        for lane in self.list_lanes():
            if lane["laneId"] == lane_id:
                return lane
        return {}

    def _available_models(self) -> list[dict[str, str]]:
        try:
            result = self._call_method("model/list")
        except DaemonError:
            return []
        models: Any = result
        if isinstance(result, dict):
            models = (
                result.get("models")
                or result.get("availableModels")
                or []
            )
        if not isinstance(models, list):
            return []
        available: list[dict[str, str]] = []
        for model in models:
            if isinstance(model, str):
                available.append(
                    {"modelId": model, "name": model, "description": ""}
                )
                continue
            if not isinstance(model, dict):
                continue
            model_id = model.get("modelId") or model.get("id") or model.get("name")
            if not model_id:
                continue
            available.append(
                {
                    "modelId": str(model_id),
                    "name": str(model.get("name") or model_id),
                    "description": str(model.get("description") or ""),
                }
            )
        return available

    def _view_page(self, lane_id: str) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": self._page_limit}
        cursor = self._cursors.get(lane_id)
        if cursor:
            params["cursor"] = cursor
        page = self._call_method("view/page", lane_id, params) or {}
        next_cursor = page.get("nextCursor")
        if not isinstance(next_cursor, str) or not next_cursor:
            next_cursor = None
        for event in page.get("events") or []:
            view_cursor = (event.get("params") or {}).get("viewCursor")
            if isinstance(view_cursor, str) and view_cursor:
                next_cursor = view_cursor
        if next_cursor:
            self._cursors[lane_id] = next_cursor
        return page

    def _read_from(
        self, lane_id: str, start_cursor: str | None
    ) -> list[dict[str, Any]]:
        """Read every view record from ``start_cursor`` to the current end.

        Re-reads a fixed region rather than advancing a cursor, because the
        materialized view inserts and revises records at earlier cursors as
        a turn progresses.
        """
        events: list[dict[str, Any]] = []
        cursor = start_cursor
        for _ in range(self._replay_pages):
            params: dict[str, Any] = {"limit": self._page_limit}
            if cursor:
                params["cursor"] = cursor
            page = self._call_method("view/page", lane_id, params) or {}
            page_events = page.get("events") or []
            events.extend(page_events)
            if not page_events:
                break
            next_cursor = page.get("nextCursor")
            if not isinstance(next_cursor, str) or not next_cursor:
                next_cursor = _view_cursor_of(page_events[-1])
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
        return events

    def _prime_lane(self, lane_id: str) -> None:
        """Mark the lane's existing transcript seen, so only new events emit."""
        seen = self._seen.setdefault(lane_id, set())
        for _ in range(self._prime_pages):
            page = self._view_page(lane_id)
            events = page.get("events") or []
            if not events:
                break
            for event in events:
                seen.add(_event_key(event))
            if not page.get("nextCursor"):
                break

    def _new_updates(
        self, lane_id: str, event: dict[str, Any]
    ) -> list[dict[str, Any]]:
        key = _event_key(event)
        seen = self._seen.setdefault(lane_id, set())
        if key in seen:
            return []
        seen.add(key)
        return self._updates_from_event(event, lane_id)

    def _updates_from_event(
        self, event: dict[str, Any], lane_id: str | None = None
    ) -> list[dict[str, Any]]:
        params = event.get("params")
        if not isinstance(params, dict):
            params = {}
        item = params.get("item")
        if not isinstance(item, dict):
            item = {}
        method = str(event.get("method") or event.get("kind") or "")
        kind = str(item.get("kind") or "")
        if kind == "userMessage":
            return [
                {
                    "sessionUpdate": "user_message_chunk",
                    "content": {
                        "type": "text",
                        "text": str(item.get("text") or ""),
                    },
                }
            ]
        if kind in _MESSAGE_ITEM_KINDS:
            update = self._message_update(lane_id, item, "agent_message_chunk")
            return [update] if update else []
        if kind in _THOUGHT_ITEM_KINDS:
            update = self._message_update(lane_id, item, "agent_thought_chunk")
            return [update] if update else []
        if kind in _TOOL_ITEM_KINDS:
            return [self._tool_update(lane_id, item)]
        if method in ("agent_message", "agentMessage", "agent_message_chunk"):
            return [_agent_chunk(params.get("text") or params.get("delta"))]
        if method in ("agent_thought", "agentThought", "agent_thought_chunk"):
            return [_thought_chunk(params.get("text") or params.get("delta"))]
        return []

    def _message_update(
        self,
        lane_id: str | None,
        item: dict[str, Any],
        chunk_kind: str,
    ) -> dict[str, Any] | None:
        """Turn one materialized assistant/thought item into a text chunk.

        The view exposes whole items, and an item may be revised from
        ``item/started`` to ``item/completed``. Keep the text already sent
        per item and stream only the delta, so a completion does not re-send
        the whole message. ``messageId`` is the item id and stays stable for
        every chunk of that message (R11).
        """
        item_id = str(item.get("itemId") or item.get("id") or "")
        text = _text_of(item.get("text"))
        if not text:
            text = _text_of(item.get("content"))
        if not text:
            return None
        key = (lane_id or "", item_id)
        previous = self._message_text.get(key)
        if previous is not None:
            if not text.startswith(previous):
                # A revision that does not extend the sent prefix (or a
                # duplicate): do not stream it twice.
                return None
            delta = text[len(previous):]
            if not delta:
                return None
            self._message_text[key] = text
            text = delta
        else:
            self._message_text[key] = text
        update: dict[str, Any] = {
            "sessionUpdate": chunk_kind,
            "content": {"type": "text", "text": text},
        }
        if item_id:
            update["messageId"] = item_id
        return update

    def _tool_update(
        self, lane_id: str | None, item: dict[str, Any]
    ) -> dict[str, Any]:
        tool_call_id = str(
            item.get("itemId") or item.get("toolCallId") or item.get("id") or "tool"
        )
        tool = str(
            item.get("tool")
            or item.get("title")
            or item.get("name")
            or item.get("kind")
            or "tool"
        )
        observed = (
            item.get("visibleOutput")
            or item.get("output")
            or item.get("args")
            or item.get("command")
        )
        if observed:
            title = f"{tool}: {_one_line(_stringify(observed))}"
        else:
            title = tool
        seen = self._seen_tool_calls.setdefault(lane_id or "", set())
        first = tool_call_id not in seen
        seen.add(tool_call_id)
        update: dict[str, Any] = {
            "sessionUpdate": "tool_call" if first else "tool_call_update",
            "toolCallId": tool_call_id,
            "title": title,
            "status": _acp_status(item.get("status")),
        }
        locations = _locations(item)
        if locations:
            update["locations"] = locations
        return update

    def _terminal_of(
        self, event: dict[str, Any], turn_id: str | None = None
    ) -> str | None:
        method = str(event.get("method") or "")
        if method != "turn/completed":
            return None
        params = event.get("params")
        if not isinstance(params, dict):
            params = {}
        event_turn = _turn_id_of(event)
        if turn_id and event_turn and event_turn != turn_id:
            return None
        item = params.get("item")
        if not isinstance(item, dict):
            item = {}
        terminal = str(
            params.get("terminal")
            or params.get("status")
            or item.get("status")
            or ""
        ).lower()
        reason = str(params.get("reason") or "").lower()
        if terminal in ("cancelled", "canceled"):
            return "cancelled"
        if terminal in ("failed", "failure", "error", "errored"):
            # The materialized view briefly reports a turn as failed with
            # ``reason: "incomplete"`` while children are still settling,
            # then revises it to its real terminal. Do not stop on that
            # interim marker.
            if reason == "incomplete" and "durationMs" not in params:
                return None
            return "failed"
        return "end_turn"

    # -- pending approvals and user input ---------------------------------

    def _drain_pending(
        self, lane_id: str
    ) -> Iterator[contract.MappingEvent]:
        try:
            result = self._call({"command": "pending", "session": lane_id}) or {}
        except DaemonError:
            return
        if not isinstance(result, dict):
            return
        approvals = result.get("approvals") or result.get("pendingApprovals") or []
        for approval in approvals:
            event = self._permission_event(lane_id, approval)
            if event is not None:
                yield event
        inputs = (
            result.get("userInputs")
            or result.get("inputs")
            or result.get("questions")
            or []
        )
        for user_input in inputs:
            event = self._question_event(lane_id, user_input)
            if event is not None:
                yield event

    def _permission_event(
        self, lane_id: str, approval: Any
    ) -> contract.MappingEvent | None:
        if not isinstance(approval, dict):
            return None
        request_id = str(
            approval.get("approvalId")
            or approval.get("requestId")
            or approval.get("id")
            or ""
        )
        if not request_id:
            return None
        key = (lane_id, request_id)
        if key in self._emitted_permissions or key in self._answered_permissions:
            return None
        self._emitted_permissions.add(key)
        self._pending_context[key] = {
            "approvalId": request_id,
            "requirementId": approval.get("currentRequirementId")
            or approval.get("requirementId"),
        }
        return contract.MappingEvent(
            contract.PERMISSION,
            {
                "requestId": request_id,
                "toolCall": _tool_call_from_approval(approval, request_id),
                "options": _options_from_approval(approval),
            },
        )

    def _question_event(
        self, lane_id: str, user_input: Any
    ) -> contract.MappingEvent | None:
        if not isinstance(user_input, dict):
            return None
        request_id = str(
            user_input.get("userInputId")
            or user_input.get("requestId")
            or user_input.get("id")
            or ""
        )
        if not request_id:
            return None
        key = (lane_id, request_id)
        if key in self._emitted_questions or key in self._answered_questions:
            return None
        self._emitted_questions.add(key)
        return contract.MappingEvent(
            contract.QUESTION,
            {
                "requestId": request_id,
                "message": _question_text(user_input),
            },
        )

    def _lookup_requirement(
        self, lane_id: str, approval_id: str
    ) -> str | None:
        try:
            result = self._call({"command": "pending", "session": lane_id}) or {}
        except DaemonError:
            return None
        if not isinstance(result, dict):
            return None
        for approval in result.get("approvals") or []:
            if not isinstance(approval, dict):
                continue
            if str(approval.get("approvalId")) == approval_id:
                requirement = approval.get("currentRequirementId") or approval.get(
                    "requirementId"
                )
                return str(requirement) if requirement is not None else None
        return None


# ---------------------------------------------------------------------------
# Translation helpers (module-level so they are easy to unit test).
# ---------------------------------------------------------------------------


def _event_key(event: dict[str, Any]) -> str:
    """A stable identity for a view record.

    The materialized view reassigns ``viewCursor`` positions as records are
    inserted and revised, so a cursor is not a stable key. Prefer the item id
    plus its revision, then the source-range record id, and only fall back to
    the view cursor.
    """
    params = event.get("params") if isinstance(event.get("params"), dict) else {}
    item = params.get("item")
    if isinstance(item, dict):
        item_id = item.get("itemId") or item.get("id") or item.get("toolCallId")
        if item_id:
            revision = item.get("revision")
            status = item.get("status")
            return f"item:{item_id}:{revision}:{status}"
    source = params.get("sourceRange")
    if isinstance(source, dict):
        last = source.get("last")
        if isinstance(last, dict) and last.get("id"):
            return f"id:{last['id']}"
    view_cursor = params.get("viewCursor")
    if isinstance(view_cursor, str) and view_cursor:
        return f"vc:{view_cursor}"
    method = event.get("method") or event.get("kind") or "?"
    return f"{method}:{_stringify(params)}"


def _command_id_of(event: dict[str, Any]) -> str | None:
    """The submission command id a view record was caused by, if any.

    Turn records carry ``params.commandId``; item records carry it on
    ``params.item.commandId``. This is how a queued prompt is matched to
    the turn the daemon eventually starts for it.
    """
    params = event.get("params")
    if not isinstance(params, dict):
        return None
    command = params.get("commandId")
    if not command:
        item = params.get("item")
        if isinstance(item, dict):
            command = item.get("commandId")
    return str(command) if command else None


def _turn_id_of(event: dict[str, Any]) -> str | None:
    """The turn a view record belongs to, if the record names one.

    Turn records carry ``params.turnId``; item records carry it on
    ``params.item.turnId``. Session-level records (name, branch, token
    usage not scoped to a turn) return ``None`` and are never used for
    turn scoping.
    """
    params = event.get("params")
    if not isinstance(params, dict):
        return None
    turn = params.get("turnId")
    if not turn:
        item = params.get("item")
        if isinstance(item, dict):
            turn = item.get("turnId")
    return str(turn) if turn else None


def _view_cursor_of(event: dict[str, Any]) -> str | None:
    params = event.get("params")
    if not isinstance(params, dict):
        return None
    cursor = params.get("viewCursor")
    return str(cursor) if isinstance(cursor, str) and cursor else None


def _text_of(value: Any) -> str:
    """Coerce an item text/content field to a plain string."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        inner = value.get("text") or value.get("content")
        if isinstance(inner, str):
            return inner
    return ""


def _agent_chunk(text: Any) -> dict[str, Any]:
    if isinstance(text, dict):
        text = text.get("text") or text.get("content") or ""
    return {
        "sessionUpdate": "agent_message_chunk",
        "content": {"type": "text", "text": str(text or "")},
    }


def _thought_chunk(text: Any) -> dict[str, Any]:
    if isinstance(text, dict):
        text = text.get("text") or text.get("content") or ""
    return {
        "sessionUpdate": "agent_thought_chunk",
        "content": {"type": "text", "text": str(text or "")},
    }


def _acp_status(status: Any) -> str:
    value = str(status or "").lower()
    if value in ("completed", "complete", "done", "success", "succeeded"):
        return "completed"
    if value in ("failed", "failure", "error", "errored"):
        return "failed"
    if value in ("running", "in_progress", "active", "started"):
        return "in_progress"
    return "pending"


def _acp_tool_kind(kind: Any) -> str:
    value = str(kind or "").lower()
    if any(token in value for token in ("command", "shell", "bash", "exec")):
        return "execute"
    if any(token in value for token in ("edit", "write", "patch", "apply")):
        return "edit"
    if "read" in value:
        return "read"
    if any(token in value for token in ("search", "grep", "glob")):
        return "search"
    if any(token in value for token in ("fetch", "http", "web")):
        return "fetch"
    if any(token in value for token in ("delete", "remove", "rm")):
        return "delete"
    return "other"


def _acp_option_kind(choice_id: Any, label: Any) -> str:
    words = set(re.findall(r"[a-z]+", f"{choice_id or ''} {label or ''}".lower()))
    if words & {"always", "permanent"}:
        if words & {"deny", "reject", "block"}:
            return "reject_always"
        return "allow_always"
    if words & {"deny", "reject", "block", "no"}:
        return "reject_once"
    return "allow_once"


def _locations(item: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for source in (item.get("locations"), item.get("files")):
        if not isinstance(source, list):
            continue
        for entry in source:
            if isinstance(entry, str):
                out.append({"path": entry})
            elif isinstance(entry, dict):
                path = entry.get("path") or entry.get("file")
                if not path:
                    continue
                location: dict[str, Any] = {"path": str(path)}
                if isinstance(entry.get("line"), int):
                    location["line"] = entry["line"]
                out.append(location)
    return out


def _tool_call_from_approval(
    approval: dict[str, Any], request_id: str
) -> dict[str, Any]:
    subject = approval.get("subject")
    if not isinstance(subject, dict):
        subject = {}
    tool = (
        approval.get("toolName")
        or subject.get("command")
        or subject.get("kind")
        or "approval"
    )
    raw = approval.get("rawArgs")
    if raw:
        title = f"{tool}: {_one_line(_stringify(raw))}"
    elif subject.get("command"):
        title = f"{tool} {_one_line(_stringify(subject['command']))}"
    else:
        title = str(tool)
    tool_call: dict[str, Any] = {
        "toolCallId": str(request_id),
        "title": title,
        "kind": _acp_tool_kind(subject.get("kind") or approval.get("kind") or tool),
    }
    locations = _locations(approval)
    if locations:
        tool_call["locations"] = locations
    return tool_call


def _options_from_approval(approval: dict[str, Any]) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for choice in approval.get("availableChoices") or []:
        if isinstance(choice, str):
            choice = {"choiceId": choice, "label": choice}
        if not isinstance(choice, dict):
            continue
        choice_id = choice.get("choiceId") or choice.get("id")
        if choice_id is None:
            continue
        options.append(
            {
                "optionId": str(choice_id),
                "name": str(
                    choice.get("label") or choice.get("name") or choice_id
                ),
                "kind": _acp_option_kind(choice_id, choice.get("label")),
            }
        )
    if not options:
        options = [
            {"optionId": "allow", "name": "Allow", "kind": "allow_once"},
            {"optionId": "deny", "name": "Reject", "kind": "reject_once"},
        ]
    return options


def _question_text(user_input: dict[str, Any]) -> str:
    parts: list[str] = []
    for question in user_input.get("questions") or []:
        if isinstance(question, dict):
            parts.append(str(question.get("question") or question.get("prompt") or ""))
        elif isinstance(question, str):
            parts.append(question)
    parts = [part for part in parts if part]
    if parts:
        return "\n".join(parts)
    return str(user_input.get("toolName") or "The lane needs input.")


def _stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, separators=(",", ":"))
    except (TypeError, ValueError):
        return str(value)


def _one_line(text: str, limit: int = 120) -> str:
    compact = " ".join(str(text).split())
    if len(compact) > limit:
        return compact[: limit - 1] + "…"
    return compact
