# SPDX-License-Identifier: AGPL-3.0-or-later
"""Real ACP stdio server loop over a daemon-backed ``LaneMapping``.

This is WS-A's protocol core: it owns newline JSON-RPC framing, the
``initialize`` handshake, the session registry verbs, and the event pump
that turns :class:`~m8s_acp.contract.MappingEvent` values into ACP
messages. It never imports the daemon mapping and holds no lane state;
all lane work goes through the frozen
:class:`~m8s_acp.contract.LaneMapping` seam (ADR 0002, ADR 0006, ADR
0008).

Wire reference: https://agentclientprotocol.com/protocol/prompt-turn
"""

from __future__ import annotations

import io
import json
import os
import select
import sys
from typing import Any, TextIO

from . import __version__, contract, jsonrpc

PROTOCOL_VERSION = 1

AGENT_INFO = {
    "name": "m8s-acp",
    "title": "m8s ACP adapter",
    "version": __version__,
}

# No filesystem or terminal capabilities: matches ADR 0003. ``loadSession``
# is required by R6; ``authMethods`` is advertised empty on the connection.
AGENT_CAPABILITIES = {
    "loadSession": True,
    "promptCapabilities": {"image": False, "audio": False, "embeddedContext": False},
    "mcpCapabilities": {"http": False, "sse": False},
}

# R11: the reference client only renders these update kinds.
RENDERABLE_UPDATES = frozenset(
    {
        "user_message_chunk",
        "agent_message_chunk",
        "agent_thought_chunk",
        "tool_call",
        "tool_call_update",
        "available_commands_update",
        "current_mode_update",
    }
)

_DEFAULT_QUESTION_SCHEMA = {
    "type": "object",
    "properties": {"response": {"type": "string", "title": "Response"}},
    "required": ["response"],
}


class LineSource:
    """Newline reader with a non-blocking peek used by the event pump."""

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def readline(self) -> str | None:
        while True:
            line = self._stream.readline()
            if line == "":
                return None
            line = line.strip()
            if line:
                return line

    def read_available(self) -> list[str]:
        lines: list[str] = []
        while self._available():
            line = self.readline()
            if line is None:
                break
            lines.append(line)
        return lines

    def _available(self) -> bool:
        stream = self._stream
        try:
            fileno = stream.fileno()
        except (AttributeError, OSError, io.UnsupportedOperation):
            fileno = None
        if fileno is not None:
            try:
                ready, _, _ = select.select([fileno], [], [], 0)
            except (OSError, ValueError):
                return False
            return bool(ready)
        getvalue = getattr(stream, "getvalue", None)
        tell = getattr(stream, "tell", None)
        if callable(getvalue) and callable(tell):
            try:
                return tell() < len(getvalue())
            except (OSError, ValueError):
                return False
        return False


class Agent:
    """Stateless ACP agent translating the wire protocol to a mapping."""

    def __init__(
        self,
        mapping: contract.LaneMapping,
        reader: LineSource,
        out: TextIO,
    ) -> None:
        self._mapping = mapping
        self._reader = reader
        self._out = out
        self._responses: dict[Any, dict[str, Any]] = {}
        self._deferred: list[tuple[Any, str, dict[str, Any]]] = []
        self._active_prompts: set[str] = set()
        self._server_seq = 0

    # -- lifecycle --------------------------------------------------------

    def serve(self) -> None:
        while True:
            line = self._reader.readline()
            if line is None:
                return
            self._receive(line)
            self._drain_deferred()

    def _receive(self, line: str) -> None:
        try:
            message = jsonrpc.decode(line)
        except ValueError:
            self._send(jsonrpc.error(None, jsonrpc.PARSE_ERROR, "Parse error"))
            return
        self._dispatch(message)

    def _dispatch(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        if method is None:
            # A response to a server-initiated request, or the bridge's
            # non-ACP ``{"type":"connected"}`` control frame (R16).
            if "id" in message:
                self._responses[message["id"]] = message
            return
        params = message.get("params")
        if not isinstance(params, dict):
            if "id" in message:
                self._send(
                    jsonrpc.error(
                        message["id"],
                        jsonrpc.INVALID_PARAMS,
                        "params must be an object",
                    )
                )
            return
        if "id" in message:
            lane_id = str(params.get("sessionId"))
            if method == "session/prompt" and lane_id in self._active_prompts:
                self._deferred.append((message["id"], method, params))
            else:
                self._handle_request(message["id"], method, params)
        else:
            self._handle_notification(method, params)

    def _drain_deferred(self) -> None:
        while self._deferred:
            request_id, method, params = self._deferred.pop(0)
            self._handle_request(request_id, method, params)

    def _pump(self) -> None:
        for line in self._reader.read_available():
            self._receive(line)

    # -- requests ---------------------------------------------------------

    def _handle_request(
        self, request_id: Any, method: str, params: dict[str, Any]
    ) -> None:
        try:
            if method == "initialize":
                self._send(jsonrpc.result(request_id, self._initialize_result()))
            elif method == "session/new":
                self._session_new(request_id, params)
            elif method in ("session/load", "session/resume"):
                self._session_load(request_id, params)
            elif method == "session/prompt":
                self._session_prompt(request_id, params)
            elif method == "session/set_mode":
                self._set_control(params, "approval", "modeId")
                self._send(jsonrpc.result(request_id, {}))
            elif method == "session/set_model":
                self._set_control(params, "model", "modelId")
                self._send(jsonrpc.result(request_id, {}))
            else:
                self._send(
                    jsonrpc.error(
                        request_id,
                        jsonrpc.METHOD_NOT_FOUND,
                        f"unknown method {method!r}",
                    )
                )
        except Exception as exc:
            self._send(jsonrpc.error(request_id, jsonrpc.INTERNAL_ERROR, str(exc)))

    def _handle_notification(self, method: str, params: dict[str, Any]) -> None:
        if method == "session/cancel":
            self._mapping.cancel(str(params.get("sessionId") or ""))

    # -- ACP behaviours ---------------------------------------------------

    def _initialize_result(self) -> dict[str, Any]:
        return {
            "protocolVersion": PROTOCOL_VERSION,
            "agentCapabilities": AGENT_CAPABILITIES,
            "agentInfo": AGENT_INFO,
            "authMethods": [],
        }

    def _session_new(self, request_id: Any, params: dict[str, Any]) -> None:
        cwd = str(params.get("cwd") or "")
        title = str(
            params.get("title") or os.path.basename(cwd.rstrip("/")) or "lane"
        )
        lane_id = self._mapping.launch_lane(cwd, title)
        result = {
            "sessionId": lane_id,
            "modes": self._mapping.modes(lane_id),
            "models": self._mapping.models(lane_id),
        }
        commands = self._mapping.commands()
        self._send(jsonrpc.result(request_id, result))
        self._send_commands(lane_id, commands)

    def _session_load(self, request_id: Any, params: dict[str, Any]) -> None:
        lane_id = str(params.get("sessionId") or "")
        cwd = str(params.get("cwd") or "")
        # R7: history is replayed before the load response.
        updates = self._mapping.resume_lane(lane_id, cwd)
        for update in updates:
            self._emit_update(lane_id, update)
        result = {
            "sessionId": lane_id,
            "modes": self._mapping.modes(lane_id),
            "models": self._mapping.models(lane_id),
        }
        commands = self._mapping.commands()
        self._send(jsonrpc.result(request_id, result))
        self._send_commands(lane_id, commands)

    def _session_prompt(self, request_id: Any, params: dict[str, Any]) -> None:
        lane_id = str(params.get("sessionId") or "")
        text = _prompt_text(params.get("prompt"))
        stop_reason = "end_turn"
        self._active_prompts.add(lane_id)
        try:
            for event in self._mapping.prompt(lane_id, text):
                if event.kind == contract.UPDATE:
                    self._emit_update(lane_id, _update_payload(event.data))
                elif event.kind == contract.PERMISSION:
                    if not self._relay_permission(lane_id, event.data):
                        self._mapping.cancel(lane_id)
                        stop_reason = "cancelled"
                        break
                elif event.kind == contract.QUESTION:
                    if not self._relay_question(lane_id, event.data):
                        self._mapping.cancel(lane_id)
                        stop_reason = "cancelled"
                        break
                elif event.kind == contract.STOP:
                    stop_reason = str(event.data.get("stopReason") or "end_turn")
                self._pump()
        finally:
            self._active_prompts.discard(lane_id)
        self._send(jsonrpc.result(request_id, {"stopReason": stop_reason}))

    def _relay_permission(self, lane_id: str, data: dict[str, Any]) -> bool:
        request_id = self._next_request_id()
        request = {
            "sessionId": lane_id,
            "toolCall": data.get("toolCall") or {},
            "options": data.get("options") or [],
        }
        self._send(
            jsonrpc.request(request_id, "session/request_permission", request)
        )
        response = self._await_response(request_id)
        if response is None:
            return False
        outcome = (response.get("result") or {}).get("outcome") or {}
        if outcome.get("outcome") == "selected":
            option_id = str(outcome.get("optionId") or "cancelled")
        else:
            option_id = "cancelled"
        self._mapping.answer_permission(
            lane_id, str(data.get("requestId") or request_id), option_id
        )
        return True

    def _relay_question(self, lane_id: str, data: dict[str, Any]) -> bool:
        request_id = self._next_request_id()
        params = {
            "sessionId": lane_id,
            "mode": "form",
            "message": str(data.get("message") or ""),
            "requestedSchema": data.get("requestedSchema") or _DEFAULT_QUESTION_SCHEMA,
        }
        self._send(jsonrpc.request(request_id, "elicitation/create", params))
        response = self._await_response(request_id)
        if response is None:
            return False
        result = response.get("result") or {}
        text = (
            _content_text(result.get("content"))
            if result.get("action") == "accept"
            else ""
        )
        self._mapping.answer_question(
            lane_id, str(data.get("requestId") or request_id), text
        )
        return True

    def _await_response(self, request_id: Any) -> dict[str, Any] | None:
        while True:
            if request_id in self._responses:
                return self._responses.pop(request_id)
            line = self._reader.readline()
            if line is None:
                return None
            self._receive(line)

    # -- controls and helpers --------------------------------------------

    def _set_control(
        self, params: dict[str, Any], name: str, value_key: str
    ) -> None:
        lane_id = str(params.get("sessionId") or "")
        value = str(params.get(value_key) or "")
        self._mapping.set_control(lane_id, name, value)

    def _emit_update(self, lane_id: str, update: dict[str, Any]) -> None:
        if not isinstance(update, dict):
            return
        if update.get("sessionUpdate") not in RENDERABLE_UPDATES:
            return
        self._send(
            jsonrpc.notification(
                "session/update", {"sessionId": lane_id, "update": update}
            )
        )

    def _send_commands(self, lane_id: str, commands: list[dict[str, Any]]) -> None:
        self._emit_update(
            lane_id,
            {
                "sessionUpdate": "available_commands_update",
                "availableCommands": commands,
            },
        )

    def _next_request_id(self) -> str:
        self._server_seq += 1
        return f"m8s-{self._server_seq}"

    def _send(self, message: dict[str, Any]) -> None:
        self._out.write(jsonrpc.encode(message))
        self._out.flush()


def serve_stdio(
    mapping: contract.LaneMapping,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
) -> None:
    """Run the daemon-backed adapter over stdio (the ACP local transport)."""
    in_stream = stdin if stdin is not None else sys.stdin
    out_stream = stdout if stdout is not None else sys.stdout
    Agent(mapping, LineSource(in_stream), out_stream).serve()


def _update_payload(data: dict[str, Any]) -> dict[str, Any]:
    if isinstance(data, dict):
        update = data.get("update")
        if isinstance(update, dict):
            return update
        return data
    return {}


def _prompt_text(prompt: Any) -> str:
    if not isinstance(prompt, list):
        return ""
    parts: list[str] = []
    for block in prompt:
        if (
            isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
        ):
            parts.append(block["text"])
    return " ".join(parts)


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        for key in ("response", "text", "answer", "value"):
            value = content.get(key)
            if isinstance(value, str):
                return value
        if len(content) == 1:
            value = next(iter(content.values()))
            if isinstance(value, str):
                return value
        return json.dumps(content, ensure_ascii=False)
    return ""
