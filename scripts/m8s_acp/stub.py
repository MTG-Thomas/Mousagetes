# SPDX-License-Identifier: AGPL-3.0-or-later
"""A minimal ACP agent used to exercise transports before the real adapter.

This is not the m8s adapter. It implements just enough ACP — initialize,
session/new, session/load, session/resume, session/close, session/prompt
with streamed updates, session/cancel, and a server-initiated
``session/request_permission`` — for WS-C and WS-E to prove the websocket
bridge, token auth, TLS, and the phone client against a real stdio ACP
peer. WS-A replaces the session handling with daemon-backed mapping.

Wire reference: https://agentclientprotocol.com/protocol/prompt-turn
"""

from __future__ import annotations

import sys
from typing import Any, Iterable, TextIO

from . import __version__, jsonrpc

PROTOCOL_VERSION = 1

AGENT_INFO = {
    "name": "m8s-acp-stub",
    "title": "m8s ACP stub",
    "version": __version__,
}

# No filesystem or terminal capabilities: matches ADR 0003 and the real
# adapter's posture, so client behavior observed here carries over.
AGENT_CAPABILITIES = {
    "loadSession": True,
    "promptCapabilities": {"image": False, "audio": False, "embeddedContext": False},
    "mcpCapabilities": {"http": False, "sse": False},
    "sessionCapabilities": {"close": {}},
}

_PERMISSION_OPTIONS = [
    {"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
    {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"},
]


class LineReader:
    """Newline reader that skips blank lines and reports EOF as ``None``."""

    def __init__(self, stream: Iterable[str]) -> None:
        self._lines = iter(stream)

    def readline(self) -> str | None:
        for line in self._lines:
            stripped = line.strip()
            if stripped:
                return stripped
        return None


class StubAgent:
    """Deterministic ACP agent used as a transport test peer."""

    def __init__(self) -> None:
        self._sessions: set[str] = set()
        self._cancelled: set[str] = set()
        self._deferred: list[dict[str, Any]] = []
        self._session_seq = 0
        self._request_seq = 0

    # -- lifecycle --------------------------------------------------------

    def serve(self, reader: LineReader, out: TextIO) -> None:
        while True:
            line = reader.readline()
            if line is None:
                return
            try:
                message = jsonrpc.decode(line)
            except ValueError:
                self._write(out, jsonrpc.error(None, jsonrpc.PARSE_ERROR, "Parse error"))
                continue
            self._dispatch(message, reader, out)

    def _dispatch(
        self, message: dict[str, Any], reader: LineReader, out: TextIO
    ) -> None:
        method = message.get("method")
        if method is None:
            # A response to a server-initiated request nobody is waiting on.
            self._deferred.append(message)
            return
        params = message.get("params") or {}
        if not isinstance(params, dict):
            if "id" in message:
                self._write(
                    out,
                    jsonrpc.error(
                        message["id"], jsonrpc.INVALID_PARAMS, "params must be an object"
                    ),
                )
            return
        if "id" in message:
            self._handle_request(message["id"], method, params, reader, out)
        else:
            self._handle_notification(method, params)

    # -- requests ---------------------------------------------------------

    def _handle_request(
        self,
        request_id: Any,
        method: str,
        params: dict[str, Any],
        reader: LineReader,
        out: TextIO,
    ) -> None:
        if method == "initialize":
            self._write(out, jsonrpc.result(request_id, self._initialize_result()))
        elif method == "session/new":
            self._write(out, jsonrpc.result(request_id, self._new_session()))
        elif method in ("session/load", "session/resume"):
            self._load_session(params, out)
            self._write(out, jsonrpc.result(request_id, {}))
        elif method == "session/close":
            self._sessions.discard(str(params.get("sessionId")))
            self._write(out, jsonrpc.result(request_id, {}))
        elif method == "session/prompt":
            self._prompt(request_id, params, reader, out)
        else:
            self._write(
                out,
                jsonrpc.error(
                    request_id, jsonrpc.METHOD_NOT_FOUND, f"unknown method {method!r}"
                ),
            )

    def _handle_notification(self, method: str, params: dict[str, Any]) -> None:
        if method == "session/cancel":
            self._cancelled.add(str(params.get("sessionId")))
        # Other notifications are accepted and ignored.

    # -- ACP behaviours ---------------------------------------------------

    def _initialize_result(self) -> dict[str, Any]:
        return {
            "protocolVersion": PROTOCOL_VERSION,
            "agentCapabilities": AGENT_CAPABILITIES,
            "agentInfo": AGENT_INFO,
            "authMethods": [],
        }

    def _new_session(self) -> dict[str, Any]:
        self._session_seq += 1
        session_id = f"stub-{self._session_seq:04d}"
        self._sessions.add(session_id)
        return {"sessionId": session_id}

    def _load_session(self, params: dict[str, Any], out: TextIO) -> None:
        session_id = str(params.get("sessionId", ""))
        self._sessions.add(session_id)
        cwd = str(params.get("cwd", ""))
        self._update(
            out,
            session_id,
            {
                "sessionUpdate": "user_message_chunk",
                "content": {"type": "text", "text": f"stub history for {cwd or session_id}"},
            },
        )
        self._update(
            out,
            session_id,
            {
                "sessionUpdate": "agent_message_chunk",
                "messageId": f"{session_id}-history",
                "content": {"type": "text", "text": "Stub session restored."},
            },
        )

    def _prompt(
        self,
        request_id: Any,
        params: dict[str, Any],
        reader: LineReader,
        out: TextIO,
    ) -> None:
        session_id = str(params.get("sessionId", ""))
        self._sessions.add(session_id)
        text = _prompt_text(params.get("prompt"))
        self._update(
            out,
            session_id,
            {
                "sessionUpdate": "agent_message_chunk",
                "messageId": f"{session_id}-msg",
                "content": {"type": "text", "text": f"stub received: {text}"},
            },
        )
        if "needs-approval" in text:
            self._request_permission(session_id, reader, out)
        stop = "cancelled" if session_id in self._cancelled else "end_turn"
        self._cancelled.discard(session_id)
        self._write(out, jsonrpc.result(request_id, {"stopReason": stop}))

    def _request_permission(
        self, session_id: str, reader: LineReader, out: TextIO
    ) -> None:
        self._request_seq += 1
        request_id = f"stub-perm-{self._request_seq:04d}"
        tool_call_id = f"{session_id}-tool-{self._request_seq:04d}"
        self._update(
            out,
            session_id,
            {
                "sessionUpdate": "tool_call",
                "toolCallId": tool_call_id,
                "title": "Stub tool awaiting approval",
                "kind": "other",
                "status": "pending",
            },
        )
        self._write(
            out,
            jsonrpc.request(
                request_id,
                "session/request_permission",
                {
                    "sessionId": session_id,
                    "toolCall": {"toolCallId": tool_call_id},
                    "options": _PERMISSION_OPTIONS,
                },
            ),
        )
        response = self._await_response(request_id, reader, out)
        outcome = ((response or {}).get("result") or {}).get("outcome") or {}
        if outcome.get("outcome") == "selected" and outcome.get("optionId") == "allow-once":
            self._update(
                out,
                session_id,
                {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": tool_call_id,
                    "status": "completed",
                    "content": [
                        {
                            "type": "content",
                            "content": {"type": "text", "text": "approved by stub"},
                        }
                    ],
                },
            )
            return
        if response is None:
            # The client went away with the request unanswered: cancel the
            # turn rather than reporting a clean end.
            self._cancelled.add(session_id)
        self._update(
            out,
            session_id,
            {
                "sessionUpdate": "tool_call_update",
                "toolCallId": tool_call_id,
                "status": "failed",
            },
        )

    def _await_response(
        self, request_id: Any, reader: LineReader, out: TextIO
    ) -> dict[str, Any] | None:
        """Read until the matching response, handling interleaved messages."""
        while True:
            line = reader.readline()
            if line is None:
                return None
            try:
                message = jsonrpc.decode(line)
            except ValueError:
                self._write(out, jsonrpc.error(None, jsonrpc.PARSE_ERROR, "Parse error"))
                continue
            if message.get("id") == request_id and "method" not in message:
                return message
            self._dispatch(message, reader, out)

    # -- helpers ----------------------------------------------------------

    def _update(self, out: TextIO, session_id: str, update: dict[str, Any]) -> None:
        self._write(
            out,
            jsonrpc.notification(
                "session/update", {"sessionId": session_id, "update": update}
            ),
        )

    @staticmethod
    def _write(out: TextIO, message: dict[str, Any]) -> None:
        out.write(jsonrpc.encode(message))
        out.flush()


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


def serve_stdio(stdin: TextIO | None = None, stdout: TextIO | None = None) -> None:
    """Run the stub over stdio (the standard ACP local transport)."""
    in_stream = stdin if stdin is not None else sys.stdin
    out_stream = stdout if stdout is not None else sys.stdout
    StubAgent().serve(LineReader(in_stream), out_stream)
