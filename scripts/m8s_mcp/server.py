# SPDX-License-Identifier: AGPL-3.0-or-later
"""MCP stdio server for the m8s Codex bridge (stdlib only).

Newline-delimited JSON-RPC 2.0 on stdin/stdout — the transport Codex MCP
clients speak over SSH. Lifecycle: ``initialize``,
``notifications/initialized``, ``tools/list``, ``tools/call`` (plus
``ping``). Tool failures are MCP ``isError`` results, never silent drops.

The server advertises ``capabilities: {"tools": {}}`` and nothing else: no
resources, no prompts, no roots, and no filesystem or terminal proxying
(ADR 0003 applies to this surface too — the client steers lanes with
prompts and approvals and reads rendered output only).
"""

from __future__ import annotations

import json
import sys
from typing import Any, TextIO

from . import __version__
from .adapter import TOOLS, Adapter, McpAdapterError

PROTOCOL_VERSION = "2024-11-05"

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

SERVER_INFO = {"name": "m8s-mcp", "version": __version__}

TOOL_DESCRIPTIONS = {
    "health": "Check the m8s daemon and this adapter (versions, task counts, modes).",
    "models": "List models available on the m8s host.",
    "sessions": "Lane metadata across both hosts without taking any writer lease.",
    "session_read": "Point-in-time snapshot of a lane from either host; never resumes or steers it.",
    "start": (
        "Start an asynchronous lane task. read_only runs on the enforced "
        "host or fails closed; worktree is the explicit YOLO selection "
        "(isolated worktree at the exact committed ref). Always supply "
        "requestId; repeat it only after uncertain submission."
    ),
    "tasks": "List tasks owned by this adapter.",
    "status": "Task progress, live preview, and exact pending approvals.",
    "result": "Terminal evidence bound to the admitted turn; completed is success.",
    "resume": "Explicit follow-up in the adapter-owned idle lane; never automatic.",
    "approve": "Answer the exact offered approval triple; no persistent grants.",
    "cancel": "Cancel the adapter-owned task's admitted turn; terminal proof via status.",
    "changes": "Git status and bounded diff of the task workspace; never commits.",
}

TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "health": {"type": "object", "properties": {}, "additionalProperties": False},
    "models": {"type": "object", "properties": {}, "additionalProperties": False},
    "sessions": {
        "type": "object",
        "properties": {
            "workspace": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
            "cursor": {"type": "string"},
        },
        "additionalProperties": False,
    },
    "session_read": {
        "type": "object",
        "properties": {"sessionId": {"type": "string"}, "includeItems": {"type": "boolean"}},
        "required": ["sessionId"],
        "additionalProperties": False,
    },
    "start": {
        "type": "object",
        "properties": {
            "workspace": {"type": "string"},
            "prompt": {"type": "string", "minLength": 1, "maxLength": 100000},
            "mode": {"type": "string", "enum": ["read_only", "worktree"]},
            "model": {"type": "string"},
            "ref": {"type": "string"},
            "requestId": {"type": "string", "format": "uuid"},
        },
        "required": ["workspace", "prompt"],
        "additionalProperties": False,
    },
    "tasks": {"type": "object", "properties": {}, "additionalProperties": False},
    "status": {
        "type": "object",
        "properties": {"taskId": {"type": "string", "format": "uuid"}},
        "required": ["taskId"],
        "additionalProperties": False,
    },
    "result": {
        "type": "object",
        "properties": {"taskId": {"type": "string", "format": "uuid"}},
        "required": ["taskId"],
        "additionalProperties": False,
    },
    "resume": {
        "type": "object",
        "properties": {
            "taskId": {"type": "string", "format": "uuid"},
            "prompt": {"type": "string", "minLength": 1, "maxLength": 100000},
        },
        "required": ["taskId", "prompt"],
        "additionalProperties": False,
    },
    "approve": {
        "type": "object",
        "properties": {
            "taskId": {"type": "string", "format": "uuid"},
            "approvalId": {"type": "string"},
            "requirementId": {},
            "choiceId": {"type": "string"},
        },
        "required": ["taskId", "approvalId", "requirementId", "choiceId"],
        "additionalProperties": False,
    },
    "cancel": {
        "type": "object",
        "properties": {"taskId": {"type": "string", "format": "uuid"}},
        "required": ["taskId"],
        "additionalProperties": False,
    },
    "changes": {
        "type": "object",
        "properties": {"taskId": {"type": "string", "format": "uuid"}},
        "required": ["taskId"],
        "additionalProperties": False,
    },
}


def _encode(message: dict[str, Any]) -> str:
    return json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n"


def _result(request_id: Any, value: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": value}


def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _tool_result(value: Any) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(value, ensure_ascii=False)}]}


def _tool_error(message: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": message}], "isError": True}


class Server:
    """One MCP session over stdio. Holds no lane state (the Adapter owns tasks)."""

    def __init__(self, adapter: Adapter, stdin: TextIO, stdout: TextIO) -> None:
        self._adapter = adapter
        self._stdin = stdin
        self._stdout = stdout
        self._initialized = False

    def serve(self) -> None:
        while True:
            line = self._stdin.readline()
            if line == "":
                return
            line = line.strip()
            if not line:
                continue
            self._handle_line(line)

    def _handle_line(self, line: str) -> None:
        try:
            message = json.loads(line)
        except ValueError:
            self._send(_error(None, PARSE_ERROR, "Parse error"))
            return
        if not isinstance(message, dict):
            self._send(_error(None, INVALID_REQUEST, "Invalid Request"))
            return
        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params") or {}
        if not isinstance(params, dict):
            if request_id is not None:
                self._send(_error(request_id, INVALID_PARAMS, "params must be an object"))
            return
        if method is None or request_id is None:
            # Notifications (initialized) and responses carry no reply.
            if method == "notifications/initialized":
                self._initialized = True
            return
        if method == "initialize":
            self._send(_result(request_id, self._initialize()))
        elif method == "ping":
            self._send(_result(request_id, {}))
        elif method == "tools/list":
            self._send(_result(request_id, {"tools": self._tool_list()}))
        elif method == "tools/call":
            self._send(_result(request_id, self._tools_call(params)))
        else:
            self._send(_error(request_id, METHOD_NOT_FOUND, f"unknown method {method!r}"))

    def _initialize(self) -> dict[str, Any]:
        self._initialized = True
        return {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": SERVER_INFO,
        }

    def _tool_list(self) -> list[dict[str, Any]]:
        return [
            {
                "name": name,
                "description": TOOL_DESCRIPTIONS[name],
                "inputSchema": TOOL_SCHEMAS[name],
            }
            for name in TOOLS
        ]

    def _tools_call(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        args = params.get("arguments") or {}
        if not isinstance(name, str) or not isinstance(args, dict):
            return _tool_error("INVALID_PARAMS: name must be a string, arguments an object")
        if name not in TOOLS:
            return _tool_error(f"unknownTool: unknown tool {name!r}")
        try:
            return _tool_result(self._adapter.dispatch(name, args))
        except McpAdapterError as exc:
            return _tool_error(str(exc))

    def _send(self, message: dict[str, Any]) -> None:
        self._stdout.write(_encode(message))
        self._stdout.flush()


def serve_stdio(
    adapter: Adapter,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
) -> None:
    """Run the MCP adapter over stdio (the Codex-over-SSH transport)."""
    Server(adapter, stdin if stdin is not None else sys.stdin, stdout if stdout is not None else sys.stdout).serve()
