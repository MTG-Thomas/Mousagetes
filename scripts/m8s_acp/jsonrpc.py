# SPDX-License-Identifier: AGPL-3.0-or-later
"""Minimal JSON-RPC 2.0 codec for ACP's newline-delimited framing.

ACP carries one JSON-RPC message per line on stdio. This module owns
encoding, decoding, and the message builders; it holds no session state,
so the real adapter and the transport-test stub share it unchanged.
"""

from __future__ import annotations

import json
from typing import Any

# JSON-RPC 2.0 reserved error codes.
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603


def encode(message: dict[str, Any]) -> str:
    """Encode one message as a newline-terminated JSON-RPC line."""
    return json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n"


def decode(line: str) -> dict[str, Any]:
    """Decode one JSON-RPC line. Raises ValueError on malformed input."""
    message = json.loads(line)
    if not isinstance(message, dict):
        raise ValueError("JSON-RPC message must be an object")
    return message


def result(request_id: Any, value: Any) -> dict[str, Any]:
    """Build a successful response."""
    return {"jsonrpc": "2.0", "id": request_id, "result": value}


def error(
    request_id: Any, code: int, message: str, data: Any = None
) -> dict[str, Any]:
    """Build an error response."""
    payload: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        payload["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": payload}


def notification(method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Build a notification (no id, no response expected)."""
    return {"jsonrpc": "2.0", "method": method, "params": params}


def request(request_id: Any, method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Build a server-initiated request (agent -> client)."""
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
