#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for the m8s ACP adapter scaffold (framing + transport stub).

Stdlib unittest only. Never starts a daemon, never binds a port, never
touches the network: the stub is driven over in-memory streams.
"""

from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from m8s_acp import jsonrpc  # noqa: E402
from m8s_acp.stub import LineReader, serve_stdio  # noqa: E402


def request(method: str, params: dict, request_id: int) -> dict:
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}


def notification(method: str, params: dict) -> dict:
    return {"jsonrpc": "2.0", "method": method, "params": params}


def response(request_id, result: dict) -> dict:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def run_stub(*messages: dict) -> list[dict]:
    stdin = io.StringIO("\n".join(json.dumps(m) for m in messages) + "\n")
    stdout = io.StringIO()
    serve_stdio(stdin, stdout)
    return [json.loads(line) for line in stdout.getvalue().splitlines() if line.strip()]


def by_id(messages: list[dict], request_id) -> dict:
    for message in messages:
        if message.get("id") == request_id:
            return message
    raise AssertionError(f"no message with id {request_id!r}: {messages}")


def updates_for(messages: list[dict], session_id: str) -> list[dict]:
    return [
        m["params"]["update"]
        for m in messages
        if m.get("method") == "session/update"
        and m.get("params", {}).get("sessionId") == session_id
    ]


class FramingTests(unittest.TestCase):
    def test_encode_decode_roundtrip(self) -> None:
        message = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        self.assertEqual(jsonrpc.decode(jsonrpc.encode(message)), message)
        self.assertTrue(jsonrpc.encode(message).endswith("\n"))

    def test_decode_rejects_non_object(self) -> None:
        with self.assertRaises(ValueError):
            jsonrpc.decode("[1, 2, 3]")

    def test_error_helper_omits_null_data(self) -> None:
        self.assertNotIn("data", jsonrpc.error(1, -32601, "nope")["error"])
        self.assertEqual(jsonrpc.error(1, -32601, "nope", {"x": 1})["error"]["data"], {"x": 1})

    def test_notification_has_no_id(self) -> None:
        built = jsonrpc.notification("session/update", {"sessionId": "s"})
        self.assertNotIn("id", built)

    def test_request_has_id_and_method(self) -> None:
        built = jsonrpc.request("r1", "session/request_permission", {})
        self.assertEqual(built["id"], "r1")
        self.assertEqual(built["method"], "session/request_permission")

    def test_line_reader_skips_blanks_and_reports_eof(self) -> None:
        reader = LineReader(["", "  \n", "a\n", "b\n"])
        self.assertEqual(reader.readline(), "a")
        self.assertEqual(reader.readline(), "b")
        self.assertIsNone(reader.readline())


class StubHandshakeTests(unittest.TestCase):
    def test_initialize_advertises_no_fs_or_terminal(self) -> None:
        messages = run_stub(request("initialize", {"protocolVersion": 1}, 1))
        result = by_id(messages, 1)["result"]
        self.assertEqual(result["protocolVersion"], 1)
        capabilities = result["agentCapabilities"]
        self.assertTrue(capabilities["loadSession"])
        self.assertNotIn("fs", capabilities)
        self.assertNotIn("terminal", capabilities)
        self.assertFalse(capabilities["promptCapabilities"]["image"])

    def test_new_session_returns_stable_id(self) -> None:
        messages = run_stub(request("session/new", {"cwd": "/tmp/w", "mcpServers": []}, 1))
        self.assertEqual(by_id(messages, 1)["result"]["sessionId"], "stub-0001")

    def test_unknown_method_is_method_not_found(self) -> None:
        messages = run_stub(request("session/nope", {}, 1))
        self.assertEqual(by_id(messages, 1)["error"]["code"], jsonrpc.METHOD_NOT_FOUND)


class StubPromptTests(unittest.TestCase):
    def test_prompt_streams_update_and_ends_turn(self) -> None:
        messages = run_stub(
            request("session/new", {"cwd": "/tmp/w", "mcpServers": []}, 1),
            request(
                "session/prompt",
                {"sessionId": "stub-0001", "prompt": [{"type": "text", "text": "hello"}]},
                2,
            ),
        )
        updates = updates_for(messages, "stub-0001")
        self.assertTrue(any(u["sessionUpdate"] == "agent_message_chunk" for u in updates))
        self.assertEqual(by_id(messages, 2)["result"]["stopReason"], "end_turn")

    def test_unknown_session_id_is_echoed_not_rejected(self) -> None:
        # The stub is a transport peer: it accepts any session id so bridge
        # tests do not need a prior session/new.
        messages = run_stub(
            request(
                "session/prompt",
                {"sessionId": "x", "prompt": [{"type": "text", "text": "hi"}]},
                1,
            )
        )
        self.assertEqual(by_id(messages, 1)["result"]["stopReason"], "end_turn")

    def test_permission_request_roundtrip_allow(self) -> None:
        messages = run_stub(
            request(
                "session/prompt",
                {
                    "sessionId": "s",
                    "prompt": [{"type": "text", "text": "please needs-approval now"}],
                },
                1,
            ),
            response(
                "stub-perm-0001",
                {"outcome": {"outcome": "selected", "optionId": "allow-once"}},
            ),
        )
        permission = by_id(messages, "stub-perm-0001")
        self.assertEqual(permission["method"], "session/request_permission")
        options = permission["params"]["options"]
        self.assertEqual([o["optionId"] for o in options], ["allow-once", "reject-once"])
        updates = updates_for(messages, "s")
        self.assertTrue(
            any(
                u["sessionUpdate"] == "tool_call_update" and u["status"] == "completed"
                for u in updates
            )
        )
        self.assertEqual(by_id(messages, 1)["result"]["stopReason"], "end_turn")

    def test_missing_permission_response_ends_cancelled(self) -> None:
        messages = run_stub(
            request(
                "session/prompt",
                {"sessionId": "s", "prompt": [{"type": "text", "text": "needs-approval"}]},
                1,
            )
        )
        self.assertEqual(by_id(messages, 1)["result"]["stopReason"], "cancelled")

    def test_cancel_during_permission_ends_cancelled(self) -> None:
        messages = run_stub(
            request(
                "session/prompt",
                {"sessionId": "s", "prompt": [{"type": "text", "text": "needs-approval"}]},
                1,
            ),
            notification("session/cancel", {"sessionId": "s"}),
            response("stub-perm-0001", {"outcome": {"outcome": "cancelled"}}),
        )
        self.assertEqual(by_id(messages, 1)["result"]["stopReason"], "cancelled")

    def test_load_replays_history_before_responding(self) -> None:
        messages = run_stub(
            request("session/load", {"sessionId": "s", "cwd": "/tmp/w", "mcpServers": []}, 1)
        )
        # Replay notifications must precede the load response.
        replay_index = next(
            i for i, m in enumerate(messages) if m.get("method") == "session/update"
        )
        response_index = next(i for i, m in enumerate(messages) if m.get("id") == 1)
        self.assertLess(replay_index, response_index)
        self.assertEqual(by_id(messages, 1)["result"], {})


if __name__ == "__main__":
    unittest.main()
