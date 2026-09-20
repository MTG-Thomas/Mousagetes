#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Integration test: real ACP transport + real daemon mapping + fake daemon.

Closes the gap the unit suites leave: ``test_acp_agent`` drives the transport
with a fake mapping, and ``test_acp_mapping`` drives the mapping with a fake
daemon, but nothing wires both real halves together. This does, over in-memory
streams, with no daemon, no socket, and no model turns (so it is unaffected by
provider quota).
"""

from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from m8s_acp.agent import serve_stdio  # noqa: E402
from m8s_acp.mapping import DaemonMapping  # noqa: E402


def turn_event(
    method: str, record_id: str, cursor: str, turn_id: str, terminal: str | None = None
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "turnId": turn_id,
        "viewCursor": cursor,
        "sourceRange": {"last": {"id": record_id}},
    }
    if terminal is not None:
        params["terminal"] = terminal
        params["durationMs"] = 1
    return {"method": method, "params": params}


def item_event(
    record_id: str, cursor: str, item_id: str, kind: str, text: str, turn_id: str
) -> dict[str, Any]:
    item = {
        "id": item_id,
        "itemId": item_id,
        "kind": kind,
        "text": text,
        "turnId": turn_id,
        "revision": 1,
        "status": "completed",
    }
    return {
        "method": "item/completed",
        "params": {
            "item": item,
            "viewCursor": cursor,
            "sourceRange": {"last": {"id": record_id}},
        },
    }


class FakeControl:
    """In-memory daemon: the few verbs DaemonMapping uses for one prompt."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.sent = False
        self.pages_after_send = 0

    def request(self, request: dict[str, Any]) -> Any:
        self.calls.append(request)
        command = request.get("command")
        if command == "launch":
            return {"session": {"sessionId": "s1"}}
        if command == "list":
            return {
                "sessions": [
                    {
                        "sessionId": "s1",
                        "alias": "lane-1",
                        "workspace": "/w",
                        "status": "idle",
                        "modelId": "m1",
                        "approvalMode": "allowAll",
                        "activeTurnId": "t0",
                    }
                ]
            }
        if command == "model/list":
            return {"models": [{"modelId": "m1", "name": "m1"}]}
        if command == "pending":
            return {"approvals": [], "userInputs": []}
        if command == "send":
            self.sent = True
            return {"turnId": "t1", "startedNewTurn": True, "disposition": "started"}
        if command == "call" and request.get("method") == "view/page":
            return self._page()
        return {}

    def _page(self) -> dict[str, Any]:
        if not self.sent:
            # The synthetic brief turn, consumed by priming.
            return {
                "events": [
                    turn_event("turn/started", "r1", "v:1", "t0"),
                    turn_event("turn/completed", "r2", "v:2", "t0", "completed"),
                ],
                "nextCursor": None,
            }
        if self.pages_after_send == 0:
            self.pages_after_send += 1
            return {
                "events": [
                    turn_event("turn/started", "r3", "v:3", "t1"),
                    item_event("r4", "v:4", "i1", "agentMessage", "E2E-OK", "t1"),
                    turn_event("turn/completed", "r5", "v:5", "t1", "completed"),
                ],
                "nextCursor": None,
            }
        return {"events": [], "nextCursor": None}


class AgentMappingIntegrationTest(unittest.TestCase):
    def run_agent(self, *messages: dict[str, Any]) -> tuple[list[dict], FakeControl]:
        control = FakeControl()
        mapping = DaemonMapping(control, poll_interval=0, sleep=lambda _s: None)
        stdin = io.StringIO("\n".join(json.dumps(m) for m in messages) + "\n")
        stdout = io.StringIO()
        serve_stdio(mapping, stdin, stdout)
        parsed = [
            json.loads(line)
            for line in stdout.getvalue().splitlines()
            if line.strip()
        ]
        return parsed, control

    def test_prompt_streams_and_stops_through_both_halves(self) -> None:
        out, control = self.run_agent(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": 1}},
            {"jsonrpc": "2.0", "id": 2, "method": "session/new", "params": {"cwd": "/w", "mcpServers": []}},
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {"sessionId": "s1", "prompt": [{"type": "text", "text": "hi"}]},
            },
        )
        responses = {m["id"]: m for m in out if "id" in m and "method" not in m}

        self.assertEqual(responses[1]["result"]["protocolVersion"], 1)
        new = responses[2]["result"]
        self.assertEqual(new["sessionId"], "s1")
        self.assertIn("modes", new)
        self.assertIn("models", new)
        self.assertEqual(responses[3]["result"]["stopReason"], "end_turn")

        chunks = [
            m["params"]["update"]
            for m in out
            if m.get("method") == "session/update"
            and m["params"]["update"].get("sessionUpdate") == "agent_message_chunk"
        ]
        self.assertTrue(chunks, "expected at least one agent_message_chunk")
        self.assertEqual(
            "".join(c["content"]["text"] for c in chunks), "E2E-OK"
        )

        # The synthetic brief turn must not leak or terminate the prompt.
        self.assertTrue(
            any(
                m.get("method") == "session/update"
                and m["params"]["update"].get("sessionUpdate")
                == "available_commands_update"
                for m in out
            )
        )
        # Sanity: the mapping really went through the daemon for the prompt.
        self.assertIn("send", [c.get("command") for c in control.calls])


if __name__ == "__main__":
    unittest.main()
