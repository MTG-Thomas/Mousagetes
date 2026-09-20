#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for the real ACP protocol core (``m8s_acp.agent``).

Stdlib unittest only, driven over in-memory streams with a fake in-memory
``LaneMapping``: no daemon, no sockets, no network. Covers R1, R2, R6, R7,
R10, R11, and R16 from ``docs/acp-client-eval.md``.
"""

from __future__ import annotations

import argparse
import ast
import io
import json
import sys
import unittest
from pathlib import Path
from typing import Any, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from m8s_acp import agent as agent_module  # noqa: E402
from m8s_acp import cli, contract  # noqa: E402
from m8s_acp.agent import serve_stdio  # noqa: E402


def request(method: str, params: dict, request_id: int) -> dict:
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}


def notification(method: str, params: dict) -> dict:
    return {"jsonrpc": "2.0", "method": method, "params": params}


def response(request_id, result: dict) -> dict:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


class FakeMapping:
    """In-memory LaneMapping for transport tests; records every call."""

    def __init__(self) -> None:
        self.lane = "lane-1"
        self.launched: list[tuple[str, str]] = []
        self.resumed: list[tuple[str, str]] = []
        self.prompt_calls: list[tuple[str, str]] = []
        self.cancelled: list[str] = []
        self.permission_answers: list[tuple[str, str, str]] = []
        self.question_answers: list[tuple[str, str, str]] = []
        self.controls: list[tuple[str, str, str]] = []
        self.replay: list[dict[str, Any]] = []
        self.prompt_script: list[contract.MappingEvent] = []
        self.command_list: list[dict[str, Any]] = [
            {"name": "model", "description": "Set model", "input": {"hint": "<id>"}}
        ]
        self.mode_payload = {
            "currentModeId": "default",
            "availableModes": [
                {"id": "default", "name": "Default", "description": "d"}
            ],
        }
        self.model_payload = {
            "currentModelId": "muse",
            "availableModels": [
                {"modelId": "muse", "name": "Muse", "description": "d"}
            ],
        }

    def list_lanes(self) -> list[dict[str, Any]]:
        return [{"laneId": self.lane, "title": "t"}]

    def launch_lane(self, cwd: str, title: str) -> str:
        self.launched.append((cwd, title))
        return self.lane

    def resume_lane(self, lane_id: str, cwd: str) -> list[dict[str, Any]]:
        self.resumed.append((lane_id, cwd))
        return list(self.replay)

    def prompt(self, lane_id: str, text: str) -> Iterator[contract.MappingEvent]:
        self.prompt_calls.append((lane_id, text))
        yield from self.prompt_script

    def cancel(self, lane_id: str) -> None:
        self.cancelled.append(lane_id)

    def answer_permission(
        self, lane_id: str, request_id: str, option_id: str
    ) -> None:
        self.permission_answers.append((lane_id, request_id, option_id))

    def answer_question(self, lane_id: str, request_id: str, text: str) -> None:
        self.question_answers.append((lane_id, request_id, text))

    def set_control(self, lane_id: str, name: str, value: str) -> None:
        self.controls.append((lane_id, name, value))

    def commands(self) -> list[dict[str, Any]]:
        return list(self.command_list)

    def modes(self, lane_id: str) -> dict[str, Any]:
        return dict(self.mode_payload)

    def models(self, lane_id: str) -> dict[str, Any]:
        return dict(self.model_payload)


def run_agent(mapping: FakeMapping, *messages: dict) -> list[dict]:
    stdin = io.StringIO("\n".join(json.dumps(m) for m in messages) + "\n")
    stdout = io.StringIO()
    serve_stdio(mapping, stdin, stdout)
    return [json.loads(line) for line in stdout.getvalue().splitlines() if line.strip()]


def run_agent_raw(mapping: FakeMapping, *messages: dict) -> str:
    stdin = io.StringIO("\n".join(json.dumps(m) for m in messages) + "\n")
    stdout = io.StringIO()
    serve_stdio(mapping, stdin, stdout)
    return stdout.getvalue()


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


def update_of_kind(messages: list[dict], kind: str) -> dict:
    for update in updates_for(messages, "lane-1"):
        if update.get("sessionUpdate") == kind:
            return update
    raise AssertionError(f"no {kind!r} update in {messages}")


class InitializeTests(unittest.TestCase):
    def test_initialize_shape(self) -> None:
        mapping = FakeMapping()
        messages = run_agent(mapping, request("initialize", {"protocolVersion": 1}, 1))
        result = by_id(messages, 1)["result"]
        self.assertEqual(result["protocolVersion"], 1)
        capabilities = result["agentCapabilities"]
        self.assertTrue(capabilities["loadSession"])
        self.assertEqual(result["authMethods"], [])
        self.assertNotIn("fs", capabilities)
        self.assertNotIn("terminal", capabilities)
        self.assertNotIn("fs", result)
        self.assertNotIn("terminal", result)

    def test_initialize_does_not_touch_mapping(self) -> None:
        mapping = FakeMapping()
        run_agent(mapping, request("initialize", {"protocolVersion": 1}, 1))
        self.assertEqual(mapping.launched, [])

    def test_unknown_method_is_method_not_found(self) -> None:
        mapping = FakeMapping()
        messages = run_agent(mapping, request("session/nope", {}, 1))
        self.assertEqual(by_id(messages, 1)["error"]["code"], agent_module.jsonrpc.METHOD_NOT_FOUND)


class FramingTests(unittest.TestCase):
    def test_one_newline_terminated_object_per_frame(self) -> None:
        mapping = FakeMapping()
        raw = run_agent_raw(
            mapping,
            request("initialize", {"protocolVersion": 1}, 1),
            request("session/new", {"cwd": "/tmp/w", "mcpServers": []}, 2),
        )
        self.assertTrue(raw.endswith("\n"))
        lines = raw.split("\n")
        self.assertEqual(lines[-1], "")
        objects = []
        for line in lines:
            if line:
                parsed = json.loads(line)
                self.assertIsInstance(parsed, dict)
                objects.append(parsed)
        self.assertEqual(raw.count("\n"), len(objects))
        self.assertGreaterEqual(len(objects), 2)


class NewSessionTests(unittest.TestCase):
    def test_new_session_returns_id_modes_models_and_commands(self) -> None:
        mapping = FakeMapping()
        messages = run_agent(
            mapping,
            request("session/new", {"cwd": "/tmp/work", "mcpServers": []}, 1),
        )
        result = by_id(messages, 1)["result"]
        self.assertEqual(result["sessionId"], "lane-1")
        self.assertEqual(result["modes"]["currentModeId"], "default")
        self.assertEqual(result["models"]["currentModelId"], "muse")
        commands = update_of_kind(messages, "available_commands_update")
        self.assertEqual(commands["availableCommands"][0]["name"], "model")
        self.assertEqual(mapping.launched, [("/tmp/work", "work")])


class LoadTests(unittest.TestCase):
    def test_load_replays_history_before_response(self) -> None:
        mapping = FakeMapping()
        mapping.replay = [
            {
                "sessionUpdate": "user_message_chunk",
                "content": {"type": "text", "text": "hello"},
            },
            {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "world"},
            },
        ]
        messages = run_agent(
            mapping,
            request("session/load", {"sessionId": "lane-1", "cwd": "/tmp/work"}, 1),
        )
        replay_indexes = [
            i
            for i, m in enumerate(messages)
            if m.get("method") == "session/update"
            and m["params"]["update"].get("sessionUpdate")
            != "available_commands_update"
        ]
        response_index = next(i for i, m in enumerate(messages) if m.get("id") == 1)
        self.assertTrue(replay_indexes)
        self.assertLess(max(replay_indexes), response_index)
        self.assertEqual(update_of_kind(messages, "user_message_chunk")["content"]["text"], "hello")
        result = by_id(messages, 1)["result"]
        self.assertEqual(result["sessionId"], "lane-1")
        self.assertIn("modes", result)
        self.assertIn("models", result)
        self.assertEqual(mapping.resumed, [("lane-1", "/tmp/work")])


class PromptTests(unittest.TestCase):
    def test_prompt_streams_only_renderable_updates_and_stops(self) -> None:
        mapping = FakeMapping()
        mapping.prompt_script = [
            contract.MappingEvent(
                contract.UPDATE,
                {
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "hi"},
                    }
                },
            ),
            contract.MappingEvent(
                contract.UPDATE,
                {"update": {"sessionUpdate": "plan", "entries": []}},
            ),
            contract.MappingEvent(contract.STOP, {"stopReason": "end_turn"}),
        ]
        messages = run_agent(
            mapping,
            request(
                "session/prompt",
                {"sessionId": "lane-1", "prompt": [{"type": "text", "text": "hello"}]},
                1,
            ),
        )
        kinds = [u["sessionUpdate"] for u in updates_for(messages, "lane-1")]
        self.assertIn("agent_message_chunk", kinds)
        self.assertNotIn("plan", kinds)
        self.assertEqual(by_id(messages, 1)["result"]["stopReason"], "end_turn")
        self.assertEqual(mapping.prompt_calls, [("lane-1", "hello")])

    def test_prompt_relays_permission_and_answers(self) -> None:
        mapping = FakeMapping()
        mapping.prompt_script = [
            contract.MappingEvent(
                contract.UPDATE,
                {
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "working"},
                    }
                },
            ),
            contract.MappingEvent(
                contract.PERMISSION,
                {
                    "requestId": "perm-1",
                    "toolCall": {
                        "toolCallId": "t1",
                        "title": "Edit file",
                        "kind": "edit",
                        "locations": [{"path": "/tmp/w/a.txt"}],
                    },
                    "options": [
                        {"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
                        {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"},
                    ],
                },
            ),
            contract.MappingEvent(
                contract.UPDATE,
                {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "t1",
                        "status": "completed",
                    }
                },
            ),
            contract.MappingEvent(contract.STOP, {"stopReason": "end_turn"}),
        ]
        messages = run_agent(
            mapping,
            request(
                "session/prompt",
                {"sessionId": "lane-1", "prompt": [{"type": "text", "text": "go"}]},
                1,
            ),
            response("m8s-1", {"outcome": {"outcome": "selected", "optionId": "allow-once"}}),
        )
        permission = by_id(messages, "m8s-1")
        self.assertEqual(permission["method"], "session/request_permission")
        self.assertEqual(permission["params"]["sessionId"], "lane-1")
        self.assertEqual(permission["params"]["toolCall"]["title"], "Edit file")
        self.assertEqual(len(permission["params"]["options"]), 2)
        self.assertEqual(
            mapping.permission_answers, [("lane-1", "perm-1", "allow-once")]
        )
        self.assertEqual(
            update_of_kind(messages, "tool_call_update")["status"], "completed"
        )
        self.assertEqual(by_id(messages, 1)["result"]["stopReason"], "end_turn")

    def test_prompt_relays_question_elicitation(self) -> None:
        mapping = FakeMapping()
        mapping.prompt_script = [
            contract.MappingEvent(
                contract.QUESTION, {"requestId": "q-1", "message": "Which branch?"}
            ),
            contract.MappingEvent(contract.STOP, {"stopReason": "end_turn"}),
        ]
        messages = run_agent(
            mapping,
            request(
                "session/prompt",
                {"sessionId": "lane-1", "prompt": [{"type": "text", "text": "go"}]},
                1,
            ),
            response("m8s-1", {"action": "accept", "content": {"response": "main"}}),
        )
        elicitation = by_id(messages, "m8s-1")
        self.assertEqual(elicitation["method"], "elicitation/create")
        self.assertEqual(elicitation["params"]["mode"], "form")
        self.assertEqual(elicitation["params"]["message"], "Which branch?")
        self.assertEqual(elicitation["params"]["sessionId"], "lane-1")
        self.assertIn("requestedSchema", elicitation["params"])
        self.assertEqual(mapping.question_answers, [("lane-1", "q-1", "main")])

    def test_prompt_without_response_ends_cancelled(self) -> None:
        mapping = FakeMapping()
        mapping.prompt_script = [
            contract.MappingEvent(
                contract.PERMISSION,
                {"requestId": "perm-1", "toolCall": {}, "options": []},
            ),
            contract.MappingEvent(contract.STOP, {"stopReason": "end_turn"}),
        ]
        messages = run_agent(
            mapping,
            request(
                "session/prompt",
                {"sessionId": "lane-1", "prompt": [{"type": "text", "text": "go"}]},
                1,
            ),
        )
        self.assertEqual(by_id(messages, 1)["result"]["stopReason"], "cancelled")
        self.assertIn("lane-1", mapping.cancelled)


class CancelTests(unittest.TestCase):
    def test_cancel_notification_calls_mapping(self) -> None:
        mapping = FakeMapping()
        messages = run_agent(
            mapping,
            notification("session/cancel", {"sessionId": "lane-1"}),
        )
        self.assertEqual(messages, [])
        self.assertEqual(mapping.cancelled, ["lane-1"])

    def test_cancel_mid_turn_stops_with_cancelled(self) -> None:
        mapping = FakeMapping()
        mapping.prompt_script = [
            contract.MappingEvent(
                contract.UPDATE,
                {
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "working"},
                    }
                },
            ),
            contract.MappingEvent(
                contract.PERMISSION,
                {
                    "requestId": "perm-1",
                    "toolCall": {"toolCallId": "t1"},
                    "options": [],
                },
            ),
            contract.MappingEvent(contract.STOP, {"stopReason": "cancelled"}),
        ]
        messages = run_agent(
            mapping,
            request(
                "session/prompt",
                {"sessionId": "lane-1", "prompt": [{"type": "text", "text": "go"}]},
                1,
            ),
            notification("session/cancel", {"sessionId": "lane-1"}),
            response("m8s-1", {"outcome": {"outcome": "cancelled"}}),
        )
        self.assertEqual(mapping.cancelled, ["lane-1"])
        self.assertEqual(
            mapping.permission_answers, [("lane-1", "perm-1", "cancelled")]
        )
        self.assertEqual(by_id(messages, 1)["result"]["stopReason"], "cancelled")


class BridgeControlFrameTests(unittest.TestCase):
    def test_connected_frame_before_first_acp_message_is_tolerated(self) -> None:
        mapping = FakeMapping()
        messages = run_agent(
            mapping,
            {"type": "connected", "clientId": "abc"},
            request("initialize", {"protocolVersion": 1}, 1),
        )
        result = by_id(messages, 1)["result"]
        self.assertEqual(result["protocolVersion"], 1)
        self.assertFalse(any("error" in m for m in messages))


class ControlTests(unittest.TestCase):
    def test_set_mode_and_set_model_route_to_set_control(self) -> None:
        mapping = FakeMapping()
        messages = run_agent(
            mapping,
            request("session/set_mode", {"sessionId": "lane-1", "modeId": "plan"}, 1),
            request("session/set_model", {"sessionId": "lane-1", "modelId": "opus"}, 2),
        )
        self.assertEqual(
            mapping.controls,
            [("lane-1", "approval", "plan"), ("lane-1", "model", "opus")],
        )
        self.assertEqual(by_id(messages, 1)["result"], {})
        self.assertEqual(by_id(messages, 2)["result"], {})


class ModuleBoundaryTests(unittest.TestCase):
    def test_agent_imports_no_mapping_module(self) -> None:
        tree = ast.parse(Path(agent_module.__file__).read_text(encoding="utf-8"))
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
            elif isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
        self.assertFalse(
            [name for name in imported if name == "mapping" or name.endswith(".mapping")],
            imported,
        )

    def test_construct_mapping_filters_kwargs_to_signature(self) -> None:
        class StubMapping:
            def __init__(self, socket: str | None = None) -> None:
                self.socket = socket

        args = argparse.Namespace(
            socket="/tmp/control.sock", workspace_root="/w", token_file=None
        )
        built = cli._construct_mapping(StubMapping, args)
        self.assertEqual(built.socket, "/tmp/control.sock")


if __name__ == "__main__":
    unittest.main()
