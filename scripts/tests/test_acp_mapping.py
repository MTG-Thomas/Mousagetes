#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Unit tests for the daemon-backed ACP mapping (WS-B).

Stdlib unittest only. Every test drives :class:`DaemonMapping` against an
in-memory fake control server (a plain object with ``request()``), mirroring
``test_muse_msp.py``'s fake-host style: no real daemon, no socket, no
network. The one exception is the framing test, which uses a
``socket.socketpair`` (an in-memory pipe) to exercise ``ControlClient``.
"""

from __future__ import annotations

import json
import socket
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from m8s_acp import contract  # noqa: E402
from m8s_acp.daemon import ControlClient, DaemonError  # noqa: E402
from m8s_acp.mapping import (  # noqa: E402
    NEW_SESSION_BRIEF,
    DaemonMapping,
    TurnFailedError,
    TurnTimeoutError,
    command_descriptors,
    parse_command,
)


class FakeDaemon:
    """In-memory control server: records requests, returns canned results."""

    def __init__(self, handler=None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.handler = handler if handler is not None else (lambda request: {})

    def request(self, request: dict[str, Any]) -> Any:
        self.calls.append(request)
        return self.handler(request)

    def methods(self) -> list[str]:
        return [call.get("method", "") for call in self.calls]

    def count(self, method: str) -> int:
        return sum(1 for call in self.calls if call.get("method") == method)


def make_mapping(handler=None, **kwargs) -> tuple[DaemonMapping, FakeDaemon]:
    fake = FakeDaemon(handler)
    kwargs.setdefault("poll_interval", 0)
    kwargs.setdefault("sleep", lambda _seconds: None)
    return DaemonMapping(fake, **kwargs), fake


class ListLanesTest(unittest.TestCase):
    def test_maps_roster_to_lane_records(self) -> None:
        def handler(request):
            self.assertEqual(request, {"command": "list"})
            return {
                "sessions": [
                    {
                        "sessionId": "s1",
                        "alias": "lane-1",
                        "workspace": "/w",
                        "status": "idle",
                        "modelId": "m1",
                        "approvalMode": "allowAll",
                    },
                    {"name": "orphan"},
                ]
            }

        mapping, _ = make_mapping(handler)
        self.assertEqual(
            mapping.list_lanes(),
            [
                {
                    "laneId": "s1",
                    "title": "lane-1",
                    "cwd": "/w",
                    "status": "idle",
                    "modelId": "m1",
                    "approvalMode": "allowAll",
                }
            ],
        )


class LaunchLaneTest(unittest.TestCase):
    def test_launch_uses_placeholder_brief_and_returns_lane_id(self) -> None:
        def handler(request):
            self.assertEqual(request["command"], "launch")
            return {"session": {"sessionId": "s9"}}

        mapping, fake = make_mapping(handler)
        self.assertEqual(mapping.launch_lane("/tmp/w", "lane-9"), "s9")
        request = fake.calls[0]
        self.assertEqual(request["workspace"], "/tmp/w")
        # The alias must be unique (Muse's name authority rejects
        # duplicates), so it keeps the title as a prefix plus a suffix.
        self.assertTrue(request["name"].startswith("lane-9"))
        self.assertNotEqual(request["name"], "lane-9")
        self.assertEqual(request["prompt"], NEW_SESSION_BRIEF)
        self.assertTrue(request["prompt"].strip())

    def test_brief_is_overridable(self) -> None:
        def handler(request):
            return {"session": {"sessionId": "s1"}}

        mapping, fake = make_mapping(handler, new_session_brief="wait")
        mapping.launch_lane("/tmp/w", "lane")
        self.assertEqual(fake.calls[0]["prompt"], "wait")


class ResumeLaneTest(unittest.TestCase):
    def test_resume_then_replays_view_history(self) -> None:
        pages = [
            {
                "events": [
                    {"method": "turn/started", "params": {"viewCursor": "v1"}},
                    {
                        "params": {
                            "viewCursor": "v2",
                            "item": {"kind": "userMessage", "text": "hello"},
                        }
                    },
                    {
                        "params": {
                            "viewCursor": "v3",
                            "item": {
                                "kind": "toolCall",
                                "itemId": "t1",
                                "tool": "shell",
                                "status": "completed",
                                "visibleOutput": "all tests pass",
                                "files": ["a.py"],
                            },
                        }
                    },
                ],
                "nextCursor": "c1",
            },
            {"events": [], "nextCursor": None},
        ]

        def handler(request):
            method = request.get("method")
            if method == "view/page":
                return pages.pop(0) if pages else {"events": [], "nextCursor": None}
            return {}

        mapping, fake = make_mapping(handler)
        updates = mapping.resume_lane("s1", "/w")
        self.assertEqual(
            [u["sessionUpdate"] for u in updates],
            ["user_message_chunk", "tool_call"],
        )
        tool = updates[1]
        self.assertIn("shell", tool["title"])
        self.assertIn("all tests pass", tool["title"])
        self.assertEqual(tool["status"], "completed")
        self.assertEqual(tool["locations"], [{"path": "a.py"}])
        self.assertEqual(
            fake.methods()[:3], ["session/resume", "session/read", "view/page"]
        )


class PromptTest(unittest.TestCase):
    def _handler(self, *, terminal="completed", approvals=None, inputs=None):
        state = {"views": 0}

        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "turn-1"}
            if request.get("command") == "pending":
                return {
                    "approvals": approvals or [],
                    "userInputs": inputs or [],
                }
            if request.get("method") == "view/page":
                state["views"] += 1
                if state["views"] == 1:
                    return {"events": [], "nextCursor": None}
                return {
                    "events": [
                        {
                            "params": {
                                "viewCursor": "v0",
                                "item": {
                                    "kind": "userMessage",
                                    "text": "do it",
                                },
                            }
                        },
                        {
                            "params": {
                                "viewCursor": "v1",
                                "item": {
                                    "kind": "assistantMessage",
                                    "text": "working",
                                },
                            }
                        },
                        {
                            "params": {
                                "viewCursor": "v2",
                                "item": {
                                    "kind": "toolCall",
                                    "itemId": "t1",
                                    "tool": "shell",
                                    "status": "running",
                                },
                            }
                        },
                        {
                            "method": "turn/completed",
                            "params": {"viewCursor": "v3", "terminal": terminal},
                        },
                    ],
                    "nextCursor": None,
                }
            return {}

        return handler

    def test_streams_updates_and_ends_with_stop(self) -> None:
        mapping, fake = make_mapping(self._handler(), max_polls=3)
        events = list(mapping.prompt("s1", "do it"))
        kinds = [event.kind for event in events]
        self.assertIn(contract.UPDATE, kinds)
        self.assertEqual(events[-1].kind, contract.STOP)
        self.assertEqual(events[-1].data["stopReason"], "end_turn")
        updates = [e.data["update"]["sessionUpdate"] for e in events if e.kind == contract.UPDATE]
        self.assertIn("agent_message_chunk", updates)
        self.assertIn("tool_call", updates)
        # The client already renders the user's own prompt; it is not replayed.
        self.assertNotIn("user_message_chunk", updates)
        self.assertIn({"command": "send", "session": "s1", "prompt": "do it"}, fake.calls)

    def test_cancelled_terminal_maps_to_cancelled_stop(self) -> None:
        mapping, _ = make_mapping(self._handler(terminal="cancelled"), max_polls=3)
        events = list(mapping.prompt("s1", "stop"))
        self.assertEqual(events[-1].data["stopReason"], "cancelled")

    def test_bounds_when_no_terminal_arrives(self) -> None:
        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "turn-1"}
            if request.get("command") == "pending":
                return {"approvals": [], "userInputs": []}
            if request.get("method") == "view/page":
                return {"events": [], "nextCursor": None}
            return {}

        mapping, _ = make_mapping(handler, max_polls=2)
        with self.assertRaises(TurnTimeoutError):
            list(mapping.prompt("s1", "hi"))


class TurnScopingTest(unittest.TestCase):
    """The synthetic session brief must not be streamed or mistaken for the
    client's turn (root cause: any terminal in the view latched completion)."""

    def _handler(self):
        pages = [
            {"events": [], "nextCursor": None},
            {
                "events": [
                    {
                        "params": {
                            "viewCursor": "b1",
                            "item": {
                                "kind": "userMessage",
                                "text": NEW_SESSION_BRIEF,
                                "turnId": "turn-brief",
                            },
                        }
                    },
                    {
                        "params": {
                            "viewCursor": "b2",
                            "item": {
                                "kind": "agentMessage",
                                "itemId": "m-brief",
                                "text": "Ready. Waiting for your task.",
                                "turnId": "turn-brief",
                            },
                        }
                    },
                    {
                        "method": "turn/completed",
                        "params": {
                            "viewCursor": "b3",
                            "turnId": "turn-brief",
                            "terminal": "completed",
                        },
                    },
                ],
                "nextCursor": None,
            },
            {
                "events": [
                    {
                        "params": {
                            "viewCursor": "r1",
                            "item": {
                                "kind": "userMessage",
                                "text": "Reply with exactly: E2E-OK.",
                                "turnId": "turn-real",
                            },
                        }
                    },
                    {
                        "params": {
                            "viewCursor": "r2",
                            "item": {
                                "kind": "agentMessage",
                                "itemId": "m-real",
                                "text": "E2E-OK",
                                "turnId": "turn-real",
                            },
                        }
                    },
                    {
                        "method": "turn/completed",
                        "params": {
                            "viewCursor": "r3",
                            "turnId": "turn-real",
                            "terminal": "completed",
                        },
                    },
                ],
                "nextCursor": None,
            },
        ]

        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "turn-real", "startedNewTurn": True}
            if request.get("command") == "pending":
                return {"approvals": [], "userInputs": []}
            if request.get("method") == "view/page":
                return pages.pop(0) if pages else {"events": [], "nextCursor": None}
            return {}

        return handler

    def test_brief_turn_is_neither_streamed_nor_terminal(self) -> None:
        mapping, fake = make_mapping(self._handler(), max_polls=5)
        events = list(mapping.prompt("s1", "Reply with exactly: E2E-OK."))
        updates = [
            e.data["update"]
            for e in events
            if e.kind == contract.UPDATE
        ]
        chunks = [
            u for u in updates if u["sessionUpdate"] == "agent_message_chunk"
        ]
        text = "".join(chunk["content"]["text"] for chunk in chunks)
        self.assertEqual(text, "E2E-OK")
        self.assertNotIn("Ready. Waiting", text)
        self.assertEqual(chunks[0]["messageId"], "m-real")
        self.assertNotIn(
            "user_message_chunk",
            [u["sessionUpdate"] for u in updates],
        )
        self.assertEqual(events[-1].kind, contract.STOP)
        self.assertEqual(events[-1].data["stopReason"], "end_turn")


class EventMappingTest(unittest.TestCase):
    """Assistant/tool/reasoning items become renderable ACP updates (R8/R11)."""

    def _handler(self):
        pages = [
            {"events": [], "nextCursor": None},
            {
                "events": [
                    {
                        "params": {
                            "viewCursor": "e1",
                            "item": {
                                "kind": "agentMessage",
                                "itemId": "m1",
                                "status": "inProgress",
                                "text": "Hel",
                                "turnId": "t1",
                            },
                        }
                    },
                    {
                        "params": {
                            "viewCursor": "e2",
                            "item": {
                                "kind": "agentMessage",
                                "itemId": "m1",
                                "status": "completed",
                                "text": "Hello",
                                "turnId": "t1",
                            },
                        }
                    },
                    {
                        "params": {
                            "viewCursor": "e3",
                            "item": {
                                "kind": "reasoning",
                                "itemId": "r1",
                                "text": "weighing options",
                                "turnId": "t1",
                            },
                        }
                    },
                    {
                        "params": {
                            "viewCursor": "e4",
                            "item": {
                                "kind": "toolCall",
                                "itemId": "tool-1",
                                "tool": "shell",
                                "status": "running",
                                "args": {"cmd": "ls"},
                                "turnId": "t1",
                            },
                        }
                    },
                    {
                        "params": {
                            "viewCursor": "e5",
                            "item": {
                                "kind": "toolCall",
                                "itemId": "tool-1",
                                "tool": "shell",
                                "status": "completed",
                                "visibleOutput": "all tests pass",
                                "files": ["a.py"],
                                "turnId": "t1",
                            },
                        }
                    },
                    {
                        "method": "turn/completed",
                        "params": {
                            "viewCursor": "e6",
                            "turnId": "t1",
                            "terminal": "completed",
                        },
                    },
                ],
                "nextCursor": None,
            }
        ]

        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "t1"}
            if request.get("command") == "pending":
                return {"approvals": [], "userInputs": []}
            if request.get("method") == "view/page":
                return pages.pop(0) if pages else {"events": [], "nextCursor": None}
            return {}

        return handler

    def test_message_delta_and_stable_message_id(self) -> None:
        mapping, _ = make_mapping(self._handler(), max_polls=5)
        events = list(mapping.prompt("s1", "go"))
        chunks = [
            e.data["update"]
            for e in events
            if e.kind == contract.UPDATE
            and e.data["update"]["sessionUpdate"] == "agent_message_chunk"
        ]
        self.assertEqual(
            [c["content"]["text"] for c in chunks], ["Hel", "lo"]
        )
        self.assertEqual({c["messageId"] for c in chunks}, {"m1"})

    def test_reasoning_becomes_agent_thought_chunk(self) -> None:
        mapping, _ = make_mapping(self._handler(), max_polls=5)
        events = list(mapping.prompt("s1", "go"))
        thoughts = [
            e.data["update"]
            for e in events
            if e.kind == contract.UPDATE
            and e.data["update"]["sessionUpdate"] == "agent_thought_chunk"
        ]
        self.assertEqual(thoughts[0]["content"]["text"], "weighing options")

    def test_tool_items_map_to_tool_call_then_update(self) -> None:
        mapping, _ = make_mapping(self._handler(), max_polls=5)
        events = list(mapping.prompt("s1", "go"))
        tools = [
            e.data["update"]
            for e in events
            if e.kind == contract.UPDATE
            and e.data["update"]["sessionUpdate"] in ("tool_call", "tool_call_update")
        ]
        self.assertEqual(
            [t["sessionUpdate"] for t in tools],
            ["tool_call", "tool_call_update"],
        )
        self.assertEqual(tools[0]["toolCallId"], "tool-1")
        self.assertEqual(tools[0]["title"], 'shell: {"cmd":"ls"}')
        self.assertEqual(tools[1]["status"], "completed")
        self.assertIn("all tests pass", tools[1]["title"])
        self.assertEqual(tools[1]["locations"], [{"path": "a.py"}])

    def test_item_inserted_at_a_reused_cursor_is_still_streamed(self) -> None:
        # The view is not append-only: as the turn progresses the daemon
        # assigns a fresh record to a cursor already passed. Keying on the
        # viewCursor alone would drop the assistant text.
        pages = [
            {"events": [], "nextCursor": None},
            {
                "events": [
                    {
                        "method": "turn/completed",
                        "params": {
                            "viewCursor": "dup",
                            "turnId": "t1",
                            "terminal": "failed",
                            "reason": "incomplete",
                        },
                    }
                ],
                "nextCursor": None,
            },
            {
                "events": [
                    {
                        "params": {
                            "viewCursor": "dup",
                            "item": {
                                "kind": "agentMessage",
                                "itemId": "m-late",
                                "text": "late output",
                                "turnId": "t1",
                            },
                        }
                    },
                    {
                        "method": "turn/completed",
                        "params": {
                            "viewCursor": "fin",
                            "turnId": "t1",
                            "terminal": "completed",
                        },
                    },
                ],
                "nextCursor": None,
            },
        ]

        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "t1"}
            if request.get("command") == "pending":
                return {"approvals": [], "userInputs": []}
            if request.get("method") == "view/page":
                return pages.pop(0) if pages else {"events": [], "nextCursor": None}
            return {}

        mapping, _ = make_mapping(handler, max_polls=6)
        events = list(mapping.prompt("s1", "go"))
        chunks = [
            e.data["update"]
            for e in events
            if e.kind == contract.UPDATE
            and e.data["update"]["sessionUpdate"] == "agent_message_chunk"
        ]
        self.assertEqual(
            "".join(c["content"]["text"] for c in chunks), "late output"
        )
        self.assertEqual(events[-1].data["stopReason"], "end_turn")


class TerminalTest(unittest.TestCase):
    def _handler(self, terminal, reason=None, duration=True):
        params = {"viewCursor": "c1", "turnId": "t1", "terminal": terminal}
        if reason is not None:
            params["reason"] = reason
        if duration:
            params["durationMs"] = 42
        pages = [
            {"events": [], "nextCursor": None},
            {"events": [{"method": "turn/completed", "params": params}], "nextCursor": None},
        ]

        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "t1"}
            if request.get("command") == "pending":
                return {"approvals": [], "userInputs": []}
            if request.get("method") == "view/page":
                return pages.pop(0) if pages else {"events": [], "nextCursor": None}
            return {}

        return handler

    def test_failed_terminal_raises_instead_of_end_turn(self) -> None:
        mapping, _ = make_mapping(self._handler("failed"), max_polls=3)
        with self.assertRaises(TurnFailedError):
            list(mapping.prompt("s1", "go"))

    def test_interim_incomplete_failure_is_not_terminal(self) -> None:
        pages = [
            {"events": [], "nextCursor": None},
            {
                "events": [
                    {
                        "method": "turn/completed",
                        "params": {
                            "viewCursor": "i1",
                            "turnId": "t1",
                            "terminal": "failed",
                            "reason": "incomplete",
                        },
                    }
                ],
                "nextCursor": None,
            },
            {
                "events": [
                    {
                        "method": "turn/completed",
                        "params": {
                            "viewCursor": "i2",
                            "turnId": "t1",
                            "terminal": "completed",
                        },
                    }
                ],
                "nextCursor": None,
            },
        ]

        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "t1"}
            if request.get("command") == "pending":
                return {"approvals": [], "userInputs": []}
            if request.get("method") == "view/page":
                return pages.pop(0) if pages else {"events": [], "nextCursor": None}
            return {}

        mapping, _ = make_mapping(handler, max_polls=5)
        events = list(mapping.prompt("s1", "go"))
        self.assertEqual(events[-1].kind, contract.STOP)
        self.assertEqual(events[-1].data["stopReason"], "end_turn")

    def test_in_place_terminal_revision_is_re_read(self) -> None:
        # The materialized view rewrites a turn/completed record at the same
        # viewCursor from an interim failure to its real terminal. A strictly
        # forward cursor steps past it; the loop must re-read the record.
        def page(terminal, reason=None):
            params = {"viewCursor": "same-c1", "turnId": "t1", "terminal": terminal}
            if reason is not None:
                params["reason"] = reason
            return {"events": [{"method": "turn/completed", "params": params}]}

        pages = [
            {"events": [], "nextCursor": None},
            page("failed", "incomplete"),
            page("failed", "incomplete"),
            page("completed"),
        ]

        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "t1"}
            if request.get("command") == "pending":
                return {"approvals": [], "userInputs": []}
            if request.get("method") == "view/page":
                return pages.pop(0) if pages else {"events": [], "nextCursor": None}
            return {}

        mapping, _ = make_mapping(handler, max_polls=8)
        events = list(mapping.prompt("s1", "go"))
        self.assertEqual(events[-1].kind, contract.STOP)
        self.assertEqual(events[-1].data["stopReason"], "end_turn")


class PermissionTest(unittest.TestCase):
    APPROVAL = {
        "approvalId": "a1",
        "toolName": "shell",
        "currentRequirementId": "r1",
        "subject": {"kind": "command", "command": "rm -rf build"},
        "rawArgs": {"cmd": "rm -rf build"},
        "files": ["build/out.txt"],
        "availableChoices": [
            {"choiceId": "allow", "label": "Allow once"},
            {"choiceId": "deny", "label": "Reject", "acceptsFeedback": True},
        ],
    }

    def _handler(self):
        state = {"views": 0}

        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "t1"}
            if request.get("command") == "pending":
                return {"approvals": [self.APPROVAL], "userInputs": []}
            if request.get("method") == "view/page":
                state["views"] += 1
                if state["views"] == 1:
                    return {"events": [], "nextCursor": None}
                return {
                    "events": [
                        {"method": "turn/completed", "params": {"viewCursor": "v1"}}
                    ],
                    "nextCursor": None,
                }
            return {}

        return handler

    def test_permission_payload_carries_tool_call_and_options(self) -> None:
        mapping, _ = make_mapping(self._handler(), max_polls=3)
        events = list(mapping.prompt("s1", "clean up"))
        permissions = [e for e in events if e.kind == contract.PERMISSION]
        self.assertEqual(len(permissions), 1)
        data = permissions[0].data
        self.assertEqual(data["requestId"], "a1")
        tool_call = data["toolCall"]
        self.assertEqual(tool_call["toolCallId"], "a1")
        self.assertEqual(tool_call["kind"], "execute")
        self.assertIn("shell", tool_call["title"])
        self.assertEqual(tool_call["locations"], [{"path": "build/out.txt"}])
        self.assertEqual(
            [option["optionId"] for option in data["options"]], ["allow", "deny"]
        )
        self.assertEqual(data["options"][0]["kind"], "allow_once")
        self.assertEqual(data["options"][1]["kind"], "reject_once")

    def test_answer_permission_maps_choice_and_dedupes(self) -> None:
        mapping, fake = make_mapping(self._handler(), max_polls=3)
        list(mapping.prompt("s1", "clean up"))
        mapping.answer_permission("s1", "a1", "allow")
        mapping.answer_permission("s1", "a1", "allow")
        self.assertEqual(fake.count("approval/decide"), 1)
        decision = next(c for c in fake.calls if c.get("method") == "approval/decide")
        self.assertEqual(decision["session"], "s1")
        self.assertEqual(decision["params"]["choiceId"], "allow")
        self.assertEqual(decision["params"]["approvalId"], "a1")
        self.assertEqual(decision["params"]["requirementId"], "r1")

    def test_permission_is_emitted_once_across_polls(self) -> None:
        mapping, _ = make_mapping(self._handler(), max_polls=3)
        events = list(mapping.prompt("s1", "clean up"))
        self.assertEqual(sum(1 for e in events if e.kind == contract.PERMISSION), 1)


class QuestionTest(unittest.TestCase):
    USER_INPUT = {
        "userInputId": "u1",
        "toolName": "ask",
        "questions": [{"question": "Which environment?"}],
    }

    def _handler(self):
        state = {"views": 0}

        def handler(request):
            if request.get("command") == "send":
                return {"turnId": "t1"}
            if request.get("command") == "pending":
                return {"approvals": [], "userInputs": [self.USER_INPUT]}
            if request.get("method") == "view/page":
                state["views"] += 1
                if state["views"] == 1:
                    return {"events": [], "nextCursor": None}
                return {
                    "events": [
                        {"method": "turn/completed", "params": {"viewCursor": "v1"}}
                    ],
                    "nextCursor": None,
                }
            return {}

        return handler

    def test_question_event_and_answer_dedupes(self) -> None:
        mapping, fake = make_mapping(self._handler(), max_polls=3)
        events = list(mapping.prompt("s1", "deploy"))
        questions = [e for e in events if e.kind == contract.QUESTION]
        self.assertEqual(len(questions), 1)
        self.assertEqual(questions[0].data["requestId"], "u1")
        self.assertIn("Which environment?", questions[0].data["message"])
        mapping.answer_question("s1", "u1", "staging")
        mapping.answer_question("s1", "u1", "staging")
        self.assertEqual(fake.count("userInput/clarify"), 1)
        clarify = next(c for c in fake.calls if c.get("method") == "userInput/clarify")
        self.assertEqual(clarify["params"]["userInputId"], "u1")
        self.assertEqual(
            clarify["params"]["clarification"],
            {"format": "text", "content": "staging"},
        )


class ControlTest(unittest.TestCase):
    def test_cancel_maps_to_turn_cancel(self) -> None:
        mapping, fake = make_mapping()
        mapping.cancel("s1")
        self.assertEqual(
            fake.calls[0],
            {
                "command": "call",
                "method": "turn/cancel",
                "commandId": "auto",
                "session": "s1",
            },
        )

    def test_set_control_maps_each_control(self) -> None:
        mapping, fake = make_mapping()
        mapping.set_control("s1", "model", "m2")
        mapping.set_control("s1", "effort", "high")
        mapping.set_control("s1", "approval", "onRequest")
        calls = {
            call["method"]: call["params"]
            for call in fake.calls
            if call.get("command") == "call"
        }
        self.assertEqual(calls["session/setModel"], {"model": "m2"})
        self.assertEqual(
            calls["session/setReasoningEffort"], {"reasoningEffort": "high"}
        )
        self.assertEqual(calls["session/setApprovalMode"], {"mode": "onRequest"})

    def test_set_control_rejects_unknown_name(self) -> None:
        mapping, _ = make_mapping()
        with self.assertRaises(ValueError):
            mapping.set_control("s1", "temperature", "9")


class AdvertiseTest(unittest.TestCase):
    def test_modes_returns_current_and_available(self) -> None:
        def handler(request):
            if request.get("command") == "list":
                return {"sessions": [{"sessionId": "s1", "approvalMode": "allowAll"}]}
            return {}

        mapping, _ = make_mapping(handler)
        modes = mapping.modes("s1")
        self.assertEqual(modes["currentModeId"], "allowAll")
        self.assertIn(
            "onRequest", {mode["id"] for mode in modes["availableModes"]}
        )

    def test_models_merges_current_and_catalog(self) -> None:
        def handler(request):
            if request.get("command") == "list":
                return {"sessions": [{"sessionId": "s1", "modelId": "m1"}]}
            if request.get("method") == "model/list":
                return {
                    "models": [
                        {"id": "m1", "name": "Model One"},
                        {"modelId": "m2", "description": "second"},
                    ]
                }
            return {}

        mapping, _ = make_mapping(handler)
        models = mapping.models("s1")
        self.assertEqual(models["currentModelId"], "m1")
        self.assertEqual(
            [m["modelId"] for m in models["availableModels"]], ["m1", "m2"]
        )

    def test_commands_advertise_the_control_set(self) -> None:
        descriptors = command_descriptors()
        names = [descriptor["name"] for descriptor in descriptors]
        self.assertEqual(
            names,
            [
                "model",
                "effort",
                "approval",
                "goal",
                "fork",
                "compact",
                "retire",
                "budget",
            ],
        )
        for descriptor in descriptors:
            self.assertTrue(descriptor["description"])
        model = descriptors[0]
        self.assertEqual(model["input"]["hint"], "<model-id>")


class RunCommandTest(unittest.TestCase):
    def test_each_slash_command_reaches_the_daemon(self) -> None:
        cases = [
            (("model", "m2"), "session/setModel"),
            (("effort", "low"), "session/setReasoningEffort"),
            (("approval", "onRequest"), "session/setApprovalMode"),
            (("goal", "ship it"), "goal/set"),
            (("fork", ""), "session/fork"),
            (("compact", ""), "session/compact"),
        ]
        for (name, argument), method in cases:
            with self.subTest(command=name):
                mapping, fake = make_mapping()
                mapping.run_command("s1", name, argument)
                self.assertIn(method, fake.methods())

    def test_retire_and_budget_use_top_level_commands(self) -> None:
        mapping, fake = make_mapping()
        mapping.run_command("s1", "retire")
        mapping.run_command("s1", "budget", "1000")
        self.assertEqual(
            fake.calls[0], {"command": "retire", "session": "s1"}
        )
        self.assertEqual(
            fake.calls[1],
            {"command": "budget", "session": "s1", "maxTokens": 1000},
        )

    def test_unknown_command_rejected(self) -> None:
        mapping, _ = make_mapping()
        with self.assertRaises(ValueError):
            mapping.run_command("s1", "explode")

    def test_parse_command(self) -> None:
        self.assertEqual(parse_command("/model gpt"), ("model", "gpt"))
        self.assertEqual(parse_command("/fork"), ("fork", ""))
        self.assertIsNone(parse_command("hello"))

    def test_prompt_intercepts_slash_command(self) -> None:
        mapping, fake = make_mapping()
        events = list(mapping.prompt("s1", "/effort high"))
        self.assertEqual(
            [event.kind for event in events],
            [contract.UPDATE, contract.STOP],
        )
        self.assertIn("session/setReasoningEffort", fake.methods())
        self.assertNotIn("send", [c.get("command") for c in fake.calls])


class ContractSurfaceTest(unittest.TestCase):
    def test_daemon_mapping_exposes_every_lane_mapping_method(self) -> None:
        for name in (
            "list_lanes",
            "launch_lane",
            "resume_lane",
            "prompt",
            "cancel",
            "answer_permission",
            "answer_question",
            "set_control",
            "commands",
            "modes",
            "models",
        ):
            with self.subTest(method=name):
                # Defined on DaemonMapping itself, not an inherited Protocol
                # stub: a new contract method cannot pass silently.
                self.assertIn(name, DaemonMapping.__dict__)
                self.assertTrue(callable(getattr(DaemonMapping, name)))


class ControlClientTest(unittest.TestCase):
    def test_roundtrip_over_socketpair(self) -> None:
        left, right = socket.socketpair()
        try:
            right.sendall(b'{"ok":true,"result":{"sessions":[]}}\n')
            client = ControlClient(connector=lambda: left)
            self.assertEqual(client.request({"command": "list"}), {"sessions": []})
            right.settimeout(1.0)
            raw = right.recv(65536)
            self.assertEqual(json.loads(raw.decode()), {"command": "list"})
        finally:
            right.close()

    def test_error_kind_is_preserved(self) -> None:
        left, right = socket.socketpair()
        try:
            right.sendall(
                b'{"ok":false,"error":"unknown session","errorKind":"sessionNotFound"}\n'
            )
            client = ControlClient(connector=lambda: left)
            with self.assertRaises(DaemonError) as caught:
                client.request({"command": "call", "method": "session/read"})
            self.assertEqual(caught.exception.kind, "sessionNotFound")
        finally:
            right.close()

    def test_unreachable_socket_raises_daemon_error(self) -> None:
        def refuse() -> socket.socket:
            raise OSError("no such file")

        client = ControlClient(
            socket_path="/nonexistent/control.sock", connector=refuse
        )
        with self.assertRaises(DaemonError):
            client.request({"command": "list"})


if __name__ == "__main__":
    unittest.main()
