#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Focused tests for the muse-msp.py controller's schema-wide CLI surface.

Stdlib unittest only. Covers request building and daemon dispatch against a
fake host — never starts a daemon or touches the network.
"""

from __future__ import annotations

import asyncio
import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "muse-msp.py"


def load_module():
    spec = importlib.util.spec_from_file_location("muse_msp", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


muse_msp = load_module()


def parse(argv: list[str]):
    return muse_msp.parser().parse_args(argv)


class FakeHost:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.aliases = {"lane": "session-1"}

    def resolve(self, reference: str) -> str:
        return self.aliases.get(reference, reference)

    def record(self, record: dict) -> None:
        pass

    async def call(self, method: str, params: dict | None = None):
        self.calls.append((method, dict(params or {})))
        return {"ok": True}

    async def supervised_call(self, method: str, params: dict | None = None, command_id: str = "auto"):
        # Reuse the real implementation; it only needs call() and record().
        return await muse_msp.MspHost.supervised_call(self, method, params, command_id)  # type: ignore[arg-type]


class BuildRequestTest(unittest.TestCase):
    def test_legacy_commands_unchanged(self) -> None:
        self.assertEqual(muse_msp.build_request(parse(["list"])), {"command": "list"})
        req = muse_msp.build_request(parse(["send", "lane", "hi"]))
        self.assertEqual(req["command"], "send")
        self.assertEqual(req["session"], "lane")

    def test_call_passthrough(self) -> None:
        req = muse_msp.build_request(
            parse(["call", "session/read", "--session", "lane", "--params", '{"x": 1}'])
        )
        self.assertEqual(req["method"], "session/read")
        self.assertEqual(req["session"], "lane")
        self.assertEqual(req["params"], {"x": 1})
        self.assertEqual(req["commandId"], "auto")

    def test_curated_methods(self) -> None:
        cases = [
            (["read", "s"], "session/read"),
            (["view", "s"], "view/page"),
            (["goal", "s", "pause"], "goal/pause"),
            (["usage"], "usage/read"),
            (["models"], "model/list"),
            (["account"], "account/read"),
            (["skills", "s"], "skill/list"),
            (["turn", "s", "cancel"], "turn/cancel"),
            (["task", "s", "stop-all"], "task/stopAll"),
            (["compact", "s"], "session/compact"),
            (["fork", "s"], "session/fork"),
            (["rename", "s", "n"], "session/rename"),
            (["resume-session", "s"], "session/resume"),
            (["set-model", "s", "m"], "session/setModel"),
            (["set-effort", "s", "low"], "session/setReasoningEffort"),
            (["set-approval-mode", "s", "onRequest"], "session/setApprovalMode"),
        ]
        for argv, method in cases:
            with self.subTest(argv=argv):
                req = muse_msp.build_request(parse(argv))
                self.assertEqual(req["command"], "call")
                self.assertEqual(req["method"], method)

    def test_goal_set_carries_objective(self) -> None:
        req = muse_msp.build_request(parse(["goal", "s", "set", "do things"]))
        self.assertEqual(req["method"], "goal/set")
        self.assertEqual(req["params"], {"objective": "do things"})

    def test_workflow_and_subagent_mapping(self) -> None:
        req = muse_msp.build_request(parse(["workflow", "s", "cancel", "run-1"]))
        self.assertEqual(req["method"], "workflow/cancel")
        self.assertEqual(req["params"], {"workflowRunId": "run-1"})
        req = muse_msp.build_request(
            parse(["workflow", "s", "child", "run-1", "--child-id", "c", "--attempt", "2", "--child-action", "retry"])
        )
        self.assertEqual(req["method"], "workflow/childControl")
        req = muse_msp.build_request(parse(["subagent", "s", "sub-1", "send", "--body", "hi"]))
        self.assertEqual(req["method"], "subagent/sendMessage")
        self.assertEqual(req["params"]["body"], "hi")
        req = muse_msp.build_request(parse(["subagent", "s", "sub-1", "read-result"]))
        self.assertEqual(req["method"], "subagent/readResult")

    def test_view_defaults(self) -> None:
        req = muse_msp.build_request(parse(["view", "s"]))
        self.assertEqual(req["params"], {"limit": 50})


class DispatchTest(unittest.TestCase):
    def run_call(self, request: dict):
        async def go():
            host = FakeHost()
            # Exercise the real daemon dispatch path with a fake transport host.
            result = await muse_msp.dispatch(
                host,  # type: ignore[arg-type]
                {"command": "call", **request},
            )
            return host, result

        return asyncio.run(go())

    def test_command_id_minted_for_command_methods(self) -> None:
        host, _ = self.run_call({"method": "goal/pause", "session": "lane"})
        method, params = host.calls[0]
        self.assertEqual(method, "goal/pause")
        self.assertEqual(params["sessionId"], "session-1")
        self.assertTrue(params["commandId"])

    def test_no_command_id_for_read_methods(self) -> None:
        host, _ = self.run_call({"method": "session/list"})
        _, params = host.calls[0]
        self.assertNotIn("commandId", params)

    def test_explicit_command_id_preserved(self) -> None:
        host, _ = self.run_call({"method": "goal/pause", "session": "lane", "commandId": "mine"})
        self.assertEqual(host.calls[0][1]["commandId"], "mine")

    def test_alias_resolved_to_session_id(self) -> None:
        host, _ = self.run_call({"method": "skill/list", "session": "lane"})
        self.assertEqual(host.calls[0], ("skill/list", {"sessionId": "session-1"}))


if __name__ == "__main__":
    unittest.main()
