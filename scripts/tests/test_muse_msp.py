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


class FakeHost(muse_msp.MspHost):
    def __init__(self) -> None:
        # Deliberately no super().__init__: no daemon state, no budget file read.
        self.calls: list[tuple[str, dict]] = []
        self.aliases = {"lane": "session-1"}
        self.sessions: dict[str, dict] = {}
        self.budgets: dict[str, dict] = {}
        self.watchers = set()

    def record(self, record: dict) -> None:
        pass

    async def call(self, method: str, params: dict | None = None):
        self.calls.append((method, dict(params or {})))
        return {"ok": True}


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

    def test_reload_request_builds(self) -> None:
        self.assertEqual(
            muse_msp.build_request(parse(["reload"])), {"command": "reload"}
        )

    def test_adopt_request_builds(self) -> None:
        self.assertEqual(
            muse_msp.build_request(parse(["adopt", "lane-9"])),
            {"command": "adopt", "session": "lane-9"},
        )
        self.assertEqual(
            muse_msp.build_request(parse(["adopt", "s9", "--name", "lane-9"])),
            {"command": "adopt", "session": "s9", "name": "lane-9"},
        )

    def test_adopt_registers_live_session_without_turn(self) -> None:
        import tempfile
        from unittest import mock

        class AdoptHost(FakeHost):
            async def call(self, method: str, params: dict | None = None):
                assert method == "session/list"
                return {
                    "sessions": [
                        {"sessionId": "s9", "name": "lane-9", "status": "idle"}
                    ]
                }

        async def go(host, request):
            return await muse_msp.dispatch(host, request)

        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(
                muse_msp, "SESSIONS_FILE", Path(tmp) / "sessions.json"
            ):
                host = AdoptHost()
                host.sessions = {}
                host.aliases = {}
                result = asyncio.run(go(host, {"command": "adopt", "session": "lane-9"}))
                with self.assertRaises(ValueError):
                    asyncio.run(go(AdoptHost(), {"command": "adopt", "session": "nope"}))
        self.assertEqual(result["session"]["alias"], "lane-9")
        self.assertEqual(result["session"]["sessionId"], "s9")
        self.assertEqual(host.aliases["lane-9"], "s9")

    def test_reload_flags_stop_and_responds(self) -> None:
        import tempfile
        from unittest import mock

        async def go():
            host = FakeHost()
            host.stopping = asyncio.Event()
            host.sessions = {}
            host.aliases = {}
            return await muse_msp.dispatch(host, {"command": "reload"}), host

        saved = muse_msp.RELOAD_REQUESTED
        muse_msp.RELOAD_REQUESTED = False
        try:
            with tempfile.TemporaryDirectory() as tmp:
                with mock.patch.object(
                    muse_msp, "SESSIONS_FILE", Path(tmp) / "sessions.json"
                ):
                    result, host = asyncio.run(go())
                    fired = muse_msp.RELOAD_REQUESTED
        finally:
            muse_msp.RELOAD_REQUESTED = saved
        self.assertEqual(result, {"reloading": True})
        self.assertTrue(host.stopping.is_set())
        self.assertTrue(fired)

    def test_roster_roundtrip(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sessions.json"
            sessions = {"s1": {"sessionId": "s1", "alias": "lane-1"}}
            aliases = {"lane-1": "s1"}
            muse_msp.save_roster(sessions, aliases, path)
            self.assertEqual(muse_msp.load_roster(path), (sessions, aliases))

    def test_roster_missing_or_corrupt_is_empty(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "nope.json"
            self.assertEqual(muse_msp.load_roster(missing), ({}, {}))
            bad = Path(tmp) / "bad.json"
            bad.write_text("not json{", encoding="utf-8")
            self.assertEqual(muse_msp.load_roster(bad), ({}, {}))

    def test_restore_keeps_live_drops_dead(self) -> None:
        import tempfile

        class RestoreHost(FakeHost):
            async def call(self, method: str, params: dict | None = None):
                assert method == "session/list"
                return {"sessions": [{"sessionId": "live"}]}

        async def go(path):
            host = RestoreHost()
            host.sessions = {}
            host.aliases = {}
            result = await host.restore_roster(path)
            return host, result

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sessions.json"
            muse_msp.save_roster(
                {
                    "live": {"sessionId": "live", "alias": "lane-live"},
                    "dead": {"sessionId": "dead", "alias": "lane-dead"},
                },
                {"lane-live": "live", "lane-dead": "dead"},
                path,
            )
            host, result = asyncio.run(go(path))
        self.assertEqual(result, {"restored": 1, "dropped": 1})
        self.assertIn("live", host.sessions)
        self.assertNotIn("dead", host.sessions)
        self.assertEqual(host.aliases, {"lane-live": "live"})

    def test_reload_process_execs_current_script(self) -> None:
        from unittest import mock

        with mock.patch.object(muse_msp.os, "execv") as execv:
            try:
                muse_msp.reload_process()
            except Exception:
                pass
        execv.assert_called_once()
        argv = execv.call_args[0]
        self.assertEqual(argv[0], muse_msp.sys.executable)
        self.assertEqual(
            argv[1], [muse_msp.sys.executable, str(SCRIPT.resolve()), "serve"]
        )


if __name__ == "__main__":
    unittest.main()
