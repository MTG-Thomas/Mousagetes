#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for the supervisor-owned session retire path (issue #21).

Stdlib unittest only. Covers retire request building and daemon dispatch
against a fake host, plus the retire guards: idle retire drops the session
from roster/aliases/budgets, busy/dirty/open-PR sessions are refused
without the explicit --force override, retired sessions stay out of
roster/health/stuck accounting, and the retired set survives daemon
restarts. Never starts a daemon, never touches the network, never shells
to git/gh (worktree/PR probes are stubbed).
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import tempfile
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
        # Deliberately no super().__init__: no daemon state, no file reads.
        self.calls: list[tuple[str, dict]] = []
        self.aliases = {"lane": "session-1"}
        self.sessions: dict[str, dict] = {
            "session-1": {
                "sessionId": "session-1",
                "alias": "lane",
                "status": "idle",
                "workspace": "/tmp/m8s-lanes/lane",
                "lastActivity": 1_700_000_000.0,
            }
        }
        self.budgets: dict[str, dict] = {}
        self.retired: dict[str, dict] = {}
        self.watchers = set()
        self.server_sessions: list[dict] = [{"sessionId": "session-1", "status": "idle"}]
        self.pending_reply: dict = {}
        self.events: list[dict] = []

    def record(self, record: dict) -> None:
        self.events.append(record)

    async def call(self, method: str, params: dict | None = None):
        self.calls.append((method, dict(params or {})))
        if method == "approval/listPending":
            return dict(self.pending_reply)
        if method == "session/list":
            return {"sessions": list(self.server_sessions)}
        return {"ok": True}


class RetireTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.old_budgets = muse_msp.BUDGETS_FILE
        self.old_retired = muse_msp.RETIRED_FILE
        muse_msp.BUDGETS_FILE = Path(self.tmp.name) / "budgets.json"
        muse_msp.RETIRED_FILE = Path(self.tmp.name) / "retired.json"
        # Stub the worktree/PR probes: clean tree, no branch, no PR.
        self.old_dirty = muse_msp.worktree_dirty
        self.old_progress = muse_msp.health_progress_for_workspace
        self.old_pr = muse_msp.health_pr_for_branch
        muse_msp.worktree_dirty = lambda workspace, run=None: False  # type: ignore[assignment]
        muse_msp.health_progress_for_workspace = lambda workspace, run=None: {  # type: ignore[assignment]
            "branch": None,
            "ahead": None,
        }
        muse_msp.health_pr_for_branch = lambda branch, workspace=None, run=None: None  # type: ignore[assignment]

    def tearDown(self) -> None:
        muse_msp.BUDGETS_FILE = self.old_budgets
        muse_msp.RETIRED_FILE = self.old_retired
        muse_msp.worktree_dirty = self.old_dirty
        muse_msp.health_progress_for_workspace = self.old_progress
        muse_msp.health_pr_for_branch = self.old_pr
        self.tmp.cleanup()

    def test_cli_builds_retire_request(self) -> None:
        req = muse_msp.build_request(parse(["retire", "lane"]))
        self.assertEqual(req, {"command": "retire", "session": "lane", "force": False})
        req = muse_msp.build_request(parse(["retire", "lane", "--force"]))
        self.assertEqual(req, {"command": "retire", "session": "lane", "force": True})

    def test_dispatch_retire_reaches_host(self) -> None:
        host = FakeHost()
        result = asyncio.run(muse_msp.dispatch(host, {"command": "retire", "session": "lane"}))
        self.assertTrue(result["retired"])
        self.assertEqual(result["sessionId"], "session-1")

    def test_idle_retire_removes_from_roster_and_frees_slots(self) -> None:
        host = FakeHost()
        host.budgets = {"session-1": {"maxTokens": 100}}
        result = asyncio.run(host.retire("lane"))
        self.assertEqual(result["sessionId"], "session-1")
        self.assertEqual(result["alias"], "lane")
        self.assertFalse(result["forced"])
        self.assertNotIn("session-1", host.sessions)
        self.assertNotIn("lane", host.aliases)
        self.assertNotIn("session-1", host.budgets)
        kinds = [record["kind"] for record in host.events]
        self.assertIn("lane.retired", kinds)
        persisted = json.loads(Path(muse_msp.RETIRED_FILE).read_text())
        self.assertIn("session-1", persisted)

    def test_unknown_session_is_typed(self) -> None:
        host = FakeHost()
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.retire("nope"))
        self.assertEqual(ctx.exception.kind, "sessionNotFound")

    def test_busy_session_refused_without_override(self) -> None:
        host = FakeHost()
        host.sessions["session-1"]["attention"] = ["approvalPending"]
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.retire("lane"))
        self.assertEqual(ctx.exception.kind, "sessionBusy")
        self.assertIn("session-1", host.sessions)
        result = asyncio.run(host.retire("lane", force=True))
        self.assertTrue(result["retired"])
        self.assertTrue(result["forced"])

    def test_dead_turn_needs_owner_action_without_override(self) -> None:
        host = FakeHost()
        host.sessions["session-1"]["needsOwnerAction"] = True
        host.sessions["session-1"]["lastTerminal"] = "failed"
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.retire("lane"))
        self.assertEqual(ctx.exception.kind, "sessionBusy")

    def test_pending_inputs_refused_without_override(self) -> None:
        host = FakeHost()
        host.pending_reply = {"userInputs": [{"id": "q1"}]}
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.retire("lane"))
        self.assertEqual(ctx.exception.kind, "sessionBusy")

    def test_uncommitted_work_refused_without_override(self) -> None:
        host = FakeHost()
        muse_msp.worktree_dirty = lambda workspace, run=None: True  # type: ignore[assignment]
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.retire("lane"))
        self.assertEqual(ctx.exception.kind, "uncommittedWork")
        self.assertIn("session-1", host.sessions)

    def test_open_pr_refused_without_override(self) -> None:
        host = FakeHost()
        muse_msp.health_progress_for_workspace = lambda workspace, run=None: {  # type: ignore[assignment]
            "branch": "lane/21-retire",
            "ahead": 2,
        }
        muse_msp.health_pr_for_branch = lambda branch, workspace=None, run=None: {  # type: ignore[assignment]
            "number": 7,
            "url": "https://example.invalid/pr/7",
            "checks": "passing",
        }
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.retire("lane"))
        self.assertEqual(ctx.exception.kind, "openPR")
        result = asyncio.run(host.retire("lane", force=True))
        self.assertTrue(result["retired"])
        self.assertEqual(result["worktree"]["pr"]["number"], 7)

    def test_retire_releases_branch_lease(self) -> None:
        host = FakeHost()
        old_load = muse_msp.load_claims
        old_release = muse_msp.release_claim
        released: list[str] = []
        muse_msp.load_claims = lambda: {  # type: ignore[assignment]
            "lane/21-retire": {
                "host": "h",
                "lane": "lane-21-retire",
                "branch": "lane/21-retire",
                "sessionId": "session-1",
            }
        }
        muse_msp.release_claim = lambda branch, host=None: released.append(branch) or {}  # type: ignore[assignment]
        try:
            result = asyncio.run(host.retire("lane"))
        finally:
            muse_msp.load_claims = old_load
            muse_msp.release_claim = old_release
        self.assertEqual(released, ["lane/21-retire"])
        self.assertEqual(result["releasedClaim"]["branch"], "lane/21-retire")

    def test_retired_absent_from_roster_and_stuck(self) -> None:
        host = FakeHost()
        host.sessions["session-1"]["lastActivity"] = 1_700_000_000.0 - 10_000
        del host.sessions["session-1"]["workspace"]
        asyncio.run(host.retire("lane"))
        # The served session still exists server-side, long idle.
        listed = asyncio.run(host.list_sessions())
        self.assertEqual(listed["sessions"], [])
        report = muse_msp.fuse_health(
            listed["sessions"], [], {}, {}, 1_700_000_000.0, 1800, 7200
        )
        self.assertEqual(report["members"], [])
        self.assertEqual(
            report["summary"], {"total": 0, "down": 0, "stuck": 0, "blocked": 0}
        )

    def test_retired_survives_restart_without_resurrection(self) -> None:
        host = FakeHost()
        del host.sessions["session-1"]["workspace"]
        asyncio.run(host.retire("lane"))
        # Simulate a daemon restart: fresh host, retired set reloaded from
        # disk, in-memory roster gone. The served session still exists
        # server-side, but must never rejoin the roster.
        restarted = FakeHost()
        restarted.retired = muse_msp.load_retired()
        restarted.sessions = {}
        restarted.aliases = {}
        listed = asyncio.run(restarted.list_sessions())
        self.assertEqual(listed["sessions"], [])
        # A late served frame for the retired session is logged, not re-tracked.
        asyncio.run(
            restarted._notification(
                "session/statusChanged",
                {"sessionId": "session-1", "status": "idle", "attention": []},
            )
        )
        self.assertNotIn("session-1", restarted.sessions)
        # Retiring twice reports the lane already gone, typed.
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(restarted.retire("session-1"))
        self.assertEqual(ctx.exception.kind, "sessionNotFound")
        # Even stale carried-over roster state cannot resurrect it.
        restarted.sessions = {
            "session-1": {"sessionId": "session-1", "alias": "lane", "status": "idle"}
        }
        listed = asyncio.run(restarted.list_sessions())
        self.assertEqual(listed["sessions"], [])

    def test_new_work_refused_after_retire(self) -> None:
        host = FakeHost()
        del host.sessions["session-1"]["workspace"]
        asyncio.run(host.retire("lane"))
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.submit("session-1", "hello?"))
        self.assertEqual(ctx.exception.kind, "sessionRetired")
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(
                host.supervised_call("turn/start", {"sessionId": "session-1"})
            )
        self.assertEqual(ctx.exception.kind, "sessionRetired")


class WorktreeDirtyTest(unittest.TestCase):
    def test_status_shapes(self) -> None:
        def run_clean(argv, **kw):
            class Proc:
                returncode = 0
                stdout = ""
            return Proc()

        def run_dirty(argv, **kw):
            class Proc:
                returncode = 0
                stdout = " M scripts/muse-msp.py\n"
            return Proc()

        def run_broken(argv, **kw):
            class Proc:
                returncode = 128
                stdout = ""
            return Proc()

        self.assertFalse(muse_msp.worktree_dirty("/ws", run=run_clean))
        self.assertTrue(muse_msp.worktree_dirty("/ws", run=run_dirty))
        self.assertIsNone(muse_msp.worktree_dirty("/ws", run=run_broken))
        self.assertIsNone(muse_msp.worktree_dirty(None))

        def run_raises(argv, **kw):
            raise OSError("no git")

        self.assertIsNone(muse_msp.worktree_dirty("/ws", run=run_raises))


if __name__ == "__main__":
    unittest.main()
