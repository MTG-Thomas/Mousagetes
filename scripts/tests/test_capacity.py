#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for host capacity planning (issue #19): lifecycle unload path,
load-based dispatch gating, and pre-ceiling alerting.

Stdlib unittest only. Covers posture boundaries, plan/reconcile gating
with cited evidence, the host.capacity.warning event below the rejection
point, and the attributable unload path (shared retire guards, no
override) — against a fake host and isolated files, never a daemon.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import inspect
import json
import os
import tempfile
import time
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


NOW = 1_700_000_000.0


def issue(number, **over):
    entry = {
        "number": number,
        "title": f"Work item {number}",
        "labels": [],
        "state": "open",
        "updatedAt": NOW,
        "body": f"Do work item {number}.",
    }
    entry.update(over)
    return entry


def board_of(*issues):
    return {"repo": "OWNER/REPO", "issues": list(issues)}


class IsolatedFilesMixin:
    def setUp(self) -> None:
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.old_files = {}
        for name in ("CLAIMS_FILE", "BUS_LOG", "HOSTS_FILE", "EVENTS"):
            self.old_files[name] = getattr(muse_msp, name)
            setattr(muse_msp, name, Path(self.tmp.name) / f"{name.lower()}.ndjson")
        self.old_env = dict(os.environ)

    def tearDown(self) -> None:
        for name, value in self.old_files.items():
            setattr(muse_msp, name, value)
        os.environ.clear()
        os.environ.update(self.old_env)
        self.tmp.cleanup()
        super().tearDown()


class PostureTest(IsolatedFilesMixin, unittest.TestCase):
    def test_boundaries(self) -> None:
        self.assertEqual(muse_msp.capacity_posture(14, 20, 15)["posture"], "open")
        self.assertEqual(muse_msp.capacity_posture(15, 20, 15)["posture"], "limited")
        self.assertEqual(muse_msp.capacity_posture(19, 20, 15)["posture"], "limited")
        self.assertEqual(muse_msp.capacity_posture(20, 20, 15)["posture"], "closed")
        self.assertEqual(muse_msp.capacity_posture(32, 20, 15)["posture"], "closed")

    def test_defaults_derive_from_policy(self) -> None:
        posture = muse_msp.capacity_posture(0)
        self.assertEqual(posture["ceiling"], 20)
        self.assertEqual(posture["warnAt"], 15)
        self.assertEqual(posture["posture"], "open")

    def test_evidence_cites_numbers(self) -> None:
        evidence = muse_msp.capacity_posture(20, 20, 15)["evidence"]
        self.assertIn("20", evidence)
        self.assertIn("ceiling 20", evidence)
        self.assertIn("closed", evidence)

    def test_env_overrides_and_invalid_fallback(self) -> None:
        os.environ["M8S_SESSION_CEILING"] = "8"
        os.environ["M8S_SESSION_WARN_AT"] = "6"
        posture = muse_msp.capacity_posture(6)
        self.assertEqual((posture["ceiling"], posture["warnAt"]), (8, 6))
        self.assertEqual(posture["posture"], "limited")
        os.environ["M8S_SESSION_CEILING"] = "junk"
        os.environ["M8S_SESSION_WARN_AT"] = "junk"
        posture = muse_msp.capacity_posture(0)
        self.assertEqual((posture["ceiling"], posture["warnAt"]), (20, 15))

    def test_bad_loaded_is_typed(self) -> None:
        with self.assertRaises(muse_msp.BoardError) as ctx:
            muse_msp.capacity_posture("many")
        self.assertEqual(ctx.exception.kind, "badCapacity")

    def test_loaded_proxy_counts_live_leases_with_sessions(self) -> None:
        claims = {
            "lane/a": {"live": True, "sessionId": "s-1"},
            "lane/b": {"live": True, "sessionId": "s-2"},
            "lane/c": {"live": False, "sessionId": "s-3"},
            "lane/d": {"live": True},
        }
        self.assertEqual(muse_msp.capacity_loaded_from_claims(claims), 2)
        self.assertEqual(muse_msp.capacity_loaded_from_claims({}), 0)


class GateTest(IsolatedFilesMixin, unittest.TestCase):
    def plan(self, **kw):
        board = board_of(issue(1), issue(2), issue(3))
        return muse_msp.plan_board(board, {}, NOW, **kw)

    def test_open_leaves_schedule_alone(self) -> None:
        plan = self.plan(loaded=5, ceiling=20, warn_at=15)
        self.assertEqual(plan["capacity"]["posture"], "open")
        self.assertTrue(all(spec["status"] == "scheduled" for spec in plan["specs"]))

    def test_closed_parks_everything_with_evidence(self) -> None:
        plan = self.plan(loaded=20, ceiling=20, warn_at=15)
        self.assertEqual(plan["capacity"]["posture"], "closed")
        self.assertTrue(all(spec["status"] == "queued" for spec in plan["specs"]))
        for spec in plan["specs"]:
            self.assertIn("capacity gate (closed)", spec["queuedBehind"])
            self.assertIn("20 loaded session(s) vs ceiling 20", spec["queuedBehind"])

    def test_limited_admits_only_top_rank(self) -> None:
        plan = self.plan(loaded=15, ceiling=20, warn_at=15)
        self.assertEqual(plan["capacity"]["posture"], "limited")
        statuses = [spec["status"] for spec in plan["specs"]]
        self.assertEqual(statuses, ["scheduled", "queued", "queued"])
        self.assertIn("capacity gate (limited)", plan["specs"][1]["queuedBehind"])

    def test_gate_spares_decision_blocked_and_collisions(self) -> None:
        board = board_of(
            issue(1, labels=["needs-product-call"]),
            issue(2, scope={"files": ["a.py"]}),
            issue(3, scope={"files": ["a.py"]}),
        )
        plan = muse_msp.plan_board(board, {}, NOW, loaded=20, ceiling=20, warn_at=15)
        by_number = {spec["issue"]: spec for spec in plan["specs"]}
        self.assertEqual(by_number[1]["status"], "decision-blocked")
        # Collision-queued spec keeps its collision citation, not the gate.
        self.assertEqual(by_number[3]["status"], "queued")
        self.assertNotIn("capacity gate", by_number[3]["queuedBehind"])
        self.assertIn("capacity gate (closed)", by_number[2]["queuedBehind"])


class ReconcileGateTest(IsolatedFilesMixin, unittest.TestCase):
    def test_closed_refuses_new_lanes_keeps_sync_and_page(self) -> None:
        board = board_of(
            issue(1),
            issue(2),
            issue(3, labels=["needs-product-call"]),
        )
        spec_branch = "lane/m8s-2-work-item-2"
        claims = {
            spec_branch: {
                "live": True,
                "host": "h",
                "lane": "m8s-2",
                "branch": spec_branch,
                "checkout": "/tmp/x",
                "sessionId": "s-2",
            }
        }
        plan = muse_msp.board_reconcile(
            board, claims, [], NOW, loaded=20, ceiling=20, warn_at=15
        )
        actions = {item["issue"]: item for item in plan["actions"]}
        # Issue 2 rides its live lease; nothing new proposes past the gate.
        self.assertEqual(actions[2]["action"], "in-sync")
        self.assertEqual(actions[1]["action"], "queued")
        self.assertIn("capacity gate (closed)", actions[1]["reason"])
        self.assertIn("20 loaded session(s) vs ceiling 20", actions[1]["reason"])
        self.assertEqual(actions[3]["action"], "page-human")
        self.assertFalse(
            [item for item in plan["actions"] if item["action"] == "propose"]
        )


class WarningEventTest(IsolatedFilesMixin, unittest.TestCase):
    def snapshot_file(self, *issues) -> str:
        path = Path(self.tmp.name) / "board.json"
        path.write_text(json.dumps(board_of(*issues)), encoding="utf-8")
        return str(path)

    def reconcile(self, path, **kw):
        args = argparse.Namespace(
            board_action="reconcile",
            board=path,
            events_limit=200,
            loaded=kw.get("loaded"),
            ceiling=kw.get("ceiling", 20),
            warn_at=kw.get("warn_at", 15),
        )
        return muse_msp.board_main(args)

    def warnings(self):
        return [
            event
            for event in muse_msp.read_events(0, 200)
            if event.get("kind") == "host.capacity.warning"
        ]

    def test_warning_fires_below_rejection_point(self) -> None:
        path = self.snapshot_file(issue(1), issue(2))
        result = self.reconcile(path, loaded=15)
        plan = result["reconcile"]
        self.assertEqual(plan["capacity"]["posture"], "limited")
        # Signal, not rejection: the top lane still proposes at limited.
        proposes = [a for a in plan["actions"] if a["action"] == "propose"]
        self.assertEqual(len(proposes), 1)
        events = self.warnings()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["loaded"], 15)
        self.assertEqual(events[0]["posture"], "limited")

    def test_warning_fires_at_ceiling_with_no_proposes(self) -> None:
        path = self.snapshot_file(issue(1), issue(2))
        result = self.reconcile(path, loaded=20)
        plan = result["reconcile"]
        self.assertEqual(plan["capacity"]["posture"], "closed")
        self.assertFalse([a for a in plan["actions"] if a["action"] == "propose"])
        events = self.warnings()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["posture"], "closed")

    def test_open_is_silent(self) -> None:
        path = self.snapshot_file(issue(1))
        result = self.reconcile(path, loaded=5)
        plan = result["reconcile"]
        self.assertEqual(plan["capacity"]["posture"], "open")
        self.assertTrue([a for a in plan["actions"] if a["action"] == "propose"])
        self.assertEqual(self.warnings(), [])

    def test_cli_flags_parse(self) -> None:
        args = parse(
            ["board", "reconcile", "--board", "b.json", "--loaded", "19",
             "--ceiling", "20", "--warn-at", "15"]
        )
        self.assertEqual((args.loaded, args.ceiling, args.warn_at), (19, 20, 15))
        args = parse(["board", "plan", "--board", "b.json"])
        self.assertEqual((args.loaded, args.ceiling, args.warn_at), (None, None, None))


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
                "lastActivity": NOW - 7200.0,
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
        if method == "turn/start":
            return {"turnId": "turn-9", "ok": True}
        return {"ok": True}


class UnloadTest(unittest.TestCase):
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

    def test_idle_unload_is_attributed(self) -> None:
        host = FakeHost()
        result = asyncio.run(host.unload("lane", reason="capacity", by="op"))
        self.assertTrue(result["unloaded"])
        self.assertFalse(result["forced"])
        self.assertEqual((result["by"], result["reason"]), ("op", "capacity"))
        self.assertNotIn("session-1", host.sessions)
        self.assertNotIn("lane", host.aliases)
        kinds = [event["kind"] for event in host.events]
        self.assertIn("lane.unloaded", kinds)
        event = next(e for e in host.events if e["kind"] == "lane.unloaded")
        self.assertEqual(event["by"], "op")
        self.assertEqual(event["reason"], "capacity")
        self.assertEqual((event["loadedBefore"], event["loadedAfter"]), (1, 0))

    def test_shared_guards_refuse_both_paths(self) -> None:
        # One assessment path: dirty blocks retire and unload alike.
        muse_msp.worktree_dirty = lambda workspace, run=None: True  # type: ignore[assignment]
        for command in ("retire", "unload"):
            host = FakeHost()
            with self.assertRaises(muse_msp.RetireError) as ctx:
                if command == "retire":
                    asyncio.run(host.retire("lane"))
                else:
                    asyncio.run(host.unload("lane", by="op"))
            self.assertEqual(ctx.exception.kind, "uncommittedWork")
        host = FakeHost()
        checks = asyncio.run(host._departure_checks("lane"))
        self.assertEqual(checks["blocking"][0][0], "uncommittedWork")

    def test_busy_refuses_unload(self) -> None:
        host = FakeHost()
        host.pending_reply = {"approvals": [{"id": "a-1"}]}
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.unload("lane", by="op"))
        self.assertEqual(ctx.exception.kind, "sessionBusy")

    def test_live_turn_refuses_unload(self) -> None:
        host = FakeHost()
        host.sessions["session-1"]["activeTurn"] = "turn-9"
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.unload("lane", by="op"))
        self.assertEqual(ctx.exception.kind, "sessionBusy")
        self.assertIn("live turn", str(ctx.exception))

    def test_standby_window_refuses_fresh_activity(self) -> None:
        host = FakeHost()
        host.sessions["session-1"]["lastActivity"] = time.time()
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.unload("lane", by="op"))
        self.assertEqual(ctx.exception.kind, "standby")

    def test_missing_activity_signal_refuses(self) -> None:
        host = FakeHost()
        del host.sessions["session-1"]["lastActivity"]
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.unload("lane", by="op"))
        self.assertEqual(ctx.exception.kind, "standby")

    def test_unknown_session_refuses(self) -> None:
        host = FakeHost()
        with self.assertRaises(muse_msp.RetireError) as ctx:
            asyncio.run(host.unload("nope", by="op"))
        self.assertEqual(ctx.exception.kind, "sessionNotFound")

    def test_unload_has_no_force_override(self) -> None:
        self.assertNotIn("force", inspect.signature(FakeHost.unload).parameters)

    def test_active_turn_marker_lifecycle(self) -> None:
        host = FakeHost()
        asyncio.run(host.submit("lane", "do work"))
        self.assertEqual(host.sessions["session-1"].get("activeTurn"), "turn-9")
        asyncio.run(
            host._notification("turn/completed", {"sessionId": "session-1", "terminal": "done"})
        )
        self.assertNotIn("activeTurn", host.sessions["session-1"])

    def test_cli_builds_unload_request(self) -> None:
        req = muse_msp.build_request(parse(["unload", "lane"]))
        self.assertEqual(
            req, {"command": "unload", "session": "lane", "reason": "capacity", "by": None}
        )
        req = muse_msp.build_request(parse(["unload", "lane", "--reason", "shed", "--by", "op"]))
        self.assertEqual(
            req, {"command": "unload", "session": "lane", "reason": "shed", "by": "op"}
        )

    def test_dispatch_unload_reaches_host(self) -> None:
        host = FakeHost()
        result = asyncio.run(
            muse_msp.dispatch(
                host, {"command": "unload", "session": "lane", "reason": "capacity", "by": "op"}
            )
        )
        self.assertTrue(result["unloaded"])
        self.assertEqual(result["sessionId"], "session-1")


if __name__ == "__main__":
    unittest.main()
