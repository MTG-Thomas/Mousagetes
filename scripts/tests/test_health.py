#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for `m8s health`: fused swarm health screen (issue #11).

Stdlib unittest only. Exercises the pure fusion core (liveness +
responsiveness + progress fused per member) and the exactly-three flags
(down / stuck / blocked) plus deterministic one-screen rendering — never
starts a daemon, never touches the network, never shells to gh.
"""

from __future__ import annotations

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

NOW = 1_700_000_000.0
STUCK_AFTER = 1800
DOWN_AFTER = 7200


def session(sid, **over):
    state = {"sessionId": sid, "alias": sid, "status": "running"}
    state.update(over)
    return state


def event(kind, sid, ago, **over):
    record = {"kind": kind, "at": NOW - ago, "sessionId": sid}
    record.update(over)
    return record


def fuse(sessions, events=(), pending=None, progress=None, **kw):
    return muse_msp.fuse_health(
        sessions,
        list(events),
        pending or {},
        progress or {},
        NOW,
        kw.get("stuck_after", STUCK_AFTER),
        kw.get("down_after", DOWN_AFTER),
    )


class FuseShapeTest(unittest.TestCase):
    def test_empty_swarm_renders_summary(self) -> None:
        report = fuse([])
        self.assertEqual(report["members"], [])
        self.assertEqual(
            report["summary"],
            {"total": 0, "down": 0, "stuck": 0, "blocked": 0},
        )
        screen = muse_msp.format_health(report, NOW)
        self.assertIn("0 members", screen)

    def test_stable_member_order_regardless_of_input(self) -> None:
        sessions = [session("lane-b"), session("lane-a"), session("lane-c")]
        first = [m["member"] for m in fuse(sessions)["members"]]
        second = [
            m["member"]
            for m in fuse(list(reversed(sessions)))["members"]
        ]
        self.assertEqual(first, ["lane-a", "lane-b", "lane-c"])
        self.assertEqual(first, second)

    def test_one_row_per_member_plus_flags_summary(self) -> None:
        sessions = [session("lane-a"), session("lane-b")]
        report = fuse(sessions)
        self.assertEqual(len(report["members"]), 2)
        screen = muse_msp.format_health(report, NOW)
        rows = [line for line in screen.splitlines() if "lane-" in line]
        self.assertEqual(len(rows), 2)
        self.assertIn("summary", screen.splitlines()[-1].lower())


class DownFlagTest(unittest.TestCase):
    def test_listed_but_long_silent_is_down(self) -> None:
        sessions = [session("lane-a", lastActivity=NOW - 8000)]
        events = [event("msp.event", "lane-a", 8000)]
        report = fuse(sessions, events)
        self.assertEqual(report["members"][0]["flag"], "down")
        self.assertEqual(report["summary"]["down"], 1)

    def test_never_observed_lane_is_down_not_stuck(self) -> None:
        report = fuse([session("lane-a")])
        self.assertEqual(report["members"][0]["flag"], "down")

    def test_recently_active_lane_is_not_down(self) -> None:
        sessions = [session("lane-a", lastActivity=NOW - 60)]
        events = [event("turn.submitted", "lane-a", 60)]
        report = fuse(sessions, events)
        self.assertIsNone(report["members"][0]["flag"])


class StuckFlagTest(unittest.TestCase):
    def test_running_with_no_events_and_no_commits_is_stuck(self) -> None:
        sessions = [
            session(
                "lane-a",
                lastActivity=NOW - 2000,
                lastRepoActivity=NOW - 2000,
            )
        ]
        events = [event("turn.submitted", "lane-a", 2000)]
        report = fuse(sessions, events)
        self.assertEqual(report["members"][0]["flag"], "stuck")
        self.assertEqual(report["summary"]["stuck"], 1)

    def test_recent_commits_forgive_transcript_idle(self) -> None:
        sessions = [
            session(
                "lane-a",
                lastActivity=NOW - 2000,
                lastRepoActivity=NOW - 60,
            )
        ]
        events = [event("turn.submitted", "lane-a", 2000)]
        report = fuse(sessions, events)
        self.assertIsNone(report["members"][0]["flag"])

    def test_failed_turn_needing_owner_action_is_stuck(self) -> None:
        sessions = [
            session(
                "lane-a",
                lastActivity=NOW - 60,
                needsOwnerAction=True,
                lastTerminal="failed",
            )
        ]
        events = [event("turn.submitted", "lane-a", 60)]
        report = fuse(sessions, events)
        self.assertEqual(report["members"][0]["flag"], "stuck")


class BlockedFlagTest(unittest.TestCase):
    def test_pending_approval_is_blocked(self) -> None:
        sessions = [session("lane-a", lastActivity=NOW - 30)]
        events = [event("turn.submitted", "lane-a", 30)]
        pending = {"lane-a": {"approvals": 1, "inputs": 0}}
        report = fuse(sessions, events, pending=pending)
        self.assertEqual(report["members"][0]["flag"], "blocked")
        self.assertEqual(report["summary"]["blocked"], 1)

    def test_pending_user_input_is_blocked(self) -> None:
        sessions = [session("lane-a", lastActivity=NOW - 30)]
        pending = {"lane-a": {"approvals": 0, "inputs": 2}}
        report = fuse(sessions, pending=pending)
        self.assertEqual(report["members"][0]["flag"], "blocked")

    def test_attention_hint_is_blocked(self) -> None:
        sessions = [
            session("lane-a", lastActivity=NOW - 30, attention=["approvalPending"])
        ]
        report = fuse(sessions)
        self.assertEqual(report["members"][0]["flag"], "blocked")


class FlagPriorityTest(unittest.TestCase):
    def test_blocked_beats_down_on_silent_lane(self) -> None:
        sessions = [session("lane-a", lastActivity=NOW - 9000)]
        events = [event("msp.event", "lane-a", 9000)]
        pending = {"lane-a": {"approvals": 1, "inputs": 0}}
        report = fuse(sessions, events, pending=pending)
        self.assertEqual(report["members"][0]["flag"], "blocked")

    def test_down_beats_stuck_on_long_silence(self) -> None:
        sessions = [
            session(
                "lane-a",
                lastActivity=NOW - 9000,
                lastRepoActivity=NOW - 9000,
            )
        ]
        events = [event("turn.submitted", "lane-a", 9000)]
        report = fuse(sessions, events)
        self.assertEqual(report["members"][0]["flag"], "down")

    def test_no_invented_states(self) -> None:
        sessions = [
            session("ok-lane", lastActivity=NOW - 60),
            session("down-lane", lastActivity=NOW - 9000),
            session(
                "stuck-lane",
                lastActivity=NOW - 2000,
                lastRepoActivity=NOW - 2000,
            ),
            session(
                "blocked-lane",
                lastActivity=NOW - 60,
                attention=["inputPending"],
            ),
        ]
        events = [
            event("turn.submitted", "ok-lane", 60),
            event("msp.event", "down-lane", 9000),
            event("turn.submitted", "stuck-lane", 2000),
            event("turn.submitted", "blocked-lane", 60),
        ]
        report = fuse(sessions, events)
        flags = {m["flag"] for m in report["members"]}
        self.assertLessEqual(flags, {"down", "stuck", "blocked", None})


class FusionColumnsTest(unittest.TestCase):
    def test_liveness_uses_last_event_age(self) -> None:
        sessions = [session("lane-a", lastActivity=NOW - 300)]
        events = [event("msp.event", "lane-a", 120)]
        row = fuse(sessions, events)["members"][0]
        self.assertEqual(row["lastEventAgeSeconds"], 120)

    def test_responsiveness_counts_recent_turns(self) -> None:
        sessions = [session("lane-a", lastActivity=NOW - 60)]
        events = [
            event("turn.submitted", "lane-a", 100),
            event(
                "msp.event",
                "lane-a",
                200,
                method="turn/completed",
                params={"sessionId": "lane-a"},
            ),
            event("turn.submitted", "lane-a", 5000),
        ]
        row = fuse(sessions, events)["members"][0]
        self.assertEqual(row["turnsRecent"], 2)
        self.assertEqual(row["lastTurnAgeSeconds"], 100)

    def test_progress_columns_pass_through(self) -> None:
        sessions = [session("lane-a", lastActivity=NOW - 60)]
        progress = {
            "lane-a": {
                "branch": "lane/a",
                "ahead": 2,
                "pr": {"number": 7, "checks": "passing"},
                "pending": {"approvals": 0, "inputs": 0},
            }
        }
        row = fuse(sessions, progress=progress)["members"][0]
        self.assertEqual(row["branch"], "lane/a")
        self.assertEqual(row["ahead"], 2)
        self.assertEqual(row["pr"], {"number": 7, "checks": "passing"})

    def test_nested_params_session_id_matches_events(self) -> None:
        sessions = [session("lane-a", lastActivity=NOW - 9000)]
        events = [
            event(
                "msp.event",
                None,
                9000,
                method="session/statusChanged",
                params={"sessionId": "lane-a"},
            )
        ]
        row = fuse(sessions, events)["members"][0]
        self.assertEqual(row["flag"], "down")
        self.assertEqual(row["lastEventAgeSeconds"], 9000)


class PendingCountsTest(unittest.TestCase):
    def test_counts_lists_defensively(self) -> None:
        counts = muse_msp.pending_counts(
            {"approvals": [{"id": "a"}], "userInputs": []}, [], []
        )
        self.assertEqual(counts, {"approvals": 1, "inputs": 0})

    def test_unparseable_result_falls_back_to_signals(self) -> None:
        blocker = event("blocker", "lane-a", 60, reason="approval/request")
        counts = muse_msp.pending_counts(
            {"unexpected": "shape"}, ["inputPending"], [blocker]
        )
        self.assertEqual(counts["approvals"], 1)
        self.assertEqual(counts["inputs"], 1)

    def test_clean_lane_has_no_pending(self) -> None:
        self.assertEqual(
            muse_msp.pending_counts({}, [], []),
            {"approvals": 0, "inputs": 0},
        )


class ProgressCollectorsTest(unittest.TestCase):
    def test_git_progress_uses_injected_runner(self) -> None:
        def run(argv, **kw):
            class Proc:
                returncode = 0
                stdout = "lane/a\n" if "rev-parse" in argv else "3\n"
                stderr = ""
            return Proc()

        progress = muse_msp.health_progress_for_workspace("/ws", run=run)
        self.assertEqual(progress, {"branch": "lane/a", "ahead": 3})

    def test_git_failure_degrades_to_unknown(self) -> None:
        def run(argv, **kw):
            raise OSError("no git")

        self.assertEqual(
            muse_msp.health_progress_for_workspace("/ws", run=run),
            {"branch": None, "ahead": None},
        )

    def test_gh_pr_checks_use_injected_runner(self) -> None:
        def run(argv, **kw):
            self.assertEqual(argv[:3], ["gh", "pr", "list"])
            class Proc:
                returncode = 0
                stdout = (
                    '[{"number": 7, "url": "https://example.invalid/x/7", '
                    '"statusCheckRollup": [{"conclusion": "SUCCESS"}]}]'
                )
                stderr = ""
            return Proc()

        pr = muse_msp.health_pr_for_branch("lane/a", "/ws", run=run)
        self.assertEqual(
            pr,
            {
                "number": 7,
                "url": "https://example.invalid/x/7",
                "checks": "passing",
            },
        )

    def test_gh_failure_means_no_pr(self) -> None:
        def run(argv, **kw):
            class Proc:
                returncode = 1
                stdout = ""
                stderr = "not found"
            return Proc()

        self.assertIsNone(muse_msp.health_pr_for_branch("lane/a", "/ws", run=run))
        self.assertIsNone(muse_msp.health_pr_for_branch(None, "/ws", run=run))


class HealthDispatchTest(unittest.TestCase):
    def test_health_command_fuses_without_daemon(self) -> None:
        import asyncio

        class FakeHost(muse_msp.MspHost):
            def __init__(self) -> None:
                self.calls = []
                self.aliases = {"lane-a": "lane-a"}
                self.sessions = {
                    "lane-a": {
                        "sessionId": "lane-a",
                        "alias": "lane-a",
                        "status": "running",
                        "lastActivity": NOW - 2000,
                        "lastRepoActivity": NOW - 2000,
                    }
                }
                self.budgets = {}
                self.watchers = set()

            def record(self, record) -> None:
                pass

            async def call(self, method, params=None):
                self.calls.append(method)
                if method == "session/list":
                    return {"sessions": [{"sessionId": "lane-a"}]}
                if method == "approval/listPending":
                    return {"approvals": [], "userInputs": []}
                raise AssertionError(f"unexpected call: {method}")

        saved = muse_msp.read_events
        muse_msp.read_events = lambda after=0.0, limit=200: []  # type: ignore[assignment]
        try:
            async def go():
                host = FakeHost()
                result = await muse_msp.dispatch(host, {"command": "health"})
                return host, result

            host, result = asyncio.run(go())
        finally:
            muse_msp.read_events = saved
        # No events at all, so the long-silent lane fuses to down
        # (listed but unresponsive), not stuck.
        self.assertEqual(result["summary"]["total"], 1)
        self.assertEqual(result["members"][0]["flag"], "down")
        self.assertIn("session/list", host.calls)
        self.assertIn("approval/listPending", host.calls)

    def test_health_cli_request(self) -> None:
        args = muse_msp.parser().parse_args(["health"])
        self.assertEqual(
            muse_msp.build_request(args), {"command": "health", "eventsLimit": 2000}
        )
        args = muse_msp.parser().parse_args(["health", "--events-limit", "50"])
        self.assertEqual(
            muse_msp.build_request(args), {"command": "health", "eventsLimit": 50}
        )


class UnloadedLaneTest(unittest.TestCase):
    """A notLoaded (merged/reaped) lane is a lifecycle state, not a stall.

    Regression for the web board flagging merged-and-reaped lanes as STUCK
    because transcript idleness alone was judged.
    """

    def _run(self, status):
        import asyncio

        class FakeHost(muse_msp.MspHost):
            def __init__(self) -> None:
                self.records = []
                self.sessions = {
                    "lane-a": {
                        "sessionId": "lane-a",
                        "alias": "lane-a",
                        "status": status,
                        "lastActivity": NOW - 100_000,
                    }
                }
                self.budgets = {}
                self.watchers = set()

            def record(self, record) -> None:
                self.records.append(record)

            async def call(self, method, params=None):
                if method == "session/list":
                    return {"sessions": [{"sessionId": "lane-a", "status": status}]}
                raise AssertionError(f"unexpected call: {method}")

        async def go():
            host = FakeHost()
            return host, await host.list_sessions()

        return asyncio.run(go())

    def test_not_loaded_lane_is_not_stuck(self) -> None:
        host, result = self._run("notLoaded")
        row = result["sessions"][0]
        self.assertTrue(row["unloaded"])
        self.assertIsNone(row["stuck"])
        self.assertEqual([], [r for r in host.records if r["kind"] == "lane.stuck"])

    def test_loaded_idle_lane_is_still_stuck(self) -> None:
        host, result = self._run("idle")
        row = result["sessions"][0]
        self.assertFalse(row["unloaded"])
        self.assertIsNotNone(row["stuck"])
        self.assertEqual(row["stuck"]["reason"], "idle")
        self.assertIn("lane.stuck", [r["kind"] for r in host.records])


class PrChecksTest(unittest.TestCase):
    def test_maps_rollup_to_state(self) -> None:
        self.assertEqual(
            muse_msp.pr_check_state(
                [
                    {"status": "COMPLETED", "conclusion": "SUCCESS"},
                    {"status": "COMPLETED", "conclusion": "SUCCESS"},
                ]
            ),
            "passing",
        )
        self.assertEqual(
            muse_msp.pr_check_state(
                [{"status": "COMPLETED", "conclusion": "FAILURE"}]
            ),
            "failing",
        )
        self.assertEqual(
            muse_msp.pr_check_state([{"status": "IN_PROGRESS"}]),
            "pending",
        )
        self.assertEqual(muse_msp.pr_check_state([]), "pending")
        self.assertIsNone(muse_msp.pr_check_state(None))


if __name__ == "__main__":
    unittest.main()
