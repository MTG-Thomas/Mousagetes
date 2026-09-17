#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for Moonshot P5: board-to-lane compiler.

Stdlib unittest only. Exercises deterministic priority arbitration,
lane-spec emission, P4-lease collision queuing, decision-blocked paging,
the gh-driven exporter (fake runner, no network), and the reconcile loop
— against isolated runtime files, never a daemon or the network.
"""

from __future__ import annotations

import importlib.util
import json
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
DAY = 86400.0


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


class IsolatedFilesMixin:
    def setUp(self) -> None:
        import tempfile

        self.tmp = tempfile.TemporaryDirectory()
        self.old = {}
        for name in ("CLAIMS_FILE", "BUS_LOG", "HOSTS_FILE", "EVENTS"):
            self.old[name] = getattr(muse_msp, name)
            setattr(muse_msp, name, Path(self.tmp.name) / f"{name.lower()}.ndjson")

    def tearDown(self) -> None:
        for name, value in self.old.items():
            setattr(muse_msp, name, value)
        self.tmp.cleanup()


class ArbitrationTest(unittest.TestCase):
    def test_priority_weights_and_aliases(self) -> None:
        self.assertEqual(muse_msp.board_priority(["priority-critical"])[0], 4)
        self.assertEqual(muse_msp.board_priority(["p0"])[0], 4)
        self.assertEqual(muse_msp.board_priority(["priority-high"])[0], 3)
        self.assertEqual(muse_msp.board_priority(["p1"])[0], 3)
        self.assertEqual(muse_msp.board_priority(["priority-medium"])[0], 2)
        self.assertEqual(muse_msp.board_priority(["priority-low"])[0], 1)
        self.assertEqual(muse_msp.board_priority(["p3"])[0], 1)
        self.assertEqual(muse_msp.board_priority([])[0], 1)
        self.assertEqual(muse_msp.board_priority(["docs"])[0], 1)
        # Highest label wins, and the citation names the source.
        weight, why = muse_msp.board_priority(["priority-low", "priority-high"])
        self.assertEqual(weight, 3)
        self.assertIn("priority-high", why)

    def test_score_formula_priority_times_depth_times_staleness(self) -> None:
        board = {
            "issues": [
                issue(1, labels=["priority-high"], updatedAt=NOW - 30 * DAY),
                issue(2, labels=["priority-low"], updatedAt=NOW),
            ]
        }
        specs = muse_msp.compile_board(board, NOW)["specs"]
        by_number = {spec["issue"]: spec for spec in specs}
        # high(3) x (1+0) x stale(2.0) = 6.0; low(1) x 1 x fresh(1.0) = 1.0.
        self.assertEqual(by_number[1]["priority"]["score"], 6.0)
        self.assertEqual(by_number[2]["priority"]["score"], 1.0)
        self.assertEqual([spec["issue"] for spec in specs], [1, 2])

    def test_blockers_outrank_the_blocked(self) -> None:
        board = {
            "issues": [
                issue(1, labels=["priority-low"]),
                issue(2, labels=["priority-low"], dependsOn=[1]),
                issue(3, labels=["priority-low"], dependsOn=[2]),
            ]
        }
        specs = muse_msp.compile_board(board, NOW)["specs"]
        self.assertEqual([spec["issue"] for spec in specs], [1, 2, 3])
        depths = {spec["issue"]: spec["priority"]["depth"] for spec in specs}
        self.assertEqual(depths, {1: 2, 2: 1, 3: 0})
        self.assertEqual(specs[1]["blockedBy"], [1])

    def test_closed_deps_do_not_count(self) -> None:
        board = {
            "issues": [
                issue(1, labels=["priority-low"], state="closed"),
                issue(2, labels=["priority-low"], dependsOn=[1]),
            ]
        }
        compiled = muse_msp.compile_board(board, NOW)
        self.assertEqual([spec["issue"] for spec in compiled["specs"]], [2])
        self.assertEqual(compiled["specs"][0]["priority"]["depth"], 0)
        self.assertEqual(compiled["specs"][0]["blockedBy"], [])

    def test_cycles_resolve_deterministically_and_are_reported(self) -> None:
        board = {
            "issues": [issue(1, dependsOn=[2]), issue(2, dependsOn=[1])]
        }
        first = muse_msp.compile_board(board, NOW)
        second = muse_msp.compile_board(board, NOW)
        self.assertEqual(first["specs"], second["specs"])
        self.assertEqual(first["dependencyCycles"], [[1, 2]])

    def test_ties_break_by_issue_number(self) -> None:
        board = {"issues": [issue(9), issue(3)]}
        specs = muse_msp.compile_board(board, NOW)["specs"]
        self.assertEqual([spec["issue"] for spec in specs], [3, 9])

    def test_every_spec_cites_why_it_was_scheduled(self) -> None:
        specs = muse_msp.compile_board({"issues": [issue(1)]}, NOW)["specs"]
        why = specs[0]["priority"]["why"]
        self.assertEqual(len(why), 3)
        self.assertTrue(all(isinstance(line, str) and line for line in why))

    def test_iso_timestamps_parse_like_gh_output(self) -> None:
        parsed = muse_msp.parse_board_time("2026-09-17T16:10:56Z")
        self.assertIsNotNone(parsed)
        later = parsed + 400 * 86400.0
        idle, factor = muse_msp.board_staleness("2026-09-17T16:10:56Z", later)
        self.assertGreater(idle, 300)
        self.assertGreater(factor, 10)
        self.assertIsNone(muse_msp.parse_board_time("not-a-time"))
        self.assertIsNone(muse_msp.parse_board_time(True))
        idle, factor = muse_msp.board_staleness("not-a-time", NOW)
        self.assertEqual((idle, factor), (0.0, 1.0))


class LaneSpecTest(unittest.TestCase):
    def test_spec_shape_and_defaults(self) -> None:
        specs = muse_msp.compile_board({"issues": [issue(7)]}, NOW)["specs"]
        spec = specs[0]
        self.assertEqual(spec["lane"], "m8s-7")
        self.assertEqual(spec["branch"], "lane/m8s-7-work-item-7")
        self.assertIsNone(spec["checkout"])
        self.assertEqual(spec["files"], [])
        self.assertEqual(spec["brief"], "Do work item 7.")
        self.assertEqual(spec["status"], "candidate")
        for key in ("score", "weight", "depth", "stalenessDays", "why"):
            self.assertIn(key, spec["priority"])

    def test_explicit_scope_and_branch_survive(self) -> None:
        board = {
            "issues": [
                issue(
                    1,
                    scope={
                        "branch": "lane/x",
                        "checkout": "/repo/x",
                        "files": ["a.py"],
                    },
                )
            ]
        }
        spec = muse_msp.compile_board(board, NOW)["specs"][0]
        self.assertEqual(spec["branch"], "lane/x")
        self.assertEqual(spec["checkout"], "/repo/x")
        self.assertEqual(spec["files"], ["a.py"])

    def test_bad_board_shapes_raise_typed_errors(self) -> None:
        with self.assertRaises(muse_msp.BoardError) as ctx:
            muse_msp.compile_board({"issues": "nope"}, NOW)
        self.assertEqual(ctx.exception.kind, "badBoard")
        with self.assertRaises(muse_msp.BoardError):
            muse_msp.load_board_snapshot("/nonexistent/board.json")


class CollisionTest(unittest.TestCase):
    def ranked(self, *entries):
        board = {"issues": [issue(entry[0], **entry[1]) for entry in entries]}
        return muse_msp.compile_board(board, NOW)["specs"]

    def test_same_branch_queues_behind_winner(self) -> None:
        specs = self.ranked(
            (1, {"labels": ["priority-high"], "scope": {"branch": "lane/x"}}),
            (2, {"labels": ["priority-low"], "scope": {"branch": "lane/x"}}),
        )
        specs = muse_msp.apply_collisions(specs, {})
        self.assertEqual(specs[0]["status"], "scheduled")
        self.assertEqual(specs[1]["status"], "queued")
        self.assertIn("scheduled issue #1", specs[1]["queuedBehind"])

    def test_file_and_checkout_overlap_queue(self) -> None:
        specs = self.ranked(
            (1, {"labels": ["priority-high"], "scope": {"files": ["pkg/a.py"]}}),
            (2, {"labels": ["priority-low"], "scope": {"files": ["pkg/a.py"]}}),
            (3, {"labels": ["priority-low"], "scope": {"files": ["other.py"]}}),
        )
        specs = muse_msp.apply_collisions(specs, {})
        by_number = {spec["issue"]: spec for spec in specs}
        self.assertEqual(by_number[1]["status"], "scheduled")
        self.assertEqual(by_number[2]["status"], "queued")
        self.assertIn("file overlap", by_number[2]["queuedBehind"])
        self.assertEqual(by_number[3]["status"], "scheduled")

    def test_nested_files_collide(self) -> None:
        specs = self.ranked(
            (1, {"labels": ["priority-high"], "scope": {"files": ["pkg"]}}),
            (2, {"labels": ["priority-low"], "scope": {"files": ["pkg/a.py"]}}),
        )
        specs = muse_msp.apply_collisions(specs, {})
        self.assertEqual(specs[1]["status"], "queued")

    def test_live_p4_lease_blocks_branch_and_checkout(self) -> None:
        specs = self.ranked(
            (1, {"scope": {"branch": "lane/x", "checkout": "/repo/x"}}),
        )
        live = {
            "lane/x": {"host": "peer", "lane": "other", "branch": "lane/x",
                       "live": True},
            "lane/y": {"host": "peer", "lane": "other", "branch": "lane/y",
                       "checkout": "/repo/x", "live": True},
        }
        specs = muse_msp.apply_collisions(specs, live)
        self.assertEqual(specs[0]["status"], "queued")
        self.assertIn("peer/other", specs[0]["queuedBehind"])

    def test_expired_lease_reassigns(self) -> None:
        specs = self.ranked((1, {"scope": {"branch": "lane/x"}}))
        live = {
            "lane/x": {"host": "peer", "lane": "old", "branch": "lane/x",
                       "live": False},
        }
        specs = muse_msp.apply_collisions(specs, live)
        self.assertEqual(specs[0]["status"], "scheduled")

    def test_decision_blocked_passes_through_collisions(self) -> None:
        specs = self.ranked(
            (1, {"labels": ["priority-high"], "scope": {"branch": "lane/x"}}),
            (2, {"labels": ["needs-auth"], "scope": {"branch": "lane/x"}}),
        )
        specs = muse_msp.apply_collisions(specs, {})
        by_number = {spec["issue"]: spec for spec in specs}
        self.assertEqual(by_number[1]["status"], "scheduled")
        self.assertEqual(by_number[2]["status"], "decision-blocked")
        self.assertNotIn("queuedBehind", by_number[2])


class DecisionBlockedTest(unittest.TestCase):
    def test_explicit_field_beats_labels(self) -> None:
        blocked = muse_msp.classify_decision(
            {"decisionBlocked": {"class": "spend", "reason": "gpu budget"},
             "labels": ["needs-auth"]}
        )
        self.assertEqual(blocked, {"class": "spend", "reason": "gpu budget"})

    def test_label_classes(self) -> None:
        self.assertEqual(
            muse_msp.classify_decision({"labels": ["needs-product-call"]})["class"],
            "product",
        )
        self.assertEqual(
            muse_msp.classify_decision({"labels": ["auth-boundary"]})["class"],
            "auth",
        )
        self.assertEqual(
            muse_msp.classify_decision({"labels": ["needs-spend"]})["class"],
            "spend",
        )
        self.assertIsNone(muse_msp.classify_decision({"labels": ["docs"]}))

    def test_spend_reuses_p2_decision_class(self) -> None:
        blocked = muse_msp.classify_decision({"labels": ["needs-spend"]})
        self.assertEqual(blocked["class"], muse_msp.SPEND_DECISION_CLASS)

    def test_body_markers_page_a_human(self) -> None:
        blocked = muse_msp.classify_decision(
            {"body": "Decision required: pick the API shape."}
        )
        self.assertEqual(blocked["class"], "product")

    def test_decision_blocked_never_schedules(self) -> None:
        board = {
            "issues": [
                issue(1, labels=["priority-critical", "needs-product-call"]),
            ]
        }
        plan = muse_msp.plan_board(board, live_claims={}, now=NOW)
        spec = plan["specs"][0]
        self.assertEqual(spec["status"], "decision-blocked")
        self.assertTrue(spec["pageHuman"])


class ExporterTest(unittest.TestCase):
    def fake_run(self, stdout_map, failures=()):
        def run(argv, **kwargs):
            key = " ".join(argv[1:3])

            class Proc:
                returncode = 0
                stderr = ""

                def __init__(self, stdout):
                    self.stdout = stdout

            if key in failures:
                proc = Proc("")
                proc.returncode = 1
                proc.stderr = "boom"
                return proc
            return Proc(json.dumps(stdout_map.get(key, [])))

        return run

    def test_export_builds_snapshot_read_only(self) -> None:
        seen = []

        def run(argv, **kwargs):
            seen.append(argv)

            class Proc:
                returncode = 0
                stderr = ""
                stdout = "[]"

            return Proc()

        snapshot = muse_msp.export_board_snapshot("O/R", run=run)
        self.assertEqual(snapshot["repo"], "O/R")
        self.assertEqual(snapshot["schemaVersion"], 1)
        self.assertTrue(all(argv[0] == "gh" for argv in seen))
        # Read-only verbs only: issue list + a GET.
        self.assertTrue(all("create" not in argv and "edit" not in argv for argv in seen))

    def test_export_normalizes_labels_and_milestones(self) -> None:
        stdout_map = {
            "issue list": [
                {"number": 1, "title": "T", "labels": [{"name": "priority-high"}],
                 "milestone": {"title": "Moonshot"}, "updatedAt": "t", "body": "b"},
            ],
            "api repos/O/R/milestones?state=open&per_page=100": [
                {"number": 1, "title": "Moonshot", "due_on": None}
            ],
        }
        snapshot = muse_msp.export_board_snapshot(
            "O/R", run=self.fake_run(stdout_map)
        )
        self.assertEqual(snapshot["issues"][0]["labels"], ["priority-high"])
        self.assertEqual(snapshot["issues"][0]["milestone"], "Moonshot")
        self.assertEqual(snapshot["milestones"][0]["title"], "Moonshot")

    def test_export_survives_milestone_endpoint_failure(self) -> None:
        stdout_map = {
            "issue list": [
                {"number": 1, "title": "T", "labels": [], "milestone": None,
                 "updatedAt": None, "body": None}
            ]
        }
        snapshot = muse_msp.export_board_snapshot(
            "O/R",
            run=self.fake_run(
                stdout_map,
                failures=("api repos/O/R/milestones?state=open&per_page=100",),
            ),
        )
        self.assertEqual(len(snapshot["issues"]), 1)
        self.assertEqual(snapshot["milestones"], [])

    def test_export_gh_failure_is_typed(self) -> None:
        with self.assertRaises(muse_msp.BoardError) as ctx:
            muse_msp.export_board_snapshot(
                "O/R", run=self.fake_run({}, failures=("issue list",))
            )
        self.assertEqual(ctx.exception.kind, "ghFailed")


class ReconcileTest(unittest.TestCase):
    def test_actions_cover_all_statuses(self) -> None:
        board = {
            "issues": [
                issue(1, labels=["priority-high"], scope={"branch": "lane/a"}),
                issue(2, labels=["priority-low"], scope={"branch": "lane/a"}),
                issue(3, labels=["needs-auth"], scope={"branch": "lane/c"}),
            ]
        }
        claims = {
            "lane/a": {"host": "h", "lane": "m8s-1", "branch": "lane/a",
                       "live": True},
        }
        plan = muse_msp.board_reconcile(board, claims, [], now=NOW)
        actions = {item["issue"]: item for item in plan["actions"]}
        self.assertEqual(actions[1]["action"], "in-sync")
        self.assertIn("h/m8s-1", actions[1]["reason"])
        self.assertEqual(actions[2]["action"], "queued")
        self.assertEqual(actions[3]["action"], "page-human")
        self.assertEqual(actions[3]["decisionClass"], "auth")

    def test_propose_carries_claim_instructions(self) -> None:
        board = {"issues": [issue(1, scope={"branch": "lane/a"})]}
        plan = muse_msp.board_reconcile(board, {}, [], now=NOW)
        action = plan["actions"][0]
        self.assertEqual(action["action"], "propose")
        self.assertEqual(action["branch"], "lane/a")
        self.assertIn("priority", action)

    def test_requeue_lists_expired_leases_on_desired_branches(self) -> None:
        board = {"issues": [issue(1, scope={"branch": "lane/a"})]}
        claims = {
            "lane/a": {"host": "h", "lane": "old", "branch": "lane/a",
                       "live": False},
            "lane/zzz": {"host": "h", "lane": "old", "branch": "lane/zzz",
                         "live": False},
        }
        plan = muse_msp.board_reconcile(board, claims, [], now=NOW)
        self.assertEqual(plan["requeue"], ["lane/a"])
        # Expired leases reassign: the desired lane still proposes.
        self.assertEqual(plan["actions"][0]["action"], "propose")

    def test_attention_surfaces_stuck_and_spend(self) -> None:
        board = {"issues": [issue(1)]}
        events = [
            {"kind": "lane.stuck", "sessionId": "s1", "reason": "idle"},
            {"kind": "budget.exceeded", "sessionId": "s2", "limit": "maxTokens",
             "decisionClass": "spend"},
            {"kind": "msp.event", "method": "session/tokenUsage"},
        ]
        plan = muse_msp.board_reconcile(board, {}, events, now=NOW)
        self.assertEqual(len(plan["attention"]), 2)
        kinds = {item["kind"] for item in plan["attention"]}
        self.assertEqual(kinds, {"lane.stuck", "budget.exceeded"})


class BoardCliTest(IsolatedFilesMixin, unittest.TestCase):
    def write_board(self, board) -> str:
        path = Path(self.tmp.name) / "board.json"
        path.write_text(json.dumps(board), encoding="utf-8")
        return str(path)

    def test_parser_accepts_board_actions(self) -> None:
        args = parse(["board", "plan", "--board", "b.json"])
        self.assertEqual((args.command, args.board_action), ("board", "plan"))
        args = parse(["board", "export", "--repo", "O/R", "--out", "b.json"])
        self.assertEqual(args.repo, "O/R")
        args = parse(["board", "reconcile", "--board", "b.json"])
        self.assertEqual(args.events_limit, 200)

    def test_plan_runs_daemonless(self) -> None:
        path = self.write_board({"issues": [issue(1, labels=["priority-high"])]})
        result = muse_msp.board_main(parse(["board", "plan", "--board", path]))
        self.assertEqual(result["plan"]["specs"][0]["status"], "scheduled")

    def test_reconcile_pages_humans_into_events(self) -> None:
        path = self.write_board({"issues": [issue(1, labels=["needs-spend"])]})
        result = muse_msp.board_main(
            parse(["board", "reconcile", "--board", path])
        )
        actions = result["reconcile"]["actions"]
        self.assertEqual(actions[0]["action"], "page-human")
        events = muse_msp.read_events(0, 200)
        paged = [event for event in events if event.get("kind") == "board.decisionBlocked"]
        self.assertEqual(len(paged), 1)
        self.assertEqual(paged[0]["decisionClass"], "spend")
        self.assertEqual(paged[0]["issue"], 1)


if __name__ == "__main__":
    unittest.main()
