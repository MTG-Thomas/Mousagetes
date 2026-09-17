#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for Moonshot P2 hardening: budgets, stuck detection, call validation.

Stdlib unittest only. Exercises request building and daemon dispatch against
a fake host — never starts a daemon or touches the network.
"""

from __future__ import annotations

import asyncio
import importlib.util
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


class FakeHost(muse_msp.MspHost):
    """Fake transport under the real MspHost logic (no daemon, no wire)."""

    def __init__(self) -> None:
        # Deliberately no super().__init__: no daemon state, no budget file read.
        self.calls: list[tuple[str, dict]] = []
        self.events: list[dict] = []
        self.aliases = {"lane": "session-1"}
        self.sessions: dict[str, dict] = {}
        self.budgets: dict[str, dict] = {}
        self.watchers = set()

    def record(self, record: dict) -> None:
        self.events.append(record)

    async def call(self, method: str, params: dict | None = None):
        self.calls.append((method, dict(params or {})))
        if method == "session/list":
            return {"sessions": [{"sessionId": sid} for sid in self.sessions]}
        return {"ok": True}

    async def notify(self, method, params):
        return await muse_msp.MspHost._notification(self, method, params)

    def kinds(self):
        return [e.get("kind") for e in self.events]


def run(coro):
    return asyncio.run(coro)


class ValidateCallTest(unittest.TestCase):
    def test_method_snapshot_covers_all_51(self) -> None:
        self.assertEqual(len(muse_msp.MSP_METHODS), 51)
        self.assertTrue(set(muse_msp.COMMAND_METHODS) <= set(muse_msp.MSP_METHODS))

    def test_unknown_method_is_typed(self) -> None:
        with self.assertRaises(muse_msp.CallValidationError) as ctx:
            muse_msp.validate_call("session/typo", {})
        self.assertEqual(ctx.exception.kind, "unknownMethod")

    def test_missing_command_id_is_typed(self) -> None:
        with self.assertRaises(muse_msp.CallValidationError) as ctx:
            muse_msp.validate_call("goal/pause", {"sessionId": "s"}, "off")
        self.assertEqual(ctx.exception.kind, "missingCommandId")

    def test_explicit_command_id_with_off_passes(self) -> None:
        muse_msp.validate_call("goal/pause", {"commandId": "mine"}, "off")

    def test_off_is_fine_for_read_methods(self) -> None:
        muse_msp.validate_call("session/list", {}, "off")

    def test_cli_rejects_unknown_method_before_wire(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            muse_msp.build_request(parse(["call", "session/typo"]))
        self.assertIn("unknownMethod", str(ctx.exception.code))

    def test_cli_rejects_missing_command_id(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            muse_msp.build_request(parse(["call", "goal/pause", "--command-id", "off"]))
        self.assertIn("missingCommandId", str(ctx.exception.code))


class ValidatedDispatchTest(unittest.TestCase):
    def test_unknown_method_never_hits_wire(self) -> None:
        async def go():
            host = FakeHost()
            with self.assertRaises(muse_msp.CallValidationError) as ctx:
                await muse_msp.dispatch(host, {"command": "call", "method": "nope/method"})  # type: ignore[arg-type]
            return host, ctx.exception

        host, exc = run(go())
        self.assertEqual(exc.kind, "unknownMethod")
        self.assertEqual(host.calls, [])

    def test_missing_command_id_never_hits_wire(self) -> None:
        async def go():
            host = FakeHost()
            with self.assertRaises(muse_msp.CallValidationError) as ctx:
                await muse_msp.dispatch(  # type: ignore[arg-type]
                    host,
                    {"command": "call", "method": "goal/pause", "commandId": "off",
                     "params": {"sessionId": "session-1"}},
                )
            return host, ctx.exception

        host, exc = run(go())
        self.assertEqual(exc.kind, "missingCommandId")
        self.assertEqual(host.calls, [])

    def test_control_envelope_carries_error_kind(self) -> None:
        class FakeWriter:
            def __init__(self) -> None:
                self.data = bytearray()

            def write(self, chunk: bytes) -> None:
                self.data.extend(chunk)

            async def drain(self) -> None:
                pass

            def close(self) -> None:
                pass

            async def wait_closed(self) -> None:
                pass

        async def go():
            host = FakeHost()
            reader = asyncio.StreamReader()
            reader.feed_data((json.dumps({"command": "call", "method": "bogus/method"}) + "\n").encode())
            reader.feed_eof()
            writer = FakeWriter()
            await muse_msp.handle_client(host, reader, writer)  # type: ignore[arg-type]
            return host, json.loads(bytes(writer.data).decode())

        host, response = run(go())
        self.assertFalse(response["ok"])
        self.assertEqual(response["errorKind"], "unknownMethod")
        self.assertEqual(host.calls, [])


class BudgetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.old = muse_msp.BUDGETS_FILE
        muse_msp.BUDGETS_FILE = Path(self.tmp.name) / "budgets.json"

    def tearDown(self) -> None:
        muse_msp.BUDGETS_FILE = self.old
        self.tmp.cleanup()

    def test_normalize_accepts_full_spec(self) -> None:
        budget = muse_msp.normalize_budget(
            {"maxTokens": 1000, "maxContextTokens": 500, "models": ["m1", "m2"]}
        )
        self.assertEqual(budget, {"maxTokens": 1000, "maxContextTokens": 500, "models": ["m1", "m2"]})

    def test_normalize_splits_model_string(self) -> None:
        self.assertEqual(
            muse_msp.normalize_budget({"models": "m1, m2"}), {"models": ["m1", "m2"]}
        )

    def test_normalize_rejects_bad_specs(self) -> None:
        for spec in (
            {},
            {"maxTokens": 0},
            {"maxTokens": -5},
            {"maxTokens": "lots"},
            {"bogus": 1},
            {"models": []},
            {"models": [""]},
        ):
            with self.subTest(spec=spec):
                with self.assertRaises(ValueError):
                    muse_msp.normalize_budget(spec)

    def test_set_get_clear_round_trip(self) -> None:
        async def go():
            host = FakeHost()
            host.sessions["session-1"] = {"sessionId": "session-1"}
            set_result = await host.set_budget("lane", {"maxTokens": 100})
            get_result = await host.get_budget("lane")
            persisted = json.loads(muse_msp.BUDGETS_FILE.read_text())
            clear_result = await host.clear_budget("lane")
            return host, set_result, get_result, persisted, clear_result

        host, set_result, get_result, persisted, clear_result = run(go())
        self.assertEqual(set_result["budget"], {"maxTokens": 100})
        self.assertEqual(get_result["budget"], {"maxTokens": 100})
        self.assertEqual(persisted, {"session-1": {"maxTokens": 100}})
        self.assertEqual(clear_result["budget"], None)
        self.assertIn("budget.updated", host.kinds())
        self.assertIn("budget.removed", host.kinds())

    def test_token_breach_flags_and_blocks_send(self) -> None:
        async def go():
            host = FakeHost()
            host.sessions["session-1"] = {
                "sessionId": "session-1",
                "budget": {"maxTokens": 100},
                "tokenUsage": {"totalTokens": 50},
            }
            await host.notify("session/tokenUsage", {
                "sessionId": "session-1",
                "cumulative": {"promptTokens": 90, "outputTokens": 60, "totalTokens": 150},
            })
            with self.assertRaises(muse_msp.BudgetExceededError) as ctx:
                await host.submit("lane", "more work")
            return host, ctx.exception

        host, exc = run(go())
        self.assertEqual(exc.kind, "overBudget")
        self.assertTrue(host.sessions["session-1"]["overBudget"])
        breach = [e for e in host.events if e.get("kind") == "budget.exceeded"]
        self.assertEqual(len(breach), 1)
        self.assertEqual(breach[0]["decisionClass"], "spend")
        self.assertEqual(breach[0]["limit"], "maxTokens")
        # No turn/start reached the wire.
        self.assertEqual(host.calls, [])

    def test_raising_budget_unpauses_lane(self) -> None:
        async def go():
            host = FakeHost()
            host.sessions["session-1"] = {
                "sessionId": "session-1",
                "budget": {"maxTokens": 100},
                "tokenUsage": {"totalTokens": 150},
                "overBudget": True,
            }
            await host.set_budget("lane", {"maxTokens": 1000})
            result = await host.submit("lane", "resume")
            return host, result

        host, result = run(go())
        self.assertEqual(result, {"ok": True})
        self.assertFalse(host.sessions["session-1"]["overBudget"])
        self.assertIn("budget.cleared", host.kinds())
        self.assertEqual(host.calls[0][0], "turn/start")

    def test_context_breach_blocks_generic_turn_start(self) -> None:
        async def go():
            host = FakeHost()
            host.sessions["session-1"] = {
                "sessionId": "session-1",
                "budget": {"maxContextTokens": 100},
            }
            await host.notify("session/contextUsage", {
                "sessionId": "session-1",
                "usedTokens": 120,
                "windowTokens": 200,
                "pressure": "warning",
            })
            with self.assertRaises(muse_msp.BudgetExceededError):
                await host.supervised_call(
                    "turn/start", {"sessionId": "session-1", "input": []}
                )
            return host

        host = run(go())
        self.assertEqual(host.sessions["session-1"]["overBudgetLimit"], "maxContextTokens")
        self.assertEqual(host.calls, [])

    def test_model_allowlist_enforced_on_change_and_set(self) -> None:
        async def go():
            host = FakeHost()
            host.sessions["session-1"] = {
                "sessionId": "session-1",
                "budget": {"models": ["cheap"]},
            }
            await host.notify("session/modelChanged", {
                "sessionId": "session-1", "modelId": "pricey",
            })
            with self.assertRaises(muse_msp.BudgetExceededError) as ctx:
                await host.supervised_call(
                    "session/setModel", {"sessionId": "session-1", "model": "pricey"}
                )
            allowed = await host.supervised_call(
                "session/setModel", {"sessionId": "session-1", "model": "cheap"}
            )
            return host, ctx.exception, allowed

        host, exc, allowed = run(go())
        self.assertEqual(exc.kind, "modelNotAllowed")
        self.assertEqual(host.sessions["session-1"]["overBudgetLimit"], "models")
        self.assertEqual(allowed, {"ok": True})
        set_calls = [c for c in host.calls if c[0] == "session/setModel"]
        self.assertEqual(len(set_calls), 1)
        self.assertEqual(set_calls[0][1]["model"], "cheap")

    def test_list_shows_budget_usage_and_flags(self) -> None:
        async def go():
            host = FakeHost()
            host.sessions["session-1"] = {
                "sessionId": "session-1",
                "alias": "lane",
                "budget": {"maxTokens": 100},
                "tokenUsage": {"totalTokens": 10},
                "lastActivity": time.time(),
            }
            return await host.list_sessions()

        result = run(go())
        item = result["sessions"][0]
        self.assertEqual(item["budget"], {"maxTokens": 100})
        self.assertEqual(item["tokenUsage"], {"totalTokens": 10})
        self.assertFalse(item["overBudget"])
        self.assertFalse(item["needsOwnerAction"])
        self.assertIsNone(item["stuck"])

    def test_budget_cli_builds_requests(self) -> None:
        req = muse_msp.build_request(
            parse(["budget", "lane", "--max-tokens", "100", "--models", "m1,m2"])
        )
        self.assertEqual(req["command"], "budget")
        self.assertEqual(req["maxTokens"], 100)
        self.assertEqual(req["models"], "m1,m2")
        req = muse_msp.build_request(parse(["budget", "lane", "--clear"]))
        self.assertTrue(req["clear"])
        req = muse_msp.build_request(
            parse(["launch", "--name", "n", "--workspace", "/tmp", "--prompt", "p",
                   "--max-tokens", "50"])
        )
        self.assertEqual(req["budget"], {"maxTokens": 50})


class StuckDetectionTest(unittest.TestCase):
    def test_failed_turn_needs_owner_action(self) -> None:
        async def go():
            host = FakeHost()
            await host.notify("turn/completed", {
                "sessionId": "session-1", "turnId": "t1", "terminal": "failed",
            })
            stuck = muse_msp.lane_stuck(host.sessions["session-1"], NOW)
            return host, stuck

        host, stuck = run(go())
        self.assertTrue(host.sessions["session-1"]["needsOwnerAction"])
        self.assertEqual(stuck["reason"], "turnFailed")
        attention = [e for e in host.events if e.get("kind") == "lane.attention"]
        self.assertEqual(len(attention), 1)
        self.assertEqual(attention[0]["reason"], "turnFailed")

    def test_cancelled_turn_flagged_completed_not(self) -> None:
        async def go():
            host = FakeHost()
            await host.notify("turn/completed", {
                "sessionId": "session-1", "turnId": "t1", "terminal": "cancelled",
            })
            cancelled = muse_msp.lane_stuck(host.sessions["session-1"], NOW)
            host2 = FakeHost()
            await host2.notify("turn/completed", {
                "sessionId": "s2", "turnId": "t2", "terminal": "completed",
            })
            cleared = muse_msp.lane_stuck(host2.sessions["s2"], NOW)
            return cancelled, cleared, host2

        cancelled, cleared, host2 = run(go())
        self.assertEqual(cancelled["reason"], "turnCancelled")
        self.assertIsNone(cleared)
        self.assertFalse(host2.sessions["s2"].get("needsOwnerAction"))

    def test_idle_threshold(self) -> None:
        old = {"sessionId": "s", "lastActivity": NOW - 3600}
        fresh = {"sessionId": "s", "lastActivity": NOW - 60}
        never = {"sessionId": "s"}
        self.assertEqual(muse_msp.lane_stuck(old, NOW)["reason"], "idle")
        self.assertIsNone(muse_msp.lane_stuck(fresh, NOW))
        self.assertIsNone(muse_msp.lane_stuck(never, NOW))

    def test_repo_activity_forgives_transcript_idle(self) -> None:
        state = {"sessionId": "s", "lastActivity": NOW - 3600, "lastRepoActivity": NOW - 60}
        self.assertIsNone(muse_msp.lane_stuck(state, NOW))

    def test_owner_send_clears_attention(self) -> None:
        async def go():
            host = FakeHost()
            host.sessions["session-1"] = {
                "sessionId": "session-1",
                "needsOwnerAction": True,
                "lastTerminal": "failed",
            }
            await host.submit("lane", "try again")
            return host

        host = run(go())
        self.assertFalse(host.sessions["session-1"]["needsOwnerAction"])
        self.assertIsNone(muse_msp.lane_stuck(host.sessions["session-1"], NOW))

    def test_list_flags_stuck_once_then_recovered(self) -> None:
        async def go():
            host = FakeHost()
            host.sessions["session-1"] = {
                "sessionId": "session-1", "alias": "lane",
                "lastActivity": time.time() - 7200,
            }
            first = await host.list_sessions()
            second = await host.list_sessions()
            # Transcript activity resumes; next list reports recovery.
            await host.notify("turn/started", {"sessionId": "session-1"})
            third = await host.list_sessions()
            return host, first, second, third

        host, first, second, third = run(go())
        self.assertEqual(first["sessions"][0]["stuck"]["reason"], "idle")
        self.assertEqual(second["sessions"][0]["stuck"]["reason"], "idle")
        stuck_events = [e for e in host.events if e.get("kind") == "lane.stuck"]
        self.assertEqual(len(stuck_events), 1)
        self.assertIsNone(third["sessions"][0]["stuck"])
        self.assertIn("lane.recovered", host.kinds())

    def test_repo_probe_never_raises(self) -> None:
        self.assertIsNone(muse_msp.repo_activity_ts(None))
        self.assertIsNone(muse_msp.repo_activity_ts("/nonexistent-worktree-xyz"))
        with tempfile.TemporaryDirectory() as plain:
            self.assertIsNone(muse_msp.repo_activity_ts(plain))

    def test_stuck_threshold_env_override(self) -> None:
        old = os.environ.get("M8S_STUCK_AFTER_SECONDS")
        try:
            os.environ["M8S_STUCK_AFTER_SECONDS"] = "60"
            self.assertEqual(muse_msp.stuck_after_seconds(), 60)
            os.environ["M8S_STUCK_AFTER_SECONDS"] = "bogus"
            self.assertEqual(muse_msp.stuck_after_seconds(), muse_msp.STUCK_IDLE_SECONDS)
        finally:
            if old is None:
                os.environ.pop("M8S_STUCK_AFTER_SECONDS", None)
            else:
                os.environ["M8S_STUCK_AFTER_SECONDS"] = old


if __name__ == "__main__":
    unittest.main()
