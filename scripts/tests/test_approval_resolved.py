#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Approval outcome projection (issue #35).

Read-only fold of ``approval/resolved`` into lane state: the decide ack is
not the outcome, so the durable decision/policyResult/resolvedBy (+ session
amendment durability) is recorded on the session record and surfaced in
``pending`` output. Resolved blockers stop counting as pending; outcome
identity is retained first-write-wins.

Stdlib unittest only. Exercises the real ``MspHost._notification`` fold and
the daemon ``pending`` dispatch against a fake transport — never starts a
daemon or touches the network. Notification shapes mirror the installed
``muse schema`` bundle (``ApprovalResolvedParams`` / ``ApprovalUpdatedParams``).
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


def run(coro):
    return asyncio.run(coro)


class FakeHost(muse_msp.MspHost):
    """Fake transport under the real MspHost logic (no daemon, no wire)."""

    def __init__(self) -> None:
        # Deliberately no super().__init__: no daemon state, no file reads.
        self.calls: list[tuple[str, dict]] = []
        self.events: list[dict] = []
        self.aliases = {"lane": "session-1"}
        self.sessions: dict[str, dict] = {}
        self.budgets: dict[str, dict] = {}
        self.retired: dict[str, dict] = {}
        self.watchers = set()
        self.pending_reply: dict = {"approvals": [], "userInputs": []}

    def record(self, record: dict) -> None:
        self.events.append(record)

    async def call(self, method: str, params: dict | None = None):
        self.calls.append((method, dict(params or {})))
        if method == "approval/listPending":
            return self.pending_reply
        if method == "session/list":
            return {"sessions": [{"sessionId": sid} for sid in self.sessions]}
        return {"ok": True}

    async def notify(self, method, params):
        return await muse_msp.MspHost._notification(self, method, params)


def resolved_params(
    approval_id: str,
    session_id: str = "session-1",
    decision: str = "approved",
    policy_result: str = "allow",
    resolved_by: str = "user",
    durability: str | None = "session",
    turn_id: str = "turn-1",
) -> dict:
    """Real-shaped ``approval/resolved`` params per the MSP schema bundle."""
    params: dict = {
        "sessionId": session_id,
        "approvalId": approval_id,
        "itemId": f"item-{approval_id}",
        "turnId": turn_id,
        "viewCursor": f"cursor-{approval_id}",
        "decision": decision,
        "policyResult": policy_result,
        "resolvedBy": resolved_by,
        "decidedAt": "2026-10-07T16:00:00Z",
        "decidedByCommandId": "0193a111-1111-7111-8111-111111111111",
        "sourceRange": {
            "file": "/work/file.py",
            "start": {"line": 1, "character": 0},
            "end": {"line": 2, "character": 0},
        },
        "stageEvidence": [],
    }
    if durability is not None:
        params["amendment"] = {
            "durability": durability,
            "rulePreview": "allow test-tool for this session",
        }
    return params


def updated_params(approval_id: str, source_index: int) -> dict:
    """Real-shaped ``approval/updated`` params: a superseding requirement."""
    return {
        "sessionId": "session-1",
        "approvalId": approval_id,
        "viewCursor": "cursor-next",
        "change": "requirementAdvanced",
        "currentRequirementId": {"approvalId": approval_id, "sourceIndex": source_index},
        "availableChoices": [
            {
                "choiceId": "allow",
                "decision": "approved",
                "label": "Allow",
                "scope": "session",
            }
        ],
        "sourceRange": {
            "file": "/work/file.py",
            "start": {"line": 1, "character": 0},
            "end": {"line": 2, "character": 0},
        },
        "subject": {"kind": "runCommand", "command": "pytest"},
    }


class ApprovalResolvedProjectionTest(unittest.TestCase):
    def test_resolved_projects_outcome_into_lane_state(self) -> None:
        async def go():
            host = FakeHost()
            await host.notify("approval/resolved", resolved_params("a1"))
            return host

        host = run(go())
        state = host.sessions["session-1"]
        resolutions = state.get("approvalResolutions") or {}
        self.assertIn("a1", resolutions)
        outcome = resolutions["a1"]
        self.assertEqual(outcome["approvalId"], "a1")
        self.assertEqual(outcome["decision"], "approved")
        self.assertEqual(outcome["policyResult"], "allow")
        self.assertEqual(outcome["resolvedBy"], "user")
        self.assertEqual(outcome.get("amendmentDurability"), "session")
        # The event log is preserved alongside the projection.
        methods = [
            e.get("method") for e in host.events if e.get("kind") == "msp.event"
        ]
        self.assertIn("approval/resolved", methods)

    def test_multiple_approvals_each_retain_identity(self) -> None:
        async def go():
            host = FakeHost()
            await host.notify("approval/resolved", resolved_params("a1", decision="approved"))
            await host.notify(
                "approval/resolved",
                resolved_params("a2", decision="denied", policy_result="deny",
                                resolved_by="policy", durability="localPersistent"),
            )
            return host

        host = run(go())
        resolutions = host.sessions["session-1"].get("approvalResolutions") or {}
        self.assertEqual(set(resolutions), {"a1", "a2"})
        self.assertEqual(resolutions["a1"]["decision"], "approved")
        self.assertEqual(resolutions["a2"]["decision"], "denied")
        self.assertEqual(resolutions["a2"]["policyResult"], "deny")
        self.assertEqual(resolutions["a2"]["resolvedBy"], "policy")
        self.assertEqual(resolutions["a2"].get("amendmentDurability"), "localPersistent")

    def test_stale_resolution_without_prior_blocker_is_recorded(self) -> None:
        async def go():
            host = FakeHost()
            # No blocker, no pending listing, no prior lane state at all.
            await host.notify("approval/resolved", resolved_params("ghost-9"))
            return host

        host = run(go())
        resolutions = host.sessions["session-1"].get("approvalResolutions") or {}
        self.assertIn("ghost-9", resolutions)
        self.assertEqual(resolutions["ghost-9"]["decision"], "approved")

    def test_duplicate_resolution_keeps_first_outcome(self) -> None:
        async def go():
            host = FakeHost()
            await host.notify("approval/resolved", resolved_params("a1", decision="approved"))
            await host.notify("approval/resolved", resolved_params("a1", decision="denied"))
            return host

        host = run(go())
        resolutions = host.sessions["session-1"].get("approvalResolutions") or {}
        self.assertEqual(resolutions["a1"]["decision"], "approved")

    def test_superseding_updated_preserves_resolutions_and_pending_identity(self) -> None:
        async def go():
            host = FakeHost()
            await host.notify("approval/resolved", resolved_params("a1", decision="approved"))
            await host.notify("approval/updated", updated_params("a2", source_index=2))
            return host

        host = run(go())
        state = host.sessions["session-1"]
        resolutions = state.get("approvalResolutions") or {}
        # The superseding requirement creates no resolution of its own ...
        self.assertEqual(set(resolutions), {"a1"})
        # ... and the pending requirement identity is left untouched: this
        # fold owns approval/resolved state only and never auto-approves.
        self.assertNotIn("a2", resolutions)

    def test_outcome_retained_across_unrelated_notifications(self) -> None:
        async def go():
            host = FakeHost()
            await host.notify("approval/resolved", resolved_params("a1"))
            await host.notify(
                "session/statusChanged",
                {"sessionId": "session-1", "status": "idle", "attention": []},
            )
            await host.notify(
                "turn/completed",
                {"sessionId": "session-1", "turnId": "turn-1", "terminal": "completed"},
            )
            return host

        host = run(go())
        resolutions = host.sessions["session-1"].get("approvalResolutions") or {}
        self.assertIn("a1", resolutions)
        self.assertEqual(resolutions["a1"]["resolvedBy"], "user")

    def test_malformed_resolved_never_raises_and_never_projects(self) -> None:
        async def go():
            host = FakeHost()
            await host.notify("approval/resolved", {"sessionId": "session-1"})
            await host.notify("approval/resolved", {"approvalId": "no-session"})
            return host

        host = run(go())
        state = host.sessions.get("session-1") or {}
        self.assertNotIn("no-session", state.get("approvalResolutions") or {})

    def test_pending_surfaces_resolved_outcomes(self) -> None:
        async def go():
            host = FakeHost()
            host.pending_reply = {"approvals": [], "userInputs": []}
            await host.notify(
                "approval/resolved",
                resolved_params("a1", decision="denied", policy_result="deny"),
            )
            return await muse_msp.dispatch(host, {"command": "pending", "session": "lane"})

        result = run(go())
        self.assertEqual(result.get("approvals"), [])
        resolved = result.get("resolvedApprovals") or []
        by_id = {entry.get("approvalId"): entry for entry in resolved}
        self.assertIn("a1", by_id)
        self.assertEqual(by_id["a1"]["decision"], "denied")
        self.assertEqual(by_id["a1"]["policyResult"], "deny")

    def test_resolved_blockers_no_longer_count_as_pending(self) -> None:
        blockers = [
            {"kind": "blocker", "reason": "approval/request",
             "sessionId": "s", "requestId": "a1"},
            {"kind": "blocker", "reason": "approval/request",
             "sessionId": "s", "requestId": "a2"},
            {"kind": "blocker", "reason": "userInput/request",
             "sessionId": "s", "requestId": "u1"},
        ]
        live = muse_msp.unresolved_blockers(blockers, {"a1"})
        reasons = [(e.get("requestId"), e.get("reason")) for e in live]
        self.assertNotIn(("a1", "approval/request"), reasons)
        self.assertIn(("a2", "approval/request"), reasons)
        self.assertIn(("u1", "userInput/request"), reasons)
        counts = muse_msp.pending_counts(
            {"approvals": [], "userInputs": []}, [], live
        )
        self.assertEqual(counts, {"approvals": 1, "inputs": 1})


if __name__ == "__main__":
    unittest.main()
