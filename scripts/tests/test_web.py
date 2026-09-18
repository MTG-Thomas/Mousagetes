#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for the m8s v1 web client bridge (stdlib localhost + SSE + page).

Stdlib unittest only. Never starts a daemon, never binds a port, never
touches the network: covers parser defaults, loopback guard, token auth,
send-payload validation, SSE framing, and static-page markers.
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "muse-msp.py"


def load_module():
    spec = importlib.util.spec_from_file_location("muse_msp_web", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


muse_msp = load_module()


def parse(argv: list[str]):
    return muse_msp.parser().parse_args(argv)


class WebParserTest(unittest.TestCase):
    def test_web_defaults(self) -> None:
        args = parse(["web"])
        self.assertEqual(args.bind, "127.0.0.1")
        self.assertEqual(args.port, muse_msp.WEB_DEFAULT_PORT)

    def test_web_custom_port(self) -> None:
        args = parse(["web", "--port", "9999"])
        self.assertEqual(args.port, 9999)


class WebGuardTest(unittest.TestCase):
    def test_loopback_accepted(self) -> None:
        for host in ("127.0.0.1", "::1", "localhost"):
            self.assertTrue(muse_msp.web_is_loopback(host))

    def test_non_loopback_rejected(self) -> None:
        for host in ("0.0.0.0", "::", "example.com", ""):
            self.assertFalse(muse_msp.web_is_loopback(host))

    def test_run_web_refuses_remote_bind(self) -> None:
        with self.assertRaises(SystemExit):
            muse_msp.run_web("0.0.0.0", 1)

    def test_remote_bind_needs_explicit_flag(self) -> None:
        with self.assertRaises(SystemExit):
            muse_msp.web_check_bind("100.103.119.28")
        muse_msp.web_check_bind("100.103.119.28", allow_remote=True)
        muse_msp.web_check_bind("127.0.0.1")

    def test_allow_remote_defaults_off(self) -> None:
        self.assertFalse(parse(["web"]).allow_remote)
        self.assertTrue(parse(["web", "--allow-remote"]).allow_remote)


class WebAuthTest(unittest.TestCase):
    def test_matching_token(self) -> None:
        self.assertTrue(muse_msp.web_check_token("abc", "abc"))

    def test_wrong_token_rejected(self) -> None:
        self.assertFalse(muse_msp.web_check_token("abc", "def"))

    def test_empty_tokens_rejected(self) -> None:
        self.assertFalse(muse_msp.web_check_token("", "abc"))
        self.assertFalse(muse_msp.web_check_token(None, "abc"))
        self.assertFalse(muse_msp.web_check_token("abc", ""))


class WebSendTest(unittest.TestCase):
    def test_valid_payload(self) -> None:
        session, prompt = muse_msp.web_validate_send_payload(
            {"session": "coord", "prompt": "hold the merge gate"}
        )
        self.assertEqual((session, prompt), ("coord", "hold the merge gate"))

    def test_missing_session(self) -> None:
        with self.assertRaises(ValueError):
            muse_msp.web_validate_send_payload({"prompt": "x"})

    def test_missing_prompt(self) -> None:
        with self.assertRaises(ValueError):
            muse_msp.web_validate_send_payload({"session": "coord"})

    def test_non_object_rejected(self) -> None:
        for bad in (None, "x", ["x"], 42):
            with self.assertRaises(ValueError):
                muse_msp.web_validate_send_payload(bad)


class WebSseTest(unittest.TestCase):
    def test_framing(self) -> None:
        raw = muse_msp.web_sse_format({"at": 1.0, "kind": "x"})
        self.assertTrue(raw.startswith(b"data: "))
        self.assertTrue(raw.endswith(b"\n\n"))

    def test_record_at_numeric(self) -> None:
        self.assertEqual(muse_msp.web_record_at({"at": 3.5}), 3.5)
        self.assertEqual(muse_msp.web_record_at({}), 0.0)

    def test_record_at_malformed(self) -> None:
        self.assertEqual(muse_msp.web_record_at({"at": "nope"}), 0.0)
        self.assertEqual(muse_msp.web_record_at({"at": None}), 0.0)
        self.assertEqual(muse_msp.web_record_at(None), 0.0)

    def test_framing_carries_resume_id(self) -> None:
        raw = muse_msp.web_sse_format({"at": 12.5, "kind": "x"})
        self.assertIn(b"\nid: 12.5\n\n", raw)

    def test_stream_after_prefers_last_event_id(self) -> None:
        query = {"after": ["3.0"]}
        self.assertEqual(
            muse_msp.web_stream_after({"Last-Event-ID": "9.25"}, query), 9.25
        )
        self.assertEqual(muse_msp.web_stream_after({}, query), 3.0)

    def test_stream_after_invalid_falls_back(self) -> None:
        self.assertEqual(
            muse_msp.web_stream_after({"Last-Event-ID": "nope"}, {"after": ["bad"]}),
            0.0,
        )
        self.assertEqual(muse_msp.web_stream_after(None, {}), 0.0)


class WebEventBusTest(unittest.TestCase):
    def test_tail_returns_newer_records(self) -> None:
        bus = muse_msp.WebEventBus()
        bus._publish({"at": 1.0, "kind": "a"})
        bus._publish({"at": 2.0, "kind": "b"})
        fresh, caught_up = bus.tail(1.5, 0.1)
        self.assertTrue(caught_up)
        self.assertEqual([r["kind"] for r in fresh], ["b"])

    def test_tail_timeout_when_caught_up(self) -> None:
        bus = muse_msp.WebEventBus()
        bus._publish({"at": 1.0, "kind": "a"})
        fresh, caught_up = bus.tail(1.0, 0.1)
        self.assertTrue(caught_up)
        self.assertEqual(fresh, [])

    def test_tail_wakes_on_publish(self) -> None:
        import threading

        bus = muse_msp.WebEventBus()
        seen: list = []
        worker = threading.Thread(
            target=lambda: seen.append(bus.tail(0.0, 5.0)), daemon=True
        )
        worker.start()
        bus._publish({"at": 7.0, "kind": "live"})
        worker.join(5.0)
        self.assertFalse(worker.is_alive())
        fresh, caught_up = seen[0]
        self.assertTrue(caught_up)
        self.assertEqual([r["kind"] for r in fresh], ["live"])

    def test_tail_flags_evicted_history(self) -> None:
        bus = muse_msp.WebEventBus(maxlen=2)
        bus._publish({"at": 1.0, "kind": "a"})
        bus._publish({"at": 2.0, "kind": "b"})
        bus._publish({"at": 3.0, "kind": "c"})
        fresh, caught_up = bus.tail(0.5, 0.1)
        self.assertFalse(caught_up)

    def test_start_is_idempotent(self) -> None:
        import threading

        bus = muse_msp.WebEventBus()
        calls: list = []
        bus._pump = lambda: calls.append(1)  # type: ignore[method-assign]
        before = threading.active_count()
        bus.start()
        bus.start()
        for _ in range(100):
            if len(calls) >= 1:
                break
            import time

            time.sleep(0.01)
        self.assertTrue(bus._started)
        self.assertEqual(len(calls), 1)
        self.assertLessEqual(threading.active_count(), before + 1)


class WebPublicPathsTest(unittest.TestCase):
    def test_index_is_public(self) -> None:
        self.assertIn("/", muse_msp.WEB_PUBLIC_PATHS)

    def test_api_paths_stay_gated(self) -> None:
        for path in ("/api/list", "/api/health", "/api/events", "/api/send", "/api/read"):
            self.assertNotIn(path, muse_msp.WEB_PUBLIC_PATHS)


class WebFilterTest(unittest.TestCase):
    def test_empty_kinds_allows_all(self) -> None:
        self.assertTrue(muse_msp.web_event_allowed({"kind": "x"}, frozenset()))
        self.assertTrue(muse_msp.web_event_allowed({}, frozenset()))

    def test_matches_kind_or_method(self) -> None:
        kinds = frozenset({"turn.submitted", "session/statusChanged"})
        self.assertTrue(muse_msp.web_event_allowed({"kind": "turn.submitted"}, kinds))
        self.assertTrue(
            muse_msp.web_event_allowed(
                {"kind": "msp.event", "method": "session/statusChanged"}, kinds
            )
        )

    def test_rejects_unlisted(self) -> None:
        kinds = frozenset({"lane.stuck"})
        self.assertFalse(muse_msp.web_event_allowed({"kind": "turn.submitted"}, kinds))
        self.assertFalse(muse_msp.web_event_allowed({}, kinds))

    def test_parse_kinds(self) -> None:
        self.assertEqual(
            muse_msp.web_parse_kinds({"kinds": ["turn.submitted, session/statusChanged "]}),
            frozenset({"turn.submitted", "session/statusChanged"}),
        )
        self.assertEqual(muse_msp.web_parse_kinds({}), frozenset())


class WebRosterTest(unittest.TestCase):
    def test_collects_hosts_and_bus_lanes(self) -> None:
        roster = muse_msp.web_collect_roster(
            ["b", "a"],
            [{"lane": "lane-x"}, {"lane": ""}, {}],
            [
                {"message": {"lane": "lane-y"}},
                {"message": {}},
                {},
            ],
        )
        self.assertEqual(
            roster, {"hosts": ["a", "b"], "lanes": ["lane-x", "lane-y"], "tui": []}
        )

    def test_empty_roster(self) -> None:
        self.assertEqual(
            muse_msp.web_collect_roster([], [], []),
            {"hosts": [], "lanes": [], "tui": []},
        )

    def test_collects_tui_names_sorted(self) -> None:
        roster = muse_msp.web_collect_roster([], [], [], ["b-tui", "a-tui", "a-tui"])
        self.assertEqual(roster["tui"], ["a-tui", "b-tui"])

    def test_expired_intents_excluded(self) -> None:
        import time

        now = time.time()
        roster = muse_msp.web_collect_roster(
            [],
            [],
            [
                {"message": {"lane": "old", "expiresAt": now - 10}},
                {"message": {"lane": "live", "expiresAt": now + 300}},
                {"message": {"lane": "no-ttl"}},
            ],
        )
        self.assertEqual(
            roster, {"hosts": [], "lanes": ["live", "no-ttl"], "tui": []}
        )


class WebLocalSessionsTest(unittest.TestCase):
    def test_scan_never_raises_and_returns_list(self) -> None:
        found = muse_msp.web_local_tui_sessions()
        self.assertIsInstance(found, list)
        for item in found:
            for key in ("name", "sessionId", "workspace", "pid"):
                self.assertIn(key, item)
                self.assertTrue(item[key])

    def test_unknown_session_has_no_name(self) -> None:
        self.assertIsNone(muse_msp.web_session_name("00000000-0000-0000-0000-000000000000"))


class WebAdviseTest(unittest.TestCase):
    def test_valid_payload(self) -> None:
        target, advice = muse_msp.web_validate_advise_payload(
            {"coordinator": "host-b", "advice": "hold the merge gate"}
        )
        self.assertEqual((target, advice), ("host-b", "hold the merge gate"))

    def test_rejects_bad_payloads(self) -> None:
        for bad in (
            None,
            "x",
            {},
            {"coordinator": "h"},
            {"advice": "a"},
            {"coordinator": "", "advice": "a"},
            {"coordinator": 42, "advice": "a"},
        ):
            with self.assertRaises(ValueError):
                muse_msp.web_validate_advise_payload(bad)


class WebApproveTest(unittest.TestCase):
    def test_valid_payload(self) -> None:
        params = muse_msp.web_validate_approve_payload(
            {
                "session": "s1",
                "approvalId": "a1",
                "choiceId": "allow",
                "requirementId": "r1",
            }
        )
        self.assertEqual(
            params,
            {
                "session": "s1",
                "approvalId": "a1",
                "choiceId": "allow",
                "requirementId": "r1",
            },
        )

    def test_feedback_passthrough(self) -> None:
        params = muse_msp.web_validate_approve_payload(
            {
                "session": "s1",
                "approvalId": "a1",
                "choiceId": "deny",
                "requirementId": "r1",
                "feedback": "use read-only instead",
            }
        )
        self.assertEqual(params["feedback"], "use read-only instead")

    def test_rejects_bad_payloads(self) -> None:
        for bad in (
            None,
            {},
            {"session": "s1"},
            {"session": "s1", "approvalId": "a", "choiceId": "c"},
            {
                "session": "s1",
                "approvalId": "a",
                "choiceId": "c",
                "requirementId": "r",
                "feedback": 42,
            },
        ):
            with self.assertRaises(ValueError):
                muse_msp.web_validate_approve_payload(bad)


class WebClarifyTest(unittest.TestCase):
    def test_valid_payload(self) -> None:
        params = muse_msp.web_validate_clarify_payload(
            {"session": "s1", "userInputId": "u1", "text": "go with option A"}
        )
        self.assertEqual(params["text"], "go with option A")

    def test_rejects_bad_payloads(self) -> None:
        for bad in (
            None,
            {},
            {"session": "s1", "userInputId": "u"},
            {"session": "s1", "userInputId": "u", "text": ""},
            {"session": "s1", "userInputId": "u", "text": "x" * 501},
        ):
            with self.assertRaises(ValueError):
                muse_msp.web_validate_clarify_payload(bad)


class InboxHost(muse_msp.MspHost):
    """Stub for the inbox aggregate: canned roster, one failing lane."""

    def __init__(self) -> None:
        self.sessions = [
            {"sessionId": "s1", "alias": "lane-1", "status": "idle"},
            {"sessionId": "s2", "alias": "lane-2", "status": "running"},
            {"name": "ghost"},
        ]

    def record(self, record: dict) -> None:
        pass

    async def list_sessions(self):
        return {"sessions": self.sessions}

    async def call(self, method: str, params: dict | None = None):
        assert method == "approval/listPending"
        if (params or {}).get("sessionId") == "s2":
            raise RuntimeError("boom")
        return {"approvals": [{"approvalId": "a1"}], "userInputs": []}


class InboxDispatchTest(unittest.TestCase):
    def test_aggregate_per_lane_with_inline_errors(self) -> None:
        import asyncio

        result = asyncio.run(muse_msp.dispatch(InboxHost(), {"command": "inbox"}))
        items = result["inbox"]
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["approvals"], [{"approvalId": "a1"}])
        self.assertEqual(items[0]["alias"], "lane-1")
        self.assertIn("boom", items[1]["error"])

    def test_inbox_cli_request(self) -> None:
        req = muse_msp.build_request(muse_msp.parser().parse_args(["inbox"]))
        self.assertEqual(req, {"command": "inbox"})


class WebTurnTest(unittest.TestCase):
    def test_cancel_needs_only_session(self) -> None:
        self.assertEqual(
            muse_msp.web_validate_turn_payload({"session": "s1", "action": "cancel"}),
            ("s1", "cancel", None),
        )

    def test_steer_mapping(self) -> None:
        session, action, params = muse_msp.web_validate_turn_payload(
            {"session": "s1", "action": "steer", "turnId": "t1", "input": "hold"}
        )
        self.assertEqual(
            (session, action, params),
            ("s1", "steer", {"expectedTurnId": "t1", "input": "hold"}),
        )

    def test_unqueue_mapping(self) -> None:
        self.assertEqual(
            muse_msp.web_validate_turn_payload(
                {"session": "s1", "action": "unqueue", "turnId": "t1"}
            ),
            ("s1", "unqueue", {"turnId": "t1"}),
        )

    def test_rejects_bad_payloads(self) -> None:
        for bad in (
            None,
            {},
            {"session": "s1"},
            {"session": "s1", "action": "nuke"},
            {"session": "s1", "action": "steer", "turnId": "t"},
            {"session": "s1", "action": "steer", "turnId": "t", "input": ""},
            {"session": "s1", "action": "unqueue"},
        ):
            with self.assertRaises(ValueError):
                muse_msp.web_validate_turn_payload(bad)


class WebCoordinatorTest(unittest.TestCase):
    def test_roundtrip(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "web.coordinator"
            self.assertIsNone(muse_msp.web_read_coordinator(path))
            self.assertEqual(muse_msp.web_write_coordinator("lane-1", path), "lane-1")
            self.assertEqual(muse_msp.web_read_coordinator(path), "lane-1")

    def test_clear(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "web.coordinator"
            muse_msp.web_write_coordinator("lane-1", path)
            self.assertIsNone(muse_msp.web_write_coordinator(None, path))
            self.assertIsNone(muse_msp.web_read_coordinator(path))
            self.assertIsNone(muse_msp.web_write_coordinator("  ", path))

    def test_rejects_non_string(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                muse_msp.web_write_coordinator(42, Path(tmp) / "web.coordinator")


class WebPageTest(unittest.TestCase):
    def test_panels_present(self) -> None:
        for marker in (
            'id="lanes-panel"',
            'id="health-panel"',
            'id="events-panel"',
            'id="transcript-panel"',
            'id="send-panel"',
            'id="pending"',
            'id="lanes-age"',
            'id="health-age"',
            'id="events-age"',
            'id="coord-select"',
            "routeEvent",
            "/api/coordinator",
            "/api/coordinators",
            "/api/advise",
            "message coordinator",
            "sendMessage",
            "local sessions",
            "renderFleet",
            "budgetMeter",
            "laneGroup",
            "patchLanes",
            "patchKeyed",
            "laneRowHtml",
            "data-lane",
            "data-k",
            "tailTranscript",
            "appendTxEvents",
            "txKey",
            "data-tx",
            'class="meter"',
            "txRow",
            "toggleDiff",
            "turnOp",
            'id="tx-more"',
            'id="turn-steer"',
            "/api/patch",
            "/api/turn",
            "refreshBoard",
            "boardRow",
            "tagAttention",
            'id="board-panel"',
            "/api/board",
            "/api/events/stream",
        ):
            self.assertIn(marker, muse_msp.WEB_INDEX)


if __name__ == "__main__":
    unittest.main()
