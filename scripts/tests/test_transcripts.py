#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Replay SDK golden transcripts through the real MSP framing/handling.

Stdlib unittest only. Reads the vendored fixtures under
``scripts/tests/fixtures/`` (see its README for provenance) and feeds
the server-direction wire bytes through ``MspHost._read_stdout`` plus
``_notification``/``_server_request`` — no hand-mocked wire bytes.
Covers approval round-trips, cancel-mid-turn, cursor edge cases, and
framing tolerance. Never starts a daemon or touches the network.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

SCRIPT = Path(__file__).resolve().parent.parent / "muse-msp.py"
FIXTURES = Path(__file__).resolve().parent / "fixtures"

ACK_SERVER_REQUEST = b'{"jsonrpc":"2.0","id":1,"result":{}}\n'


def load_module():
    spec = importlib.util.spec_from_file_location("muse_msp", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


muse_msp = load_module()


def fixture_frames(scenario: str) -> list[tuple[str, bytes]]:
    """Parse a vendored transcript into (dir, raw-bytes) wire frames."""
    frames = []
    path = FIXTURES / scenario / "transcript.ndjson"
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        frames.append((entry["dir"], entry["raw"].encode("utf-8")))
    return frames


class StubStdin:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    def write(self, chunk: bytes) -> None:
        self.writes.append(chunk)

    async def drain(self) -> None:
        pass


class ReplayHost(muse_msp.MspHost):
    """Real framing/handling over canned server bytes (no daemon, no wire)."""

    def __init__(self, server_lines: list[bytes]) -> None:
        # Deliberately no super().__init__: no daemon state, no file reads.
        self.proc: Any = None  # type: ignore[assignment]
        self.pending: dict = {}
        self.next_id = 1
        self.sessions: dict = {}
        self.aliases: dict = {}
        self.watchers = set()
        self.budgets: dict = {}
        self.retired: dict = {}
        self.stopping = asyncio.Event()
        self.events: list[dict] = []
        self.stdin = StubStdin()
        reader = asyncio.StreamReader(limit=muse_msp.MSP_STREAM_LIMIT)
        for raw in server_lines:
            reader.feed_data(raw + b"\n")
        reader.feed_eof()
        self.proc = SimpleNamespace(stdin=self.stdin, stdout=reader, returncode=None)

    def record(self, record: dict) -> None:
        self.events.append(record)

    def kinds(self) -> list:
        return [e.get("kind") for e in self.events]


def replay(scenario: str, cr: bool = False):
    """Feed a fixture's server bytes through _read_stdout; return host+futures."""
    server = [
        raw + (b"\r" if cr else b"")
        for direction, raw in fixture_frames(scenario)
        if direction == "server"
    ]

    async def go():
        host = ReplayHost(server)
        loop = asyncio.get_running_loop()
        futures: dict = {}
        for _direction, raw in fixture_frames(scenario):
            if _direction != "server":
                continue
            frame = json.loads(raw.decode("utf-8"))
            if "id" in frame and "method" not in frame:
                futures.setdefault(frame["id"], loop.create_future())
                host.pending[frame["id"]] = futures[frame["id"]]
        await host._read_stdout()
        return host, futures

    return asyncio.run(go())


def server_params(scenario: str, method: str) -> list[dict]:
    out = []
    for direction, raw in fixture_frames(scenario):
        if direction != "server":
            continue
        frame = json.loads(raw.decode("utf-8"))
        if frame.get("method") == method:
            out.append(frame.get("params", {}))
    return out


def scenario_sid(scenario: str) -> str:
    """The session id a fixture's server frames address."""
    for direction, raw in fixture_frames(scenario):
        if direction != "server":
            continue
        params = json.loads(raw.decode("utf-8")).get("params", {})
        if isinstance(params, dict) and params.get("sessionId"):
            return params["sessionId"]
    raise AssertionError(f"no sessionId in {scenario}")


class ApprovalRoundTripTest(unittest.TestCase):
    def test_ack_blocker_and_resolve(self) -> None:
        host, futures = replay("approval-round-trip")
        # The server request is auto-acked with an empty presentation
        # receipt, exactly as the fixture's client line shows.
        self.assertIn(ACK_SERVER_REQUEST, host.stdin.writes)
        for chunk in host.stdin.writes:
            self.assertFalse(chunk.endswith(b"\r\n"), "client frames are LF-only")
        blockers = [e for e in host.events if e.get("kind") == "blocker"]
        self.assertEqual(len(blockers), 1)
        self.assertEqual(blockers[0]["reason"], "approval/request")
        self.assertEqual(blockers[0]["sessionId"], scenario_sid("approval-round-trip"))
        # Every server response resolves its future, including the
        # string-identifier approval/decide ack.
        for key in (1, 2, 3, 4, "b9"):
            self.assertIn(key, futures)
            self.assertTrue(futures[key].done())
            self.assertIsNone(futures[key].exception())
        methods = [
            e["method"] for e in host.events if e.get("kind") == "msp.event"
        ]
        self.assertIn("approval/requested", methods)
        self.assertIn("approval/resolved", methods)
        self.assertIn("turn/completed", methods)
        expected_terminal = server_params("approval-round-trip", "turn/completed")[0][
            "terminal"
        ]
        self.assertEqual(host.sessions[scenario_sid("approval-round-trip")]["lastTerminal"], expected_terminal)
        self.assertTrue(host.sessions[scenario_sid("approval-round-trip")]["viewCursor"].startswith("v:"))

    def test_deny_round_trip_records_resolve(self) -> None:
        host, futures = replay("approval-deny-round-trip")
        self.assertIn(ACK_SERVER_REQUEST, host.stdin.writes)
        blockers = [e for e in host.events if e.get("kind") == "blocker"]
        self.assertEqual(len(blockers), 1)
        resolved = [
            e
            for e in host.events
            if e.get("kind") == "msp.event" and e.get("method") == "approval/resolved"
        ]
        self.assertEqual(len(resolved), 1)
        self.assertTrue(all(f.exception() is None for f in futures.values()))


class CancelMidTurnTest(unittest.TestCase):
    def test_cancelled_turn_flags_owner_action(self) -> None:
        host, futures = replay("cancel-mid-turn")
        self.assertTrue(futures[4].done())
        self.assertIsNone(futures[4].exception())
        state = host.sessions[scenario_sid("cancel-mid-turn")]
        self.assertEqual(state["lastTerminal"], "cancelled")
        self.assertTrue(state["needsOwnerAction"])
        attention = [e for e in host.events if e.get("kind") == "lane.attention"]
        self.assertEqual(len(attention), 1)
        self.assertEqual(attention[0]["reason"], "turnCancelled")


class CursorEdgeTest(unittest.TestCase):
    def test_truncated_cursor_is_typed(self) -> None:
        _host, futures = replay("cursor-truncated")
        exc = futures[9].exception()
        self.assertIsInstance(exc, muse_msp.MspWireError)
        self.assertEqual(exc.kind, "viewTruncated")
        self.assertEqual(exc.code, -32040)

    def test_never_existed_cursor_is_typed(self) -> None:
        _host, futures = replay("cursor-never-existed")
        exc = futures[9].exception()
        self.assertIsInstance(exc, muse_msp.MspWireError)
        self.assertEqual(exc.kind, "notFound")
        self.assertEqual(exc.code, -32011)

    def test_retained_cursor_resumes(self) -> None:
        host, futures = replay("cursor-retained")
        self.assertIsNone(futures[9].exception())
        self.assertTrue(host.sessions[scenario_sid("cursor-retained")]["viewCursor"].startswith("v:"))

    def test_resume_after_cursor_replays_tail(self) -> None:
        host, futures = replay("resume-after-cursor")
        self.assertIsNone(futures[9].exception())
        expected_terminal = server_params("resume-after-cursor", "turn/completed")[0][
            "terminal"
        ]
        self.assertEqual(host.sessions[scenario_sid("resume-after-cursor")]["lastTerminal"], expected_terminal)


class FramingToleranceTest(unittest.TestCase):
    def test_cr_terminated_server_frames_parse(self) -> None:
        clean_host, _ = replay("approval-round-trip")
        cr_host, _ = replay("approval-round-trip", cr=True)
        self.assertEqual(cr_host.kinds(), clean_host.kinds())
        self.assertNotIn("controller.protocolError", cr_host.kinds())
        blockers = [e for e in cr_host.events if e.get("kind") == "blocker"]
        self.assertEqual(len(blockers), 1)

    def test_unknown_item_kind_tolerated(self) -> None:
        host, futures = replay("tolerance-unknown-item-kind")
        self.assertNotIn("controller.protocolError", host.kinds())
        self.assertTrue(all(f.exception() is None for f in futures.values()))
        expected_terminal = server_params(
            "tolerance-unknown-item-kind", "turn/completed"
        )[0]["terminal"]
        self.assertEqual(host.sessions[scenario_sid("tolerance-unknown-item-kind")]["lastTerminal"], expected_terminal)

    def test_unknown_stream_kind_tolerated(self) -> None:
        host, futures = replay("tolerance-unknown-stream-kind")
        self.assertNotIn("controller.protocolError", host.kinds())
        self.assertTrue(all(f.exception() is None for f in futures.values()))
        self.assertIn(scenario_sid("tolerance-unknown-stream-kind"), host.sessions)


if __name__ == "__main__":
    unittest.main()
