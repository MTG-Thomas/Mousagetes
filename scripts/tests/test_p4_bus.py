#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for Moonshot P4: claim / heartbeat / intent bus with branch leases.

Stdlib unittest only. Exercises versioned message shapes, the subject log,
branch/checkout contention, heartbeat re-gossip, expiry into reassignment,
and the list/events wiring — against fake transports and isolated runtime
files, never a daemon or the network.
"""

from __future__ import annotations

import asyncio
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


class FakeHost(muse_msp.MspHost):
    """Fake transport under the real MspHost logic (no daemon, no wire)."""

    def __init__(self) -> None:
        # Deliberately no super().__init__: no daemon state, no file reads.
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

    def kinds(self):
        return [e.get("kind") for e in self.events]


def run(coro):
    return asyncio.run(coro)


class BusMixin:
    def setUp(self) -> None:
        import tempfile

        self.tmp = tempfile.TemporaryDirectory()
        self.old_claims = muse_msp.CLAIMS_FILE
        self.old_bus = muse_msp.BUS_LOG
        self.old_hosts = muse_msp.HOSTS_FILE
        muse_msp.CLAIMS_FILE = Path(self.tmp.name) / "claims.json"
        muse_msp.BUS_LOG = Path(self.tmp.name) / "bus.ndjson"
        muse_msp.HOSTS_FILE = Path(self.tmp.name) / "hosts.json"

    def tearDown(self) -> None:
        muse_msp.CLAIMS_FILE = self.old_claims
        muse_msp.BUS_LOG = self.old_bus
        muse_msp.HOSTS_FILE = self.old_hosts
        self.tmp.cleanup()


class MessageShapeTest(BusMixin, unittest.TestCase):
    def test_subjects_are_nats_shaped(self) -> None:
        self.assertEqual(
            muse_msp.BUS_SUBJECTS, ("m8s.claims", "m8s.heartbeat", "m8s.intent")
        )

    def test_constructors_carry_type_and_version(self) -> None:
        claim = muse_msp.make_claim("B", "X", "lane/x", now=NOW)
        self.assertEqual(
            (claim["type"], claim["version"]),
            (muse_msp.CLAIM_MESSAGE_TYPE, muse_msp.BUS_PROTOCOL_VERSION),
        )
        beat = muse_msp.make_heartbeat("B", "agent", [claim], now=NOW)
        self.assertEqual(
            (beat["type"], beat["version"]),
            (muse_msp.HEARTBEAT_MESSAGE_TYPE, muse_msp.BUS_PROTOCOL_VERSION),
        )
        intent = muse_msp.make_intent("B", "propose-plan", now=NOW)
        self.assertEqual(
            (intent["type"], intent["version"]),
            (muse_msp.INTENT_MESSAGE_TYPE, muse_msp.BUS_PROTOCOL_VERSION),
        )

    def test_messages_are_plain_json_transport_independent(self) -> None:
        claim = muse_msp.make_claim("B", "X", "lane/x", "/repo/x", now=NOW)
        beat = muse_msp.make_heartbeat("B", "agent", [claim], now=NOW)
        intent = muse_msp.make_intent("B", "v", "X", "lane/x", "d", now=NOW)
        for message in (claim, beat, intent):
            self.assertEqual(json.loads(json.dumps(message)), message)

    def test_validate_accepts_well_formed(self) -> None:
        muse_msp.validate_bus_message(
            "m8s.claims", muse_msp.make_claim("B", "X", "lane/x", now=NOW)
        )
        muse_msp.validate_bus_message(
            "m8s.heartbeat", muse_msp.make_heartbeat("B", "a", [], now=NOW)
        )
        muse_msp.validate_bus_message(
            "m8s.intent", muse_msp.make_intent("B", "v", now=NOW)
        )

    def test_validate_rejects_wrong_subject_type_version(self) -> None:
        claim = muse_msp.make_claim("B", "X", "lane/x", now=NOW)
        with self.assertRaises(muse_msp.BusError) as ctx:
            muse_msp.validate_bus_message("m8s.intent", claim)
        self.assertEqual(ctx.exception.kind, "badMessage")
        with self.assertRaises(muse_msp.BusError) as ctx:
            muse_msp.validate_bus_message("nats.nope", claim)
        self.assertEqual(ctx.exception.kind, "badSubject")
        future = {**claim, "version": claim["version"] + 1}
        with self.assertRaises(muse_msp.BusError) as ctx:
            muse_msp.validate_bus_message("m8s.claims", future)
        self.assertEqual(ctx.exception.kind, "badVersion")
        partial = {"type": "m8s.claim", "version": 1}
        with self.assertRaises(muse_msp.BusError) as ctx:
            muse_msp.validate_bus_message("m8s.claims", partial)
        self.assertEqual(ctx.exception.kind, "badMessage")

    def test_publish_and_read_round_trip_per_subject(self) -> None:
        claim = muse_msp.make_claim("B", "X", "lane/x", now=NOW)
        intent = muse_msp.make_intent("B", "v", now=NOW)
        muse_msp.publish("m8s.claims", claim)
        muse_msp.publish("m8s.intent", intent)
        intents = muse_msp.read_bus("m8s.intent")
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0]["message"]["verb"], "v")
        self.assertEqual(len(muse_msp.read_bus()), 2)
        with self.assertRaises(muse_msp.BusError) as ctx:
            muse_msp.read_bus("m8s.nope")
        self.assertEqual(ctx.exception.kind, "badSubject")


class LeaseTest(BusMixin, unittest.TestCase):
    def test_claim_acquire_and_reclaim_refresh(self) -> None:
        first = muse_msp.claim_branch("B", "X", "lane/x", now=NOW, ttl=100)
        second = muse_msp.claim_branch("B", "X", "lane/x", now=NOW + 10, ttl=100)
        self.assertEqual(first["claimId"], second["claimId"])
        self.assertEqual(second["expiresAt"], NOW + 110)
        log = muse_msp.read_bus("m8s.claims")
        self.assertEqual(len(log), 2)

    def test_one_lane_per_branch(self) -> None:
        muse_msp.claim_branch("B", "X", "lane/x", now=NOW, ttl=100)
        with self.assertRaises(muse_msp.BusError) as ctx:
            muse_msp.claim_branch("C", "Y", "lane/x", now=NOW + 10, ttl=100)
        self.assertEqual(ctx.exception.kind, "leaseHeld")

    def test_one_writer_per_checkout(self) -> None:
        muse_msp.claim_branch(
            "B", "X", "lane/x", checkout="/repo/x", now=NOW, ttl=100
        )
        with self.assertRaises(muse_msp.BusError) as ctx:
            muse_msp.claim_branch(
                "C", "Y", "lane/y", checkout="/repo/x", now=NOW + 10, ttl=100
            )
        self.assertEqual(ctx.exception.kind, "checkoutBusy")
        # Same writer may hold a second branch on its checkout.
        again = muse_msp.claim_branch(
            "B", "X", "lane/y", checkout="/repo/x", now=NOW + 10, ttl=100
        )
        self.assertEqual(again["branch"], "lane/y")

    def test_expiry_requeues_into_reassignment(self) -> None:
        muse_msp.claim_branch("B", "X", "lane/x", now=NOW, ttl=100)
        # Still held before expiry.
        with self.assertRaises(muse_msp.BusError):
            muse_msp.claim_branch("C", "Y", "lane/x", now=NOW + 50, ttl=100)
        # Expired: reassignment just works; new owner, new claim id.
        taken = muse_msp.claim_branch("C", "Y", "lane/x", now=NOW + 200, ttl=100)
        self.assertEqual((taken["host"], taken["lane"]), ("C", "Y"))

    def test_sweep_announces_expiry_once_with_requeue(self) -> None:
        muse_msp.claim_branch("B", "X", "lane/x", now=NOW, ttl=100)
        newly = muse_msp.sweep_claims(now=NOW + 500)
        self.assertEqual(len(newly), 1)
        self.assertEqual(newly[0]["branch"], "lane/x")
        self.assertEqual(muse_msp.sweep_claims(now=NOW + 500), [])
        table = muse_msp.claim_table(now=NOW + 500)
        self.assertFalse(table["lane/x"]["live"])
        self.assertTrue(table["lane/x"]["requeue"])

    def test_dead_host_claims_are_reassignable(self) -> None:
        muse_msp.enroll_host("peer", "peer", owner="agent-A")
        hosts = muse_msp.load_hosts()
        hosts["peer"]["lastHeartbeat"] = NOW - 10_000
        muse_msp.save_hosts(hosts)
        muse_msp.claim_branch("peer", "X", "lane/x", now=NOW, ttl=10_000)
        live_hosts = muse_msp.load_hosts()
        table = muse_msp.claim_table(now=NOW + 5, hosts=live_hosts)
        self.assertFalse(table["lane/x"]["live"])
        taken = muse_msp.claim_branch("other", "Y", "lane/x", now=NOW + 5, ttl=100)
        self.assertEqual(taken["host"], "other")

    def test_release_and_ownership(self) -> None:
        muse_msp.claim_branch("B", "X", "lane/x", now=NOW, ttl=100)
        with self.assertRaises(muse_msp.BusError) as ctx:
            muse_msp.release_claim("lane/x", host="C")
        self.assertEqual(ctx.exception.kind, "notOwner")
        released = muse_msp.release_claim("lane/x", host="B")
        self.assertEqual(released["lane"], "X")
        with self.assertRaises(muse_msp.BusError) as ctx:
            muse_msp.release_claim("lane/x")
        self.assertEqual(ctx.exception.kind, "leaseNotFound")

    def test_bad_claim_inputs_rejected(self) -> None:
        for args in (("", "X", "b"), ("B", "", "b"), ("B", "X", "")):
            with self.assertRaises(muse_msp.BusError):
                muse_msp.claim_branch(*args, now=NOW)
        with self.assertRaises(muse_msp.BusError):
            muse_msp.claim_branch("B", "X", "b", ttl=0, now=NOW)

    def test_lease_ttl_env_override(self) -> None:
        import os

        old = os.environ.get("M8S_LEASE_TTL_SECONDS")
        try:
            os.environ["M8S_LEASE_TTL_SECONDS"] = "60"
            self.assertEqual(muse_msp.lease_ttl_seconds(), 60)
            os.environ["M8S_LEASE_TTL_SECONDS"] = "bogus"
            self.assertEqual(
                muse_msp.lease_ttl_seconds(), muse_msp.LEASE_TTL_SECONDS
            )
        finally:
            if old is None:
                os.environ.pop("M8S_LEASE_TTL_SECONDS", None)
            else:
                os.environ["M8S_LEASE_TTL_SECONDS"] = old


class HeartbeatGossipTest(BusMixin, unittest.TestCase):
    def test_heartbeat_regossips_claims(self) -> None:
        muse_msp.claim_branch("B", "X", "lane/x", now=NOW, ttl=100)
        refreshed = muse_msp.refresh_claims("B", now=NOW + 200, ttl=100)
        self.assertEqual(len(refreshed), 1)
        self.assertEqual(refreshed[0]["expiresAt"], NOW + 300)
        claims_log = muse_msp.read_bus("m8s.claims")
        # Acquire + re-gossip.
        self.assertEqual(len(claims_log), 2)
        self.assertEqual(
            [r["message"]["expiresAt"] for r in claims_log], [NOW + 100, NOW + 300]
        )

    def test_heartbeat_with_no_claims_publishes_nothing(self) -> None:
        self.assertEqual(muse_msp.refresh_claims("ghost", now=NOW), [])
        self.assertEqual(muse_msp.read_bus(), [])

    def test_p3_host_heartbeat_reuses_lease_heartbeat(self) -> None:
        async def go():
            host = FakeHost()
            real_probe = muse_msp.ssh_probe
            muse_msp.ssh_probe = lambda *a, **k: {"musePath": "m", "version": "v"}  # type: ignore[assignment]
            try:
                await muse_msp.dispatch(
                    host,  # type: ignore[arg-type]
                    {"command": "host", "action": "enroll", "name": "peer",
                     "target": "peer"},
                )
                await muse_msp.dispatch(
                    host,  # type: ignore[arg-type]
                    {"command": "bus", "action": "claim", "host": "peer",
                     "lane": "X", "branch": "lane/x", "ttl": 100},
                )
                first_expiry = muse_msp.load_claims()["lane/x"]["expiresAt"]
                result = await muse_msp.dispatch(
                    host,  # type: ignore[arg-type]
                    {"command": "host", "action": "heartbeat", "name": "peer"},
                )
            finally:
                muse_msp.ssh_probe = real_probe
            return host, result, first_expiry

        host, result, first_expiry = run(go())
        self.assertEqual(result["host"]["name"], "peer")
        self.assertGreater(
            muse_msp.load_claims()["lane/x"]["expiresAt"], first_expiry
        )
        self.assertIn("claim.heartbeat", host.kinds())
        heartbeats = muse_msp.read_bus("m8s.heartbeat")
        self.assertEqual(len(heartbeats), 1)
        self.assertEqual(heartbeats[0]["message"]["host"], "peer")


class DispatchWiringTest(BusMixin, unittest.TestCase):
    def dispatch(self, host: FakeHost, request: dict):
        async def go():
            return await muse_msp.dispatch(host, request)  # type: ignore[arg-type]

        return run(go())

    def test_bus_claim_intent_list_release_flow(self) -> None:
        host = FakeHost()
        claimed = self.dispatch(
            host,
            {"command": "bus", "action": "claim", "host": "B", "lane": "X",
             "branch": "lane/x", "checkout": "/repo/x", "ttl": 100},
        )
        self.assertEqual(claimed["claim"]["branch"], "lane/x")
        intent = self.dispatch(
            host,
            {"command": "bus", "action": "intent", "host": "B", "verb": "v",
             "branch": "lane/x", "detail": "d"},
        )
        self.assertEqual(intent["intent"]["verb"], "v")
        listed = self.dispatch(host, {"command": "bus", "action": "list"})
        self.assertTrue(listed["claims"]["lane/x"]["live"])
        read = self.dispatch(
            host, {"command": "bus", "action": "read", "subject": "m8s.intent"}
        )
        self.assertEqual(len(read["records"]), 1)
        released = self.dispatch(
            host, {"command": "bus", "action": "release", "branch": "lane/x"}
        )
        self.assertEqual(released["claim"]["branch"], "lane/x")
        for kind in (
            "claim.acquired",
            "intent.published",
            "claim.released",
        ):
            self.assertIn(kind, host.kinds())

    def test_bus_contention_errors_carry_kind(self) -> None:
        host = FakeHost()
        self.dispatch(
            host,
            {"command": "bus", "action": "claim", "host": "B", "lane": "X",
             "branch": "lane/x", "ttl": 100},
        )

        async def held():
            with self.assertRaises(muse_msp.BusError) as ctx:
                await muse_msp.dispatch(  # type: ignore[arg-type]
                    host,
                    {"command": "bus", "action": "claim", "host": "C",
                     "lane": "Y", "branch": "lane/x", "ttl": 100},
                )
            return ctx.exception

        self.assertEqual(run(held()).kind, "leaseHeld")

    def test_bus_claim_requires_lane_and_branch(self) -> None:
        host = FakeHost()
        with self.assertRaises(muse_msp.BusError):
            self.dispatch(host, {"command": "bus", "action": "claim", "host": "B"})
        with self.assertRaises(muse_msp.BusError):
            self.dispatch(host, {"command": "bus", "action": "intent", "host": "B"})

    def test_list_shows_claims_and_session_lease(self) -> None:
        host = FakeHost()
        host.sessions["session-1"] = {"sessionId": "session-1", "alias": "lane"}
        self.dispatch(
            host,
            {"command": "bus", "action": "claim", "host": "B", "lane": "lane",
             "branch": "lane/x", "session": "lane", "ttl": 10_000},
        )
        result = self.dispatch(host, {"command": "list"})
        self.assertTrue(result["claims"]["lane/x"]["live"])
        self.assertEqual(result["sessions"][0]["lease"]["branch"], "lane/x")

    def test_bus_cli_builds_requests(self) -> None:
        req = muse_msp.build_request(
            parse(["bus", "claim", "--host", "B", "--lane", "X", "--branch", "b"])
        )
        self.assertEqual(req, {"command": "bus", "action": "claim", "host": "B",
                               "lane": "X", "branch": "b", "limit": 200})
        req = muse_msp.build_request(parse(["bus", "intent", "--verb", "v"]))
        self.assertEqual(req["verb"], "v")
        req = muse_msp.build_request(
            parse(["bus", "read", "--subject", "m8s.claims", "--limit", "5"])
        )
        self.assertEqual(req["subject"], "m8s.claims")
        self.assertEqual(req["limit"], 5)
        for argv in (
            ["bus", "claim", "--lane", "X"],
            ["bus", "release"],
            ["bus", "intent", "--lane", "X"],
        ):
            with self.subTest(argv=argv):
                with self.assertRaises(SystemExit):
                    muse_msp.build_request(parse(argv))

    def test_control_envelope_carries_bus_error_kind(self) -> None:
        import json as _json

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
            reader.feed_data(
                (
                    _json.dumps({"command": "bus", "action": "release",
                                 "branch": "ghost"})
                    + "\n"
                ).encode()
            )
            reader.feed_eof()
            writer = FakeWriter()
            await muse_msp.handle_client(host, reader, writer)  # type: ignore[arg-type]
            return _json.loads(bytes(writer.data).decode())

        response = run(go())
        self.assertFalse(response["ok"])
        self.assertEqual(response["errorKind"], "leaseNotFound")


if __name__ == "__main__":
    unittest.main()
