#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for Moonshot P3: SSH host enrollment, namespaced identity, placement.

Stdlib unittest only. Exercises the registry, SSH carrier argv, lane
namespacing, and placement ranking against fake transports — never starts a
daemon or touches the network. Live SSH enrollment is verified separately
(see docs/multihost.md operator steps).
"""

from __future__ import annotations

import asyncio
import importlib.util
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


def run(coro):
    return asyncio.run(coro)


class RegistryMixin:
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.old = muse_msp.HOSTS_FILE
        muse_msp.HOSTS_FILE = Path(self.tmp.name) / "hosts.json"

    def tearDown(self) -> None:
        muse_msp.HOSTS_FILE = self.old
        self.tmp.cleanup()


class SshCarrierTest(unittest.TestCase):
    def test_serve_argv_is_local_stdio(self) -> None:
        host = FakeHost()
        self.assertEqual(
            muse_msp.MspHost.serve_argv(host),
            ["muse", "serve", "--trust-workspace", "--disable-sandbox"],
        )

    def test_remote_argv_wraps_serve_in_ssh(self) -> None:
        remote = muse_msp.RemoteMspHost.__new__(muse_msp.RemoteMspHost)
        remote.ssh_target = "peer"
        remote.ssh_port = None
        argv = remote.serve_argv()
        self.assertEqual(argv[0], "ssh")
        self.assertIn("peer", argv)
        self.assertIn("--", argv)
        tail = argv[argv.index("--") + 1:]
        self.assertEqual(tail, ["muse", "serve", "--trust-workspace", "--disable-sandbox"])

    def test_remote_argv_carries_port(self) -> None:
        argv = muse_msp.build_ssh_serve_argv("peer", 2222)
        self.assertIn("-p", argv)
        self.assertIn("2222", argv)

    def test_remote_argv_rejects_bad_targets(self) -> None:
        for bad in ("", "  ", "has space", None):
            with self.subTest(target=bad):
                with self.assertRaises(muse_msp.HostError):
                    muse_msp.build_ssh_serve_argv(bad)  # type: ignore[arg-type]

    def test_no_tcp_listener_for_msp(self) -> None:
        """MSP stays stdio-bound: no TCP server or port/bind flags anywhere."""
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("asyncio.start_server(", source)
        self.assertNotIn("socketserver.TCPServer", source)
        for argv in (
            muse_msp.MspHost.serve_argv(FakeHost()),
            muse_msp.build_ssh_serve_argv("peer"),
        ):
            self.assertFalse(any(tok.startswith("--port") for tok in argv))
            self.assertFalse(any(tok.startswith("--bind") for tok in argv))

    def test_remote_host_inherits_supervision(self) -> None:
        """RemoteMspHost is an MspHost: budgets/stuck/validation unchanged."""
        self.assertTrue(issubclass(muse_msp.RemoteMspHost, muse_msp.MspHost))
        with self.assertRaises(muse_msp.CallValidationError):
            muse_msp.validate_call("nope/method", {})

    def test_probe_success_shape(self) -> None:
        class Proc:
            returncode = 0
            stdout = "/root/.local/bin/muse\nMuse Code 1.3.0\n"
            stderr = ""

        probe = muse_msp.ssh_probe("peer", run=lambda *a, **k: Proc())
        self.assertEqual(probe["musePath"], "/root/.local/bin/muse")
        self.assertIn("1.3.0", probe["version"])

    def test_probe_failure_is_typed(self) -> None:
        class Proc:
            returncode = 255
            stdout = ""
            stderr = "ssh: connect to host peer port 22: refused"

        with self.assertRaises(muse_msp.HostError) as ctx:
            muse_msp.ssh_probe("peer", run=lambda *a, **k: Proc())
        self.assertEqual(ctx.exception.kind, "hostUnreachable")


class RegistryTest(RegistryMixin, unittest.TestCase):
    def test_enroll_list_remove_round_trip(self) -> None:
        record = muse_msp.enroll_host(
            "peer", "user@peer", 4, probe={"musePath": "muse", "version": "v"}
        )
        self.assertEqual(record["sshTarget"], "user@peer")
        self.assertTrue(record["alive"])
        self.assertEqual(set(muse_msp.load_hosts()), {"peer"})
        removed = muse_msp.remove_host("peer")
        self.assertEqual(removed["name"], "peer")
        self.assertEqual(muse_msp.load_hosts(), {})

    def test_second_agent_cannot_share_live_host(self) -> None:
        muse_msp.enroll_host("peer", "user@peer", owner="agent-A")
        with self.assertRaises(muse_msp.HostError) as ctx:
            muse_msp.enroll_host("peer", "user@peer", owner="agent-B")
        self.assertEqual(ctx.exception.kind, "hostOwned")

    def test_stale_owner_may_be_replaced(self) -> None:
        muse_msp.enroll_host("peer", "user@peer", owner="agent-A")
        hosts = muse_msp.load_hosts()
        hosts["peer"]["lastHeartbeat"] = NOW - 10_000
        muse_msp.save_hosts(hosts)
        record = muse_msp.enroll_host("peer", "user@peer", owner="agent-B")
        self.assertEqual(record["agentId"], "agent-B")

    def test_same_owner_reenroll_updates(self) -> None:
        muse_msp.enroll_host("peer", "user@peer", 2, owner="agent-A")
        record = muse_msp.enroll_host(
            "peer", "user@peer", 8, remote_msp="/r/muse-msp.py", owner="agent-A"
        )
        self.assertEqual(record["maxLanes"], 8)
        self.assertEqual(record["remoteMsp"], "/r/muse-msp.py")

    def test_bad_names_and_caps_rejected(self) -> None:
        for bad in ("a/b", "", "has space", "x!y"):
            with self.subTest(name=bad):
                with self.assertRaises(muse_msp.HostError):
                    muse_msp.enroll_host(bad, "peer", owner="a")
        with self.assertRaises(muse_msp.HostError):
            muse_msp.enroll_host("peer", "peer", 0, owner="a")
        with self.assertRaises(muse_msp.HostError):
            muse_msp.remove_host("ghost")

    def test_liveness_ttl(self) -> None:
        record = muse_msp.enroll_host("peer", "peer", owner="a")
        self.assertTrue(muse_msp.host_alive(record, time.time()))
        stale = {**record, "lastHeartbeat": NOW - 10_000}
        self.assertFalse(muse_msp.host_alive(stale, NOW))
        marked = {**record, "alive": False, "lastHeartbeat": time.time()}
        self.assertFalse(muse_msp.host_alive(marked, time.time()))
        old = __import__("os").environ.get("M8S_HOST_TTL_SECONDS")
        try:
            __import__("os").environ["M8S_HOST_TTL_SECONDS"] = "bogus"
            self.assertEqual(
                muse_msp.host_ttl_seconds(), muse_msp.HOST_ALIVE_TTL_SECONDS
            )
        finally:
            if old is None:
                __import__("os").environ.pop("M8S_HOST_TTL_SECONDS", None)
            else:
                __import__("os").environ["M8S_HOST_TTL_SECONDS"] = old

    def test_heartbeat_refreshes_liveness(self) -> None:
        muse_msp.enroll_host("peer", "peer", owner="a")
        hosts = muse_msp.load_hosts()
        hosts["peer"]["lastHeartbeat"] = NOW - 10_000
        muse_msp.save_hosts(hosts)
        record = muse_msp.heartbeat_host("peer")
        self.assertTrue(muse_msp.host_alive(record, time.time()))


class IdentityTest(unittest.TestCase):
    def test_unqualified_ref_is_local(self) -> None:
        self.assertEqual(muse_msp.split_lane_ref("lane"), (None, "lane"))

    def test_qualified_ref_splits(self) -> None:
        self.assertEqual(muse_msp.split_lane_ref("peer/lane"), ("peer", "lane"))

    def test_same_alias_on_two_hosts_never_collides(self) -> None:
        local = muse_msp.split_lane_ref("lane")
        remote = muse_msp.split_lane_ref("peer/lane")
        self.assertNotEqual(
            (local, None) if local[0] is None else local, remote
        )
        self.assertEqual(muse_msp.qualify_lane("peer", "lane"), "peer/lane")
        self.assertEqual(muse_msp.qualify_lane("local", "lane"), "local/lane")

    def test_malformed_refs_rejected(self) -> None:
        for bad in ("", "/lane", "peer/", "a/b/c", "bad host/lane"):
            with self.subTest(ref=bad):
                with self.assertRaises(muse_msp.HostError):
                    muse_msp.split_lane_ref(bad)

    def test_peer_argv_needs_remote_msp(self) -> None:
        with self.assertRaises(muse_msp.HostError) as ctx:
            muse_msp.peer_cli_argv({"name": "peer", "sshTarget": "peer"}, ["list"])
        self.assertEqual(ctx.exception.kind, "noRemoteMsp")
        argv = muse_msp.peer_cli_argv(
            {"name": "peer", "sshTarget": "peer", "remoteMsp": "/r/muse-msp.py"},
            ["list"],
        )
        self.assertEqual(argv[0], "ssh")
        self.assertEqual(argv[-3:], ["python3", "/r/muse-msp.py", "list"])
        # Agent-to-agent: the peer's serve stdio is never addressed directly.
        self.assertNotIn("serve", argv)


class PlacementTest(RegistryMixin, unittest.TestCase):
    def live(self, name: str, max_lanes: int = 4) -> dict:
        return {
            name: {
                "name": name,
                "sshTarget": name,
                "maxLanes": max_lanes,
                "alive": True,
                "lastHeartbeat": NOW,
            }
        }

    def test_checkout_affinity_wins(self) -> None:
        hosts = {**self.live("a"), **self.live("b")}
        lanes = {"a": [{"checkout": "/repo/x"}], "b": [{}]}
        ranked = muse_msp.place_lane({"checkout": "/repo/x"}, hosts, lanes, NOW)
        self.assertEqual(ranked[0]["host"], "a")
        self.assertTrue(any("affinity" in r for r in ranked[0]["reasons"]))

    def test_branch_collision_avoided(self) -> None:
        hosts = {**self.live("a"), **self.live("b")}
        lanes = {"a": [{"branch": "lane/x"}], "b": []}
        ranked = muse_msp.place_lane({"branch": "lane/x"}, hosts, lanes, NOW)
        self.assertEqual(ranked[0]["host"], "b")
        self.assertTrue(any("collision" in r for r in ranked[-1]["reasons"]))

    def test_file_overlap_avoided(self) -> None:
        hosts = {**self.live("a"), **self.live("b")}
        lanes = {"a": [{"files": ["scripts/muse-msp.py"]}], "b": []}
        ranked = muse_msp.place_lane(
            {"files": ["scripts/muse-msp.py"]}, hosts, lanes, NOW
        )
        self.assertEqual(ranked[0]["host"], "b")

    def test_full_and_dead_hosts_excluded(self) -> None:
        hosts = {**self.live("full", 1), **self.live("dead")}
        hosts["dead"]["lastHeartbeat"] = NOW - 10_000
        lanes = {"full": [{"branch": "other"}]}
        self.assertEqual(muse_msp.place_lane({}, hosts, lanes, NOW), [])
        with self.assertRaises(muse_msp.HostError) as ctx:
            muse_msp.pick_host({}, hosts, lanes, NOW)
        self.assertEqual(ctx.exception.kind, "noCapacity")

    def test_capacity_prefers_roomier_host(self) -> None:
        hosts = {**self.live("a"), **self.live("b")}
        lanes = {"a": [{}, {}], "b": [{}]}
        self.assertEqual(muse_msp.pick_host({}, hosts, lanes, NOW)["host"], "b")


class DispatchTest(RegistryMixin, unittest.TestCase):
    def dispatch(self, host: FakeHost, request: dict):
        async def go():
            return await muse_msp.dispatch(host, request)  # type: ignore[arg-type]

        return run(go())

    def test_host_list_empty(self) -> None:
        result = self.dispatch(FakeHost(), {"command": "host", "action": "list"})
        self.assertEqual(result, {"hosts": []})

    def test_host_enroll_requires_probe_and_names(self) -> None:
        host = FakeHost()
        with self.assertRaises(muse_msp.HostError):
            self.dispatch(host, {"command": "host", "action": "enroll"})
        real_probe = muse_msp.ssh_probe
        muse_msp.ssh_probe = lambda *a, **k: {"musePath": "m", "version": "v"}  # type: ignore[assignment]
        try:
            result = self.dispatch(
                host,
                {"command": "host", "action": "enroll", "name": "peer", "target": "peer"},
            )
        finally:
            muse_msp.ssh_probe = real_probe
        self.assertEqual(result["host"]["name"], "peer")
        listed = self.dispatch(FakeHost(), {"command": "host", "action": "list"})
        self.assertEqual(len(listed["hosts"]), 1)
        self.assertTrue(listed["hosts"][0]["live"])

    def test_host_errors_carry_kind(self) -> None:
        host = FakeHost()
        with self.assertRaises(muse_msp.HostError) as ctx:
            self.dispatch(host, {"command": "host", "action": "remove", "name": "ghost"})
        self.assertEqual(ctx.exception.kind, "hostNotFound")

    def test_place_ranks_enrolled_hosts(self) -> None:
        muse_msp.enroll_host("peer", "peer", owner="a")
        result = self.dispatch(
            FakeHost(), {"command": "place", "branch": "lane/x", "lanesByHost": {}}
        )
        self.assertEqual(result["ranking"][0]["host"], "peer")

    def test_cli_builds_requests(self) -> None:
        req = muse_msp.build_request(parse(["host", "enroll", "--name", "p", "--target", "p"]))
        self.assertEqual(req, {"command": "host", "action": "enroll", "name": "p",
                               "target": "p", "maxLanes": 4})
        req = muse_msp.build_request(
            parse(["place", "--branch", "b", "--files", "x,y"])
        )
        self.assertEqual(req["command"], "place")
        self.assertEqual(req["files"], ["x", "y"])


if __name__ == "__main__":
    unittest.main()
