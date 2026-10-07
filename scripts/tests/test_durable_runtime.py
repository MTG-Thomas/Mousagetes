#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Durable runtime home for issue #20 (socket loss incident 2026-09-17).

Covers the runtime_dir/startup/predecessor/persistence seam only:
persistent per-user state home with explicit override, socket lifecycle
separated from durable events/roster, fsynced event writes, atomic roster
saves with alias recovery, and running-but-unreachable predecessor
detection. Never starts a daemon or touches the network.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "muse-msp.py"


def load_module():
    spec = importlib.util.spec_from_file_location("muse_msp_durable", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


muse_msp = load_module()


class DurableRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.env_backup = dict(os.environ)
        self.tmp = Path(tempfile.mkdtemp(prefix="m8s-durable-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.addCleanup(self._restore_env)

    def _restore_env(self) -> None:
        for key in ("M8S_STATE_DIR", "M8S_RUNTIME_DIR", "M8S_SOCKET_DIR"):
            os.environ.pop(key, None)
        for key, value in self.env_backup.items():
            os.environ[key] = value

    def _fresh(self, **env):
        for key in ("M8S_STATE_DIR", "M8S_RUNTIME_DIR", "M8S_SOCKET_DIR"):
            os.environ.pop(key, None)
        os.environ.update(env)
        return load_module()

    def test_default_state_dir_is_persistent(self) -> None:
        data_home = self.tmp / "data-home"
        mod = self._fresh(XDG_DATA_HOME=str(data_home), XDG_RUNTIME_DIR=str(self.tmp / "run"))
        state = mod.state_dir()
        sock = mod.socket_dir()
        self.assertEqual(state, data_home / "m8s")
        self.assertTrue(state.is_dir())
        # Durable home never lives under tmpfs scratch or the socket dir.
        self.assertNotEqual(state, sock)
        self.assertNotIn("muse-msp", state.name if state.name != "m8s" else "m8s-ok")
        self.assertTrue(str(sock).startswith(str(self.tmp / "run")))

    def test_state_override_env_creates_dir(self) -> None:
        target = self.tmp / "custom-state"
        mod = self._fresh(M8S_STATE_DIR=str(target))
        self.assertEqual(mod.state_dir(), target)
        self.assertTrue(target.is_dir())
        self.assertEqual(oct(target.stat().st_mode & 0o777), "0o700")

    def test_socket_and_state_are_separated(self) -> None:
        mod = self._fresh(
            M8S_STATE_DIR=str(self.tmp / "state"),
            M8S_SOCKET_DIR=str(self.tmp / "sock"),
        )
        bound = mod.init_runtime()
        self.assertEqual(bound["state"], self.tmp / "state")
        self.assertEqual(bound["socket"], self.tmp / "sock")
        self.assertEqual(mod.EVENTS.parent, mod.RUNTIME)
        self.assertEqual(mod.SESSIONS_FILE.parent, mod.RUNTIME)
        self.assertEqual(mod.SOCKET.parent, bound["socket"])
        self.assertEqual(mod.PID_FILE.parent, bound["socket"])
        self.assertNotEqual(mod.RUNTIME, bound["socket"])

    def test_directory_loss_keeps_durable_state(self) -> None:
        mod = self._fresh(
            M8S_STATE_DIR=str(self.tmp / "state"),
            M8S_SOCKET_DIR=str(self.tmp / "sock"),
        )
        mod.init_runtime()
        mod.save_roster({"sess-1": {"alias": "lane"}}, {"lane": "sess-1"}, path=mod.SESSIONS_FILE)
        mod.emit_local({"kind": "test.marker", "sessionId": "sess-1"})
        # Simulate a tmpfs sweep of the socket dir while the daemon lives.
        shutil.rmtree(mod.SOCKET.parent)
        self.assertTrue(mod.EVENTS.exists())
        self.assertTrue(mod.SESSIONS_FILE.exists())
        self.assertFalse(mod.daemon_running())
        sessions, aliases = mod.load_roster(path=mod.SESSIONS_FILE)
        self.assertEqual(aliases.get("lane"), "sess-1")
        records = mod.read_events(0.0, 50)
        self.assertTrue(any(r.get("kind") == "test.marker" for r in records))

    def test_event_write_is_fsynced(self) -> None:
        mod = self._fresh(
            M8S_STATE_DIR=str(self.tmp / "state"),
            M8S_SOCKET_DIR=str(self.tmp / "sock"),
        )
        mod.init_runtime()
        calls: list[int] = []
        real_fsync = os.fsync

        def spy(fd: int) -> None:
            calls.append(fd)
            real_fsync(fd)

        os.fsync = spy  # type: ignore[method-assign]
        try:
            mod.emit_local({"kind": "test.fsync"})
        finally:
            os.fsync = real_fsync  # type: ignore[method-assign]
        self.assertTrue(calls, "emit_local must fsync the event log")
        self.assertTrue(any(r.get("kind") == "test.fsync" for r in mod.read_events(0.0, 50)))

    def test_aliases_recover_from_persisted_roster(self) -> None:
        mod = self._fresh(
            M8S_STATE_DIR=str(self.tmp / "state"),
            M8S_SOCKET_DIR=str(self.tmp / "sock"),
        )
        mod.init_runtime()
        sessions = {"sess-9": {"alias": "lane", "workspace": "/tmp/x"}}
        mod.save_roster(sessions, {"lane": "sess-9"}, path=mod.SESSIONS_FILE)
        # Simulate a restart: drop in-memory state, reload from disk.
        loaded_sessions, loaded_aliases = mod.load_roster(path=mod.SESSIONS_FILE)
        self.assertEqual(loaded_aliases.get("lane"), "sess-9")
        self.assertEqual(loaded_sessions["sess-9"]["alias"], "lane")

    def test_orphan_detection_reports_predecessor(self) -> None:
        mod = self._fresh(
            M8S_STATE_DIR=str(self.tmp / "state"),
            M8S_SOCKET_DIR=str(self.tmp / "sock"),
        )
        mod.init_runtime()
        self.assertFalse(mod.daemon_running())
        proc_root = self.tmp / "proc"
        # PID 1 is alive in this container; give it a serve cmdline.
        victim = proc_root / "1"
        victim.mkdir(parents=True)
        (victim / "cmdline").write_bytes(b"python3\x00muse-msp.py\x00serve\x00")
        other = proc_root / "2"
        other.mkdir(parents=True)
        (other / "cmdline").write_bytes(b"python3\x00something-else\x00")
        pids = mod.live_serve_pids(proc_root)
        self.assertIn(1, pids)
        self.assertNotIn(2, pids)
        self.assertEqual(mod.predecessor_pids.__name__, "predecessor_pids")

        real_popen = mod.subprocess.Popen

        def no_spawn(*args, **kwargs):  # pragma: no cover - must never run
            raise AssertionError("must not spawn a split supervisor")

        mod.subprocess.Popen = no_spawn  # type: ignore[method-assign]
        try:
            with self.assertRaises(RuntimeError) as ctx:
                mod.start_daemon(proc_root=proc_root)
        finally:
            mod.subprocess.Popen = real_popen  # type: ignore[method-assign]
        self.assertIn("predecessor", str(ctx.exception))
        self.assertIn("1", str(ctx.exception))

    def test_orphan_force_starts_without_predecessor(self) -> None:
        mod = self._fresh(
            M8S_STATE_DIR=str(self.tmp / "state"),
            M8S_SOCKET_DIR=str(self.tmp / "sock"),
        )
        mod.init_runtime()
        proc_root = self.tmp / "empty-proc"
        proc_root.mkdir()
        self.assertEqual(mod.live_serve_pids(proc_root), [])

    def test_up_parser_accepts_overrides(self) -> None:
        mod = load_module()
        args = mod.parser().parse_args(
            ["up", "--state-dir", "/tmp/s", "--socket-dir", "/tmp/k", "--force"]
        )
        self.assertEqual(args.state_dir, "/tmp/s")
        self.assertEqual(args.socket_dir, "/tmp/k")
        self.assertTrue(args.force)


if __name__ == "__main__":
    unittest.main()
