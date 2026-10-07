# SPDX-License-Identifier: AGPL-3.0-or-later
"""Real daemon dispatch and MSP stdio framing against a local contract peer.

The adapter socket suite covers ControlClient framing. These tests cover
the other side of that boundary with the real MspHost.call/_read_stdout,
including alias races and a queued turn advancing after the fence read.
Runtime acceptance must separately verify installed Muse honors turnId.
"""
import asyncio
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "muse-msp.py"
spec = importlib.util.spec_from_file_location("msp_safety", SCRIPT)
msp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(msp)


class DaemonSafetyTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix="m8s-msp-safety-")
        self.addCleanup(self.workspace.cleanup)
        self.host = msp.MspHost()
        self.host.budgets = {}
        self.host.retired = {}
        self.events = []
        self.host.record = self.events.append
        self.snapshots = []
        self.host.snapshot_roster = lambda: self.snapshots.append(dict(self.host.sessions))
        self.frames = []
        original_write = self.host._write

        async def tracked_write(frame):
            self.frames.append(frame)
            await original_write(frame)

        self.host._write = tracked_write
        self.host.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", str(Path(__file__).parent / "fixtures" / "msp_turn_host.py"),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.reader = asyncio.create_task(self.host._read_stdout())

    async def asyncTearDown(self):
        self.host.proc.terminate()
        await self.host.proc.wait()
        await self.reader

    async def launch(self):
        return await msp.dispatch(self.host, {"command": "launch", "name": "unique",
            "workspace": self.workspace.name, "prompt": "local fixture only"})

    async def test_concurrent_alias_launch_creates_one_session(self):
        replies = await asyncio.gather(self.launch(), self.launch(), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, dict) for r in replies), 1)
        refusal = next(r for r in replies if isinstance(r, Exception))
        self.assertEqual(refusal.kind, "aliasInUse")
        self.assertEqual(sum(f["method"] == "session/start" for f in self.frames), 1)
        self.assertEqual(self.host.sessions["session-1"]["launchTurnId"], "turn-1")

    async def test_cancel_carries_exact_turn_on_msp_wire(self):
        await self.launch()
        reply = await msp.dispatch(self.host, {"command": "call", "method": "turn/cancel",
            "session": "session-1", "params": {"expectedTurnId": "turn-1"}})
        self.assertEqual(reply["cancelledTurnId"], "turn-1")
        frame = next(f for f in self.frames if f["method"] == "turn/cancel")
        self.assertEqual(frame["params"]["turnId"], "turn-1")
        self.assertNotIn("expectedTurnId", frame["params"])

    async def test_later_turn_rejects_before_cancel_wire(self):
        await self.launch()
        await self.host.submit("session-1", "later")
        with self.assertRaises(msp.CallValidationError) as ctx:
            await msp.dispatch(self.host, {"command": "call", "method": "turn/cancel",
                "session": "session-1", "params": {"expectedTurnId": "turn-1"}})
        self.assertEqual(ctx.exception.kind, "turnChanged")
        self.assertFalse(any(f["method"] == "turn/cancel" for f in self.frames))

    async def test_queued_turn_advancement_cannot_cancel_foreign_turn(self):
        await self.launch()
        await self.host.call("test/advanceBeforeCancel")
        with self.assertRaisesRegex(RuntimeError, "turnChanged"):
            await msp.dispatch(self.host, {"command": "call", "method": "turn/cancel",
                "session": "session-1", "params": {"expectedTurnId": "turn-1"}})
        read = await self.host.call("session/read", {"sessionId": "session-1"})
        self.assertEqual(read["session"]["activeTurnId"], "foreign-turn")
