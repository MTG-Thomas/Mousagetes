#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Typed host-call timeouts (issue #37).

Stdlib unittest only. Exercises the real ``MspHost.call`` timeout path
against a never-answering stub transport — no daemon, no network, no
retries. Fails red on the old behavior (bare ``asyncio.TimeoutError``
with an empty message and no ``kind``).
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


def run(coro):
    return asyncio.run(coro)


class ColdHost(muse_msp.MspHost):
    """Real ``MspHost.call`` over a transport that never answers."""

    def __init__(self) -> None:
        # No super().__init__: no budget-file reads; only the state that
        # the real MspHost.call touches.
        self.pending: dict = {}
        self.next_id = 1
        self.watchers = set()
        self.writes: list[dict] = []

    def record(self, record: dict) -> None:
        pass

    async def _write(self, frame: dict) -> None:
        self.writes.append(frame)
        # Never resolve the pending future: the host stays cold.


class HostTimeoutTest(unittest.TestCase):
    def test_default_timeout_unchanged(self) -> None:
        # Guard against hiding cold-host flakes by bumping the timeout.
        self.assertEqual(muse_msp.HOST_CALL_TIMEOUT_SECONDS, 60)

    def test_call_timeout_is_typed_with_method_and_duration(self) -> None:
        async def go():
            host = ColdHost()
            old = muse_msp.HOST_CALL_TIMEOUT_SECONDS
            muse_msp.HOST_CALL_TIMEOUT_SECONDS = 0.02
            try:
                with self.assertRaises(muse_msp.HostTimeoutError) as ctx:
                    await muse_msp.MspHost.call(host, "session/list", {"limit": 200})
            finally:
                muse_msp.HOST_CALL_TIMEOUT_SECONDS = old
            return host, ctx.exception

        host, exc = run(go())
        self.assertEqual(exc.kind, "hostTimeout")
        self.assertEqual(exc.method, "session/list")
        self.assertEqual(exc.timeout, 0.02)
        self.assertTrue(str(exc), "timeout message must be non-empty")
        self.assertIn("session/list", str(exc))
        self.assertIsInstance(exc, TimeoutError)
        # One wire write, no retry of the uncertain mutation.
        self.assertEqual(len(host.writes), 1)
        self.assertEqual(host.writes[0]["method"], "session/list")
        # The dead future is dropped so a late frame cannot leak.
        self.assertEqual(host.pending, {})

    def test_control_boundary_preserves_error_kind(self) -> None:
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
            host = ColdHost()
            old = muse_msp.HOST_CALL_TIMEOUT_SECONDS
            muse_msp.HOST_CALL_TIMEOUT_SECONDS = 0.02
            try:
                reader = asyncio.StreamReader()
                reader.feed_data(
                    (json.dumps({"command": "call", "method": "session/list"}) + "\n").encode()
                )
                reader.feed_eof()
                writer = FakeWriter()
                await muse_msp.handle_client(host, reader, writer)  # type: ignore[arg-type]
                return json.loads(bytes(writer.data).decode())
            finally:
                muse_msp.HOST_CALL_TIMEOUT_SECONDS = old

        response = run(go())
        self.assertFalse(response["ok"])
        self.assertEqual(response["errorKind"], "hostTimeout")
        self.assertTrue(response["error"], "control error must be non-empty")
        self.assertIn("session/list", response["error"])


if __name__ == "__main__":
    unittest.main()
