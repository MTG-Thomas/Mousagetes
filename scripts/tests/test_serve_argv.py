#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for the daemon-owned read-only host seam (`M8S_SERVE_ARGV`).

The MCP adapter verifies a read-only daemon by its advertised posture
instead of assuming it, so the advertisement itself needs pinning: the
default argv stays trusted, the env override replaces it in full, and
the `health` control command carries the exact argv the daemon spawned
with. Never starts a daemon, never spawns `muse serve`.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import unittest
from pathlib import Path
from typing import Any

SCRIPT = Path(__file__).resolve().parent.parent / "muse-msp.py"


def load_module():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location("muse_msp", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


muse_msp = load_module()


class ServeArgvTests(unittest.TestCase):
    def test_default_argv_is_trusted(self) -> None:
        saved = os.environ.pop("M8S_SERVE_ARGV", None)
        try:
            self.assertEqual(
                muse_msp.serve_argv(),
                ["muse", "serve", "--trust-workspace", "--disable-sandbox"],
            )
        finally:
            if saved is not None:
                os.environ["M8S_SERVE_ARGV"] = saved

    def test_env_override_replaces_argv_in_full(self) -> None:
        saved = os.environ.get("M8S_SERVE_ARGV")
        os.environ["M8S_SERVE_ARGV"] = "muse serve --disable-write --disable-shell"
        try:
            self.assertEqual(
                muse_msp.serve_argv(),
                ["muse", "serve", "--disable-write", "--disable-shell"],
            )
            self.assertEqual(muse_msp.MspHost().serve_argv(), muse_msp.serve_argv())
        finally:
            if saved is None:
                os.environ.pop("M8S_SERVE_ARGV", None)
            else:
                os.environ["M8S_SERVE_ARGV"] = saved

    def test_blank_override_falls_back_to_default(self) -> None:
        saved = os.environ.get("M8S_SERVE_ARGV")
        os.environ["M8S_SERVE_ARGV"] = "   "
        try:
            self.assertEqual(muse_msp.serve_argv(), list(muse_msp.SERVE_ARGV))
        finally:
            if saved is None:
                os.environ.pop("M8S_SERVE_ARGV", None)
            else:
                os.environ["M8S_SERVE_ARGV"] = saved

    def test_health_advertises_spawn_argv(self) -> None:
        class FakeHost(muse_msp.MspHost):
            def __init__(self) -> None:
                pass

            async def health(self, events_limit: int = 2000) -> Any:
                return {"members": []}

        async def go() -> Any:
            return await muse_msp.dispatch(FakeHost(), {"command": "health"})

        result = asyncio.run(go())
        self.assertEqual(result["serveArgv"], muse_msp.serve_argv())
        self.assertIn("members", result)


if __name__ == "__main__":
    unittest.main()
