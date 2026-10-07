# SPDX-License-Identifier: AGPL-3.0-or-later
"""Command line front end for the m8s MCP adapter."""

from __future__ import annotations

import argparse

from . import __version__
from .adapter import Adapter
from .server import serve_stdio


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="m8s-mcp",
        description="MCP front end for m8s (see docs/mcp.md).",
    )
    parser.add_argument("--version", action="version", version=f"m8s-mcp {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="run the daemon-backed MCP adapter over stdio")
    serve.add_argument("--socket", default=None, help="daemon control socket path")
    serve.add_argument(
        "--state-dir", default=None, help="adapter task/worktree state directory"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "serve":
        serve_stdio(Adapter(socket_path=args.socket, state_dir=args.state_dir))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
