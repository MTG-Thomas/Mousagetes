# SPDX-License-Identifier: AGPL-3.0-or-later
"""Command line front end for the m8s ACP adapter."""

from __future__ import annotations

import argparse
import inspect
from typing import Any

from . import __version__
from .agent import serve_stdio as serve_agent_stdio
from .stub import serve_stdio as serve_stub_stdio


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="m8s-acp",
        description="ACP front end for m8s (see docs/acp.md).",
    )
    parser.add_argument(
        "--version", action="version", version=f"m8s-acp {__version__}"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser(
        "serve", help="run the real daemon-backed adapter over stdio"
    )
    serve.add_argument("--socket", default=None, help="daemon control socket path")
    serve.add_argument(
        "--workspace-root", default=None, help="workspace root for new lanes"
    )
    serve.add_argument(
        "--token-file", default=None, help="bearer token file (remote transport)"
    )

    sub.add_parser(
        "stub",
        help="run the transport test stub over stdio (not the real adapter)",
    )
    return parser


def _load_mapping(args: argparse.Namespace) -> Any:
    try:
        from .mapping import DaemonMapping
    except ImportError as exc:
        raise SystemExit(
            "m8s-acp serve: the daemon mapping (m8s_acp.mapping) is not "
            "available yet; the mapping workstream has not landed. Run "
            "'m8s-acp stub' to exercise the transport with the test peer."
        ) from exc
    return _construct_mapping(DaemonMapping, args)


def _construct_mapping(cls: type, args: argparse.Namespace) -> Any:
    candidates = {
        "socket_path": getattr(args, "socket", None),
        "socket": getattr(args, "socket", None),
        "workspace_root": getattr(args, "workspace_root", None),
        "token_file": getattr(args, "token_file", None),
    }
    try:
        parameters = inspect.signature(cls).parameters
    except (TypeError, ValueError):
        parameters = {}
    kwargs = {
        name: candidates[name]
        for name in parameters
        if name in candidates and candidates[name] is not None
    }
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise SystemExit(
            f"m8s-acp serve: could not construct DaemonMapping: {exc}"
        ) from exc


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "stub":
        serve_stub_stdio()
        return 0
    if args.command == "serve":
        serve_agent_stdio(_load_mapping(args))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
