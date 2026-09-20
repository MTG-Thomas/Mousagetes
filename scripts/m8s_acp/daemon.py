# SPDX-License-Identifier: AGPL-3.0-or-later
"""Minimal JSON-lines client for the m8s daemon control socket.

The ACP adapter reaches daemon state one way and only one way: it opens the
control Unix socket, writes a single JSON request line, and reads a single
JSON response line (ADR 0002). This module owns that tiny piece of wire
code. It holds no state between calls, so a daemon bounce simply surfaces
as :class:`DaemonError` on the next call and the caller can retry.

The framing mirrors ``scripts/muse-msp.py``'s daemon-side ``client()``: a
request is one JSON object (``{"command": ...}`` or
``{"command": "call", "method": ..., "session": ..., "params": {...}}``)
followed by ``\\n``; the response is one JSON object
``{"ok": true, "result": ...}`` or ``{"ok": false, "error": ...,
"errorKind": ...}``. Stdlib only.
"""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from typing import Any, Callable

DEFAULT_TIMEOUT = 30.0

# Session lists and materialized views can exceed asyncio's 64 KiB default,
# so size the framed transport the same way the daemon does.
MAX_LINE_BYTES = 16 * 1024 * 1024


class DaemonError(RuntimeError):
    """A control request failed or the socket could not be reached."""

    def __init__(self, message: str, *, kind: str | None = None) -> None:
        super().__init__(message)
        self.kind = kind


def default_socket_path() -> Path:
    """Resolve the daemon control socket the way ``muse-msp.py`` does."""
    base = os.environ.get("XDG_RUNTIME_DIR")
    if base:
        return Path(base) / "muse-msp-supervisor" / "control.sock"
    return Path(f"/tmp/muse-msp-{os.getuid()}") / "control.sock"


def _unix_connector(path: Path, timeout: float) -> Callable[[], socket.socket]:
    def connect() -> socket.socket:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(str(path))
        return sock

    return connect


class ControlClient:
    """Connect, send one request, read one response, then close.

    ``connector`` is injectable so tests can hand in an in-memory socketpair
    instead of touching the filesystem. The default connector opens the real
    Unix control socket at ``socket_path``.
    """

    def __init__(
        self,
        socket_path: str | os.PathLike[str] | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        connector: Callable[[], socket.socket] | None = None,
    ) -> None:
        self.socket_path = Path(socket_path) if socket_path else default_socket_path()
        self.timeout = timeout
        self._connector = connector or _unix_connector(self.socket_path, timeout)

    def request(self, request: dict[str, Any]) -> Any:
        """Send one control request and return its ``result``.

        Raises :class:`DaemonError` if the socket is unreachable, the
        response is malformed, or the daemon replies ``ok: false``.
        """
        line = json.dumps(request, separators=(",", ":")) + "\n"
        try:
            sock = self._connector()
        except OSError as exc:
            raise DaemonError(
                f"cannot reach daemon at {self.socket_path}: {exc}"
            ) from exc
        try:
            sock.sendall(line.encode("utf-8"))
            raw = _read_line(sock)
        except OSError as exc:
            raise DaemonError(f"daemon control socket failed: {exc}") from exc
        finally:
            sock.close()
        try:
            response = json.loads(raw)
        except ValueError as exc:
            raise DaemonError(f"malformed daemon response: {exc}") from exc
        if not isinstance(response, dict):
            raise DaemonError("malformed daemon response: not an object")
        if not response.get("ok", False):
            raise DaemonError(
                str(response.get("error", "daemon error")),
                kind=response.get("errorKind"),
            )
        return response.get("result")


def _read_line(sock: socket.socket) -> bytes:
    """Read one newline-terminated line (or EOF) from the control socket."""
    data = bytearray()
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        data.extend(chunk)
        newline = data.find(b"\n")
        if newline != -1:
            return bytes(data[:newline])
        if len(data) > MAX_LINE_BYTES:
            raise DaemonError("daemon response line too large")
    if not data:
        raise DaemonError("daemon closed the control socket without a response")
    return bytes(data)
