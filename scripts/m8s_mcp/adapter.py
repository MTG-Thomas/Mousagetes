# SPDX-License-Identifier: AGPL-3.0-or-later
"""Codex-facing task operations backed only by the m8s daemon control API.

Authority boundary (ADR 0002): every lane operation in this module is one
``ControlClient.request`` against the daemon Unix socket. This module never
spawns ``muse serve``, never touches MSP framing, and never cancels,
retires, or resumes a lane except through the daemon verbs documented in
``docs/mcp.md``. Disconnecting the stdio client performs no daemon call at
all, so running turns survive adapter exit by construction.

Safety contracts preserved from the installed bridge surface:

- ``requestId`` UUID deduplication (identical inputs replay, changed inputs
  reject);
- ``worktree`` creation from the exact verified committed ref with
  ``git worktree add --detach`` (dirty source files are never copied, the
  source checkout is never modified, nothing is committed);
- terminal evidence bound to the admitted turn id (a turn/completed for any
  other turn never settles the task);
- read-only history paths (``sessions``, ``session_read``) never acquire a
  writer lease (no ``session/resume``, ``send``, or ``adopt`` on those
  paths);
- ``resume``/``approve``/``cancel``/``changes`` only for adapter-owned
  tasks (foreign lanes reject);
- cancellation carries the admitted turn id and is confirmed through
  ``status``, never asserted synchronously;
- approvals answer only the exact offered ``approvalId`` /
  current requirement id / offered ``choiceId`` triple, checked against a
  fresh ``pending`` read (stale or un-offered answers never reach the
  wire, refused answers stay answerable);
- YOLO (``allowAll`` in a worktree) requires the explicit
  ``mode="worktree"`` selection; the default is the approval-gated
  ``read_only`` posture.

Read-only enforcement: ``--disable-write`` and ``--disable-shell`` are
``muse serve`` argv, fixed per host for its lifetime, and the shared
daemon serves trusted (``--trust-workspace --disable-sandbox``). They are
not negotiable over the wire, so ``read_only`` starts fail closed with
``unsupportedReadOnly`` unless a dedicated read-only daemon socket is
supplied (``--read-only-socket``). That socket's advertised posture is
verified on every use (both flags must appear in its ``health``
``serveArgv``), else ``readOnlyMisconfigured``. There is no
approval-gated fallback: the adapter never silently downgrades.
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

from m8s_acp.daemon import ControlClient, DaemonError

# Tool names. Twelve, matching the installed bridge surface.
TOOLS = (
    "health",
    "models",
    "sessions",
    "session_read",
    "start",
    "tasks",
    "status",
    "result",
    "resume",
    "approve",
    "cancel",
    "changes",
)

READ_ONLY_TOOLS = frozenset(
    {"health", "models", "sessions", "session_read", "tasks", "status", "result", "changes"}
)

MODES = ("read_only", "worktree")

# Approval posture per start mode. read_only stays ask-on-request even on
# the enforced host (defense in depth); worktree is the explicit YOLO
# selection (allowAll inside the isolated worktree).
APPROVAL_MODE = {"read_only": "onRequest", "worktree": "allowAll"}

# Flags a read-only daemon must advertise in its health serveArgv.
RO_REQUIRED_FLAGS = ("--disable-write", "--disable-shell")

# At most four live tasks, matching the installed bridge.
MAX_ACTIVE_TASKS = 4

# Bounded previews: status/result/changes never dump unbounded transcripts.
PREVIEW_CHARS = 4000
DIFF_CHARS = 20000
EVENTS_SCAN = 500

ACTIVE_STATES = frozenset({"starting", "running", "awaiting_approval"})


class McpAdapterError(RuntimeError):
    """A typed adapter failure. ``kind`` is the machine-readable error."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def _error(kind: str, message: str) -> McpAdapterError:
    return McpAdapterError(kind, f"{kind}: {message}")


def _parse_uuid(value: Any, field: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, AttributeError, TypeError):
        raise _error("badUuid", f"{field} must be a UUID") from None


def _require_str(args: dict[str, Any], field: str, *, minimum: int = 1, maximum: int = 100000) -> str:
    value = args.get(field)
    if not isinstance(value, str) or not (minimum <= len(value) <= maximum):
        raise _error("badArgument", f"{field} must be a string of length {minimum}..{maximum}")
    return value


def _optional_str(args: dict[str, Any], field: str, *, maximum: int = 100000) -> str | None:
    value = args.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > maximum:
        raise _error("badArgument", f"{field} must be a string of at most {maximum} chars")
    return value


def _check_no_unknown(args: dict[str, Any], allowed: frozenset[str], tool: str) -> None:
    unknown = sorted(set(args) - allowed)
    if unknown:
        raise _error("badArgument", f"{tool}: unknown argument(s): {', '.join(unknown)}")


def _run_git(workdir: Path, *argv: str) -> str:
    """Run one git command with no shell. Raises McpAdapterError on failure."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(workdir), *argv],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise _error("gitFailed", f"git {' '.join(argv)} failed: {exc}") from exc
    if proc.returncode != 0:
        raise _error("gitFailed", f"git {' '.join(argv)}: {(proc.stderr or proc.stdout).strip()[:300]}")
    return proc.stdout.strip()


def _verify_workspace(workspace: str) -> Path:
    path = Path(workspace)
    if not path.is_absolute():
        raise _error("badWorkspace", "workspace must be an absolute path")
    if not path.is_dir():
        raise _error("badWorkspace", f"workspace does not exist: {workspace}")
    _run_git(path, "rev-parse", "--git-dir")
    return path


def _resolve_ref(workspace: Path, ref: str) -> str:
    """Resolve ``ref`` to the exact committed SHA. Rejects unknown refs."""
    if not ref or len(ref) > 256:
        raise _error("badRef", "ref must be a non-empty revision (<=256 chars)")
    sha = _run_git(workspace, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if not sha:
        raise _error("badRef", f"unknown ref: {ref}")
    return sha


class TaskStore:
    """File-backed adapter-owned task registry.

    One JSON document per task under ``state_dir/tasks``. Atomic writes
    (tmp file + rename), ``0700`` directories. Lanes keep running in the
    daemon regardless of this registry; on load, tasks left non-terminal
    by a previous adapter process are marked ``interrupted`` (never
    auto-replayed).
    """

    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.tasks_dir = state_dir / "tasks"
        self.worktrees_dir = state_dir / "worktrees"
        self.tasks_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.worktrees_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    def _path(self, task_id: str) -> Path:
        return self.tasks_dir / f"{task_id}.json"

    def load(self, task_id: str) -> dict[str, Any]:
        try:
            raw = self._path(task_id).read_text(encoding="utf-8")
        except OSError:
            raise _error("unknownTask", "unknown taskId (not adapter-owned)") from None
        try:
            record = json.loads(raw)
        except ValueError:
            raise _error("unknownTask", "unknown taskId (not adapter-owned)") from None
        if not isinstance(record, dict) or record.get("taskId") != task_id:
            raise _error("unknownTask", "unknown taskId (not adapter-owned)") from None
        return record

    def save(self, record: dict[str, Any]) -> None:
        path = self._path(str(record["taskId"]))
        data = json.dumps(record, indent=2, sort_keys=True)
        fd, tmp = tempfile.mkstemp(dir=str(self.tasks_dir), prefix=".tmp-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(data)
            os.replace(tmp, path)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def all(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in sorted(self.tasks_dir.glob("*.json")):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if isinstance(record, dict) and record.get("taskId"):
                records.append(record)
        return records

    def find_by_request(self, request_id: str) -> dict[str, Any] | None:
        for record in self.all():
            if record.get("requestId") == request_id:
                return record
        return None

    def active_count(self) -> int:
        return sum(1 for record in self.all() if record.get("status") in ACTIVE_STATES)

    @contextlib.contextmanager
    def locked(self) -> Iterator[None]:
        """Process- and thread-safe exclusive lock for start's check-then-act.

        Serializes requestId dedup, the active cap, worktree creation,
        launch, and the first save, so concurrent starts cannot produce
        two lanes for one request. Held across the daemon round-trip:
        starts are rare, correctness beats parallelism here.
        """
        self.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        import fcntl

        with open(self.state_dir / "lock", "a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _truncate(text: str, limit: int) -> tuple[str, bool]:
    if len(text) <= limit:
        return text, False
    return text[:limit], True


def _read_text(read: Any) -> str:
    """Best-effort assistant text from a session/read payload (preview only)."""
    parts: list[str] = []
    if isinstance(read, dict):
        messages = read.get("messages") or read.get("transcript") or []
        if isinstance(messages, list):
            for message in messages[-20:]:
                if not isinstance(message, dict):
                    continue
                role = message.get("role") or ""
                content = message.get("content") or message.get("text") or ""
                if isinstance(content, list):
                    content = " ".join(
                        block.get("text", "") for block in content if isinstance(block, dict)
                    )
                if isinstance(content, str) and content:
                    parts.append(f"{role}: {content}"[:2000])
        elif isinstance(read.get("text"), str):
            parts.append(read["text"])
    return "\n\n".join(parts)


class Adapter:
    """The twelve Codex tools. Daemon sockets are the only lane channel."""

    def __init__(
        self,
        client: ControlClient | None = None,
        *,
        socket_path: str | None = None,
        read_only_socket_path: str | None = None,
        state_dir: str | Path | None = None,
    ) -> None:
        self._rw_client = client if client is not None else ControlClient(socket_path)
        self._ro_socket = read_only_socket_path
        self._ro_client = ControlClient(read_only_socket_path) if read_only_socket_path else None
        if state_dir is None:
            state_dir = Path.home() / ".local" / "state" / "m8s-mcp"
        self._store = TaskStore(Path(state_dir))
        self._reconcile()

    # -- daemon plumbing -------------------------------------------------

    def _client_for(self, record: dict[str, Any] | None) -> ControlClient:
        """The owning daemon client for a task (read-write by default).

        Read-only tasks require the dedicated enforced socket; without it
        the call fails closed instead of reaching the trusted host.
        """
        if record is not None and record.get("daemon", "rw") == "ro":
            if self._ro_client is None:
                raise _error(
                    "unsupportedReadOnly",
                    "read_only task needs --read-only-socket (dedicated "
                    "disable-write/disable-shell daemon); refusing the trusted host",
                )
            return self._ro_client
        return self._rw_client

    def _daemon(self, request: dict[str, Any], record: dict[str, Any] | None = None) -> Any:
        try:
            return self._client_for(record).request(request)
        except DaemonError as exc:
            raise _error("daemonDown" if exc.kind is None else str(exc.kind), str(exc)) from exc

    def _call(
        self,
        method: str,
        session: str | None = None,
        params: dict[str, Any] | None = None,
        record: dict[str, Any] | None = None,
    ) -> Any:
        request: dict[str, Any] = {"command": "call", "method": method, "commandId": "auto"}
        if session:
            request["session"] = session
        if params:
            request["params"] = params
        return self._daemon(request, record)

    def _lane(self, session_id: str, record: dict[str, Any] | None = None) -> dict[str, Any] | None:
        result = self._daemon({"command": "list"}, record) or {}
        for item in result.get("sessions", []) or []:
            if (item.get("sessionId") or item.get("id")) == session_id:
                return item
        return None

    def _lanes(self, record: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        result = self._daemon({"command": "list"}, record) or {}
        return [item for item in result.get("sessions", []) or [] if isinstance(item, dict)]

    def _pending(self, session_id: str, record: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._daemon({"command": "pending", "session": session_id}, record) or {}

    def _verify_read_only(self) -> list[str]:
        """Fail closed unless the read-only socket enforces tool removal.

        Returns the advertised serve argv. Checked on every read_only
        start and reported by health: posture is verified, never assumed.
        """
        if self._ro_client is None:
            raise _error(
                "unsupportedReadOnly",
                "read_only needs --read-only-socket (dedicated "
                "disable-write/disable-shell daemon); refusing the trusted host",
            )
        try:
            health = self._ro_client.request({"command": "health", "eventsLimit": 1}) or {}
        except DaemonError as exc:
            raise _error("daemonDown", f"read-only daemon unreachable: {exc}") from exc
        argv = health.get("serveArgv") if isinstance(health, dict) else None
        if not isinstance(argv, list) or any(flag not in argv for flag in RO_REQUIRED_FLAGS):
            raise _error(
                "readOnlyMisconfigured",
                "read-only daemon does not advertise disable-write+disable-shell; refusing start",
            )
        return [str(flag) for flag in argv]

    def _enforcement(self, record: dict[str, Any]) -> str:
        if record.get("daemon", "rw") == "ro":
            return "enforced-read-only"
        if record.get("mode") == "read_only":
            # Pre-enforcement record: launched approval-gated on the
            # trusted host before fail-closed existed. Visible, not hidden.
            return "approval-gated-legacy"
        return "yolo-worktree"

    def _reconcile(self) -> None:
        """Settle persisted pointers against daemon evidence, not client death.

        A new adapter process (e.g. after a Codex stdio reconnect) must
        not mistake its own restart for host failure: a task whose
        admitted turn has a terminal event settles to it; a task whose
        lane is still supervised stays live; only a lane that is gone
        with no terminal becomes interrupted (never auto-replayed).
        """
        for record in self._store.all():
            if record.get("status") not in ACTIVE_STATES:
                continue
            try:
                client_record: dict[str, Any] | None = record
                self._client_for(record)
            except McpAdapterError:
                record["status"] = "interrupted"
                record["updatedAt"] = _now()
                record["interruptedReason"] = "read-only socket unconfigured at reconcile"
                self._store.save(record)
                continue
            try:
                terminal = self._terminal_for(
                    str(record["sessionId"]), record.get("turnId"), client_record
                )
            except McpAdapterError:
                continue  # daemon unreachable: leave pointers untouched
            if terminal is not None:
                self._settle(record, terminal)
                continue
            try:
                lane = self._lane(str(record["sessionId"]), client_record)
            except McpAdapterError:
                continue
            if lane is None:
                record["status"] = "interrupted"
                record["updatedAt"] = _now()
                record["interruptedReason"] = "lane gone with no terminal for the admitted turn"
                self._store.save(record)
            elif not record.get("recovered"):
                record["recovered"] = True
                record["updatedAt"] = _now()
                self._store.save(record)

    def _settle(self, record: dict[str, Any], terminal: dict[str, Any]) -> None:
        kind = terminal.get("terminal") or "completed"
        record["status"] = "completed" if kind == "completed" else kind
        record["terminal"] = terminal
        record["finishedAt"] = _now()
        record["updatedAt"] = _now()
        self._store.save(record)

    def _terminal_for(
        self, session_id: str, turn_id: str | None, record: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """The turn/completed record for the admitted turn, else None.

        Scans daemon events newest-first; a terminal for any other turn —
        earlier briefs, foreign turns, stale retries — never settles this
        task. No terminal event means the turn is still live.
        """
        if not turn_id:
            return None
        result = self._daemon({"command": "events", "after": 0, "limit": EVENTS_SCAN}, record) or {}
        for event in reversed(result.get("events", []) or []):
            if not isinstance(event, dict):
                continue
            if event.get("kind") == "msp.event" and event.get("method") == "turn/completed":
                params = event.get("params") or {}
            elif event.get("kind") == "turn/completed":
                params = event
            else:
                continue
            if params.get("sessionId") != session_id or str(params.get("turnId")) != str(turn_id):
                continue
            return params
        return None

    def _transcript_tail(
        self, session_id: str, record: dict[str, Any] | None = None
    ) -> tuple[str, bool]:
        try:
            read = self._call("session/read", session_id, None, record) or {}
        except McpAdapterError:
            return "", False
        text = _read_text(read)
        return _truncate(text, PREVIEW_CHARS)

    # -- read tools --------------------------------------------------------

    def health(self) -> dict[str, Any]:
        daemon = self._daemon({"command": "health", "eventsLimit": 50}) or {}
        owned = self._store.all()
        read_only: dict[str, Any] = {
            "configured": self._ro_client is not None,
            "socket": self._ro_socket,
            "enforced": None,
            "serveArgv": None,
        }
        if self._ro_client is not None:
            try:
                argv = self._verify_read_only()
            except McpAdapterError as exc:
                read_only["enforced"] = False
                read_only["error"] = str(exc)
            else:
                read_only["enforced"] = True
                read_only["serveArgv"] = argv
        return {
            "adapter": "m8s-mcp",
            "daemon": {key: daemon.get(key) for key in ("ok", "sessions", "hosts") if key in daemon}
            or daemon,
            "taskCount": len(owned),
            "activeTasks": sum(1 for task in owned if task.get("status") in ACTIVE_STATES),
            "modes": list(MODES),
            "readOnly": read_only,
        }

    def models(self) -> dict[str, Any]:
        return {"models": self._call("model/list") or []}

    def _roster(self, owner: dict[str, Any] | None) -> list[dict[str, Any]]:
        result = self._daemon({"command": "list"}, owner) or {}
        return [item for item in result.get("sessions", []) or [] if isinstance(item, dict)]

    def sessions(self, workspace: str | None = None, limit: int = 20, cursor: str | None = None) -> dict[str, Any]:
        """Lane metadata across both hosts. Never resumes, sends, or adopts: no lease."""
        if limit < 1 or limit > 100:
            raise _error("badArgument", "limit must be 1..100")
        owners: list[tuple[dict[str, Any] | None, str]] = [(None, "rw")]
        if self._ro_client is not None:
            owners.append(({"daemon": "ro"}, "ro"))
        items = []
        for owner, tag in owners:
            try:
                lanes = self._roster(owner)
            except McpAdapterError as exc:
                if tag == "ro":
                    items.append({"daemon": tag, "error": str(exc)})
                    continue
                raise
            for item in lanes:
                if workspace and item.get("workspace") != workspace:
                    continue
                items.append(
                    {
                        "sessionId": item.get("sessionId") or item.get("id"),
                        "daemon": tag,
                        "alias": item.get("alias") or item.get("name"),
                        "workspace": item.get("workspace"),
                        "status": item.get("status"),
                        "modelId": item.get("modelId"),
                        "lastActivity": item.get("lastActivity"),
                    }
                )
        if cursor:
            try:
                offset = int(cursor)
            except ValueError:
                raise _error("badArgument", "cursor must be an integer offset") from None
            items = items[offset:]
        page = items[:limit]
        out: dict[str, Any] = {"sessions": page}
        if len(items) > limit:
            out["nextCursor"] = str((int(cursor) if cursor else 0) + limit)
        return out

    def session_read(self, session_id: str, include_items: bool = False) -> dict[str, Any]:
        """Point-in-time snapshot from either host. Never resumes or takes a lease."""
        if not isinstance(session_id, str) or not session_id:
            raise _error("badArgument", "sessionId must be a non-empty string")
        try:
            return self._session_read_on(None, session_id, include_items, "rw")
        except McpAdapterError as rw_exc:
            if self._ro_client is None:
                raise
            try:
                return self._session_read_on({"daemon": "ro"}, session_id, include_items, "ro")
            except McpAdapterError:
                raise rw_exc from None

    def _session_read_on(
        self, owner: dict[str, Any] | None, session_id: str, include_items: bool, tag: str
    ) -> dict[str, Any]:
        read = self._call("session/read", session_id, None, owner) or {}
        out: dict[str, Any] = {"sessionId": session_id, "daemon": tag, "snapshot": read}
        if include_items:
            page = self._call("view/page", session_id, {"limit": 50}, owner) or {}
            out["items"] = page.get("events", [])
        return out

    def tasks(self) -> dict[str, Any]:
        return {
            "tasks": [
                {
                    "taskId": task.get("taskId"),
                    "status": task.get("status"),
                    "mode": task.get("mode"),
                    "sessionId": task.get("sessionId"),
                    "workspace": task.get("workspace"),
                    "updatedAt": task.get("updatedAt"),
                }
                for task in self._store.all()
            ]
        }

    # -- start -------------------------------------------------------------

    def start(
        self,
        workspace: str,
        prompt: str,
        mode: str = "read_only",
        model: str | None = None,
        ref: str = "HEAD",
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Submit an asynchronous lane task. Returns the task id.

        ``mode="worktree"`` is the explicit YOLO selection: it launches
        with ``allowAll`` inside a fresh detached worktree on the trusted
        host. The default ``read_only`` launches on the dedicated enforced
        host only, and fails closed (``unsupportedReadOnly`` /
        ``readOnlyMisconfigured``) otherwise -- never approval-gated on
        the trusted host. Repeating a ``requestId`` with identical inputs
        replays the existing task; changed inputs reject. Always supply a
        ``requestId``: it is what makes an uncertain retry safe.
        """
        if not isinstance(workspace, str) or not workspace:
            raise _error("badArgument", "workspace must be a non-empty string")
        if not isinstance(prompt, str) or not (1 <= len(prompt) <= 100000):
            raise _error("badArgument", "prompt must be 1..100000 chars")
        if mode not in MODES:
            raise _error("badArgument", f"mode must be one of {', '.join(MODES)}")
        if model is not None and (not isinstance(model, str) or not model):
            raise _error("badArgument", "model must be a non-empty string")
        if not isinstance(ref, str) or not ref:
            raise _error("badArgument", "ref must be a non-empty string")
        request_uuid = _parse_uuid(request_id, "requestId") if request_id is not None else None
        daemon = "ro" if mode == "read_only" else "rw"
        if daemon == "ro":
            # Fail closed before any lane, worktree, or record exists.
            self._verify_read_only()

        source = _verify_workspace(workspace)
        commit = _resolve_ref(source, ref)

        with self._store.locked():
            return self._start_locked(
                source, prompt, mode, model, ref, commit, daemon, request_uuid
            )

    def _start_locked(
        self,
        source: Path,
        prompt: str,
        mode: str,
        model: str | None,
        ref: str,
        commit: str,
        daemon: str,
        request_uuid: str | None,
    ) -> dict[str, Any]:
        store = self._store
        fingerprint = _fingerprint(str(source), prompt, mode, model, commit)
        if request_uuid is not None:
            existing = store.find_by_request(request_uuid)
            if existing is not None:
                if existing.get("fingerprint") != fingerprint:
                    raise _error(
                        "duplicateRequestId",
                        "requestId was already used with different inputs",
                    )
                return {"taskId": existing["taskId"], "duplicate": True}
        else:
            request_uuid = str(uuid.uuid4())

        if store.active_count() >= MAX_ACTIVE_TASKS:
            raise _error("tooManyActive", f"at most {MAX_ACTIVE_TASKS} tasks may be active")

        task_id = str(uuid.uuid4())
        lane_workspace = str(source)
        worktree: str | None = None
        if mode == "worktree":
            worktree = str(store.worktrees_dir / task_id)
            # Detached worktree at the exact committed SHA: dirty source
            # files are never copied, the source checkout stays untouched.
            _run_git(source, "worktree", "add", "--detach", worktree, commit)
            lane_workspace = worktree

        # The alias derives from the requestId when one is supplied, so an
        # uncertain retry (lost receipt, lane already created) finds and
        # adopts its orphan instead of launching a second lane.
        alias = f"mcp-{uuid.UUID(request_uuid).hex[:8]}"
        launch: dict[str, Any] = {
            "command": "launch",
            "name": alias,
            "workspace": lane_workspace,
            "prompt": prompt,
            "approvalMode": APPROVAL_MODE[mode],
        }
        if model:
            launch["model"] = model
        owner: dict[str, Any] | None = {"daemon": daemon}
        try:
            launched = self._daemon(launch, owner) or {}
        except McpAdapterError:
            adopted = self._adopt_orphan(owner, alias)
            if adopted is None:
                if worktree is not None:
                    _remove_worktree(source, worktree)
                raise
            launched = adopted
        session = launched.get("session") or {}
        session_id = session.get("sessionId") or session.get("id")
        if not session_id:
            if worktree is not None:
                _remove_worktree(source, worktree)
            raise _error("launchFailed", "daemon launch returned no session id")
        turn = launched.get("turn") or {}
        turn_id = turn.get("turnId")
        if not turn_id and isinstance(launched.get("session"), dict):
            turn_id = (session.get("activeTurn") or session.get("activeTurnId"))
        record = {
            "taskId": task_id,
            "requestId": request_uuid,
            "fingerprint": fingerprint,
            "daemon": daemon,
            "sessionId": str(session_id),
            "alias": alias,
            "mode": mode,
            "model": model,
            "ref": ref,
            "commit": commit,
            "sourceWorkspace": str(source),
            "workspace": lane_workspace,
            "worktree": worktree,
            "prompt": prompt,
            "turnId": str(turn_id) if turn_id else None,
            "status": "running",
            "cancelRequested": False,
            "createdAt": _now(),
            "updatedAt": _now(),
        }
        store.save(record)
        return {"taskId": task_id, "sessionId": str(session_id)}

    def _adopt_orphan(
        self, owner: dict[str, Any], alias: str
    ) -> dict[str, Any] | None:
        """Bind the lane an uncertain launch left behind, if any.

        When a launch receipt is lost after the daemon created the lane,
        the lane sits under our deterministic alias. Adopt it (session
        plus live turn) instead of launching a duplicate. Returns None
        when no such lane exists or the daemon cannot even be listed, in
        which case the original error propagates.
        """
        try:
            lanes = self._lanes(owner)
        except McpAdapterError:
            return None
        for lane in lanes:
            if lane.get("alias") == alias:
                session_id = lane.get("sessionId") or lane.get("id")
                if not session_id:
                    continue
                return {
                    "session": lane,
                    "turn": {"turnId": lane.get("activeTurnId")},
                    "adoptedOrphan": True,
                }
        return None

    # -- progress and terminal evidence -------------------------------------

    def _owned(self, task_id: str) -> dict[str, Any]:
        _parse_uuid(task_id, "taskId")
        return self._store.load(task_id)

    def _progress(self, record: dict[str, Any]) -> dict[str, Any]:
        session_id = str(record["sessionId"])
        lane = self._lane(session_id, record)
        pending: dict[str, Any] = {}
        if lane is not None:
            try:
                pending = self._pending(session_id, record)
            except McpAdapterError:
                pending = {}
        approvals = pending.get("approvals", []) if isinstance(pending, dict) else []
        preview, truncated = (
            self._transcript_tail(session_id, record) if lane is not None else ("", False)
        )
        status = record.get("status")
        if status in ACTIVE_STATES and lane is not None:
            status = "awaiting_approval" if approvals else "running"
        return {
            "taskId": record["taskId"],
            "status": status,
            "mode": record.get("mode"),
            "enforcement": self._enforcement(record),
            "sessionId": session_id,
            "turnId": record.get("turnId"),
            "lanePresent": lane is not None,
            "laneStatus": (lane or {}).get("status"),
            "approvals": approvals,
            "preview": preview,
            "previewTruncated": truncated,
            "cancelRequested": bool(record.get("cancelRequested")),
            "updatedAt": record.get("updatedAt"),
        }

    def status(self, task_id: str) -> dict[str, Any]:
        """Progress, live preview, and exact pending approvals. Bounded."""
        return self._progress(self._owned(task_id))

    def result(self, task_id: str) -> dict[str, Any]:
        """Terminal evidence bound to the admitted turn id.

        ``completed`` is success; ``failed``, ``cancelled``, and
        ``interrupted`` are not. With no terminal event for the admitted
        turn the task is still live and ``result`` says so instead of
        inventing success.
        """
        record = self._owned(task_id)
        if record.get("status") == "interrupted":
            return {
                "taskId": task_id,
                "status": "interrupted",
                "terminal": None,
                "note": "adapter restarted; never auto-replayed; use resume explicitly",
            }
        terminal = self._terminal_for(str(record["sessionId"]), record.get("turnId"), record)
        if terminal is None:
            progress = self._progress(record)
            return {
                "taskId": task_id,
                "status": progress["status"],
                "terminal": None,
                "note": "turn still live; a returned answer or accepted cancel is not terminal proof",
                "approvals": progress["approvals"],
            }
        self._settle(record, terminal)
        preview, truncated = self._transcript_tail(str(record["sessionId"]), record)
        status = record["status"]
        return {
            "taskId": task_id,
            "status": status,
            "terminal": terminal,
            "evidence": preview,
            "evidenceTruncated": truncated,
        }

    def resume(self, task_id: str, prompt: str) -> dict[str, Any]:
        """Explicit follow-up in the adapter-owned idle lane. Never automatic."""
        record = self._owned(task_id)
        prompt = _require_str({"prompt": prompt}, "prompt")
        session_id = str(record["sessionId"])
        if self._lane(session_id, record) is None:
            raise _error("laneGone", "lane is no longer supervised by the daemon")
        if (
            self._terminal_for(session_id, record.get("turnId"), record) is None
            and record.get("status") in ACTIVE_STATES
        ):
            lane = self._lane(session_id, record) or {}
            if lane.get("activeTurnId"):
                raise _error("turnBusy", "lane still has an active turn; wait or cancel first")
        sent = self._daemon({"command": "send", "session": session_id, "prompt": prompt}, record) or {}
        turn_id = sent.get("turnId")
        if turn_id:
            record["turnId"] = str(turn_id)
        record["status"] = "running"
        record["cancelRequested"] = False
        record.pop("terminal", None)
        record.pop("finishedAt", None)
        record["updatedAt"] = _now()
        self._store.save(record)
        return {"taskId": task_id, "turnId": record.get("turnId")}

    # -- approvals, cancellation, worktree diff ------------------------------

    def approve(
        self,
        task_id: str,
        approval_id: str,
        requirement_id: Any,
        choice_id: str,
    ) -> dict[str, Any]:
        """Answer the exact offered approval triple. No persistent grants.

        The triple is checked against a fresh ``pending`` read: a stale
        ``approvalId`` (``approvalNotFound``) or an un-offered ``choiceId``
        (``invalidChoice``) never reaches the wire, and a refused answer is
        never marked settled, so a later valid choice can still go through.
        The adapter only relays the choice Codex was authorized to send.
        """
        record = self._owned(task_id)
        session_id = str(record["sessionId"])
        for field, value in (("approvalId", approval_id), ("choiceId", choice_id)):
            if not isinstance(value, str) or not value:
                raise _error("badArgument", f"{field} must be a non-empty string")
        pending = self._pending(session_id, record)
        approvals = pending.get("approvals", []) if isinstance(pending, dict) else []
        offered = next(
            (item for item in approvals if isinstance(item, dict) and item.get("approvalId") == approval_id),
            None,
        )
        if offered is None:
            raise _error("approvalNotFound", "approval is no longer pending; read status again")
        current = offered.get("currentRequirementId", offered.get("requirementId"))
        if current != requirement_id:
            raise _error("requirementChanged", "approval requirement changed; read status again")
        choices = offered.get("availableChoices", [])
        if not any(
            isinstance(choice, dict) and choice.get("choiceId") == choice_id for choice in choices
        ):
            raise _error("invalidChoice", "choice was not offered; read status again")
        decided = self._call(
            "approval/decide",
            session_id,
            {"approvalId": approval_id, "requirementId": current, "choiceId": choice_id},
            record,
        )
        return {"taskId": task_id, "approvalId": approval_id, "decided": decided}

    def cancel(self, task_id: str) -> dict[str, Any]:
        """Cancel the adapter-owned task's admitted turn.

        Only the owned lane's turn is cancelled (foreign lanes reject via
        ``unknownTask``). Confirmation arrives through ``status``/``result``
        once the admitted turn's terminal event binds; this reply only
        records that cancellation was requested.
        """
        record = self._owned(task_id)
        session_id = str(record["sessionId"])
        if self._lane(session_id, record) is None:
            raise _error("laneGone", "lane is no longer supervised by the daemon")
        self._call("turn/cancel", session_id, None, record)
        record["cancelRequested"] = True
        record["updatedAt"] = _now()
        self._store.save(record)
        return {"taskId": task_id, "cancelRequested": True, "turnId": record.get("turnId")}

    def changes(self, task_id: str) -> dict[str, Any]:
        """Git status and bounded diff. Never commits, merges, or applies."""
        record = self._owned(task_id)
        target = Path(str(record["workspace"]))
        if not target.is_dir():
            raise _error("laneGone", "task workspace is gone")
        status_out = _run_git(target, "status", "--short")
        names = [line for line in status_out.splitlines() if line.strip()][:100]
        diff, truncated = "", False
        if record.get("worktree"):
            diff, truncated = _truncate(_run_git(target, "diff", "--stat", "HEAD"), DIFF_CHARS)
        return {
            "taskId": task_id,
            "worktree": record.get("worktree"),
            "commit": record.get("commit"),
            "files": names,
            "diffStat": diff,
            "diffTruncated": truncated,
        }

    # -- dispatch --------------------------------------------------------------

    def dispatch(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        """Run one named tool with validated arguments."""
        if not isinstance(args, dict):
            raise _error("badArgument", "arguments must be an object")
        if name == "health":
            _check_no_unknown(args, frozenset(), "health")
            return self.health()
        if name == "models":
            _check_no_unknown(args, frozenset(), "models")
            return self.models()
        if name == "sessions":
            _check_no_unknown(args, frozenset({"workspace", "limit", "cursor"}), "sessions")
            limit = args.get("limit", 20)
            if not isinstance(limit, int) or isinstance(limit, bool):
                raise _error("badArgument", "limit must be an integer")
            workspace = args.get("workspace")
            if workspace is not None and not isinstance(workspace, str):
                raise _error("badArgument", "workspace must be a string")
            cursor = args.get("cursor")
            if cursor is not None and not isinstance(cursor, str):
                raise _error("badArgument", "cursor must be a string")
            return self.sessions(workspace, limit, cursor)
        if name == "session_read":
            _check_no_unknown(args, frozenset({"sessionId", "includeItems"}), "session_read")
            include_items = args.get("includeItems", False)
            if not isinstance(include_items, bool):
                raise _error("badArgument", "includeItems must be a boolean")
            return self.session_read(_require_str(args, "sessionId", maximum=256), include_items)
        if name == "start":
            _check_no_unknown(
                args, frozenset({"workspace", "prompt", "mode", "model", "ref", "requestId"}), "start"
            )
            model = args.get("model")
            request_id = args.get("requestId")
            if request_id is not None and not isinstance(request_id, str):
                raise _error("badArgument", "requestId must be a UUID string")
            return self.start(
                workspace=_require_str(args, "workspace", maximum=1024),
                prompt=_require_str(args, "prompt"),
                mode=args.get("mode", "read_only"),
                model=model,
                ref=args.get("ref", "HEAD"),
                request_id=request_id,
            )
        if name == "tasks":
            _check_no_unknown(args, frozenset(), "tasks")
            return self.tasks()
        if name in ("status", "result", "cancel", "changes"):
            _check_no_unknown(args, frozenset({"taskId"}), name)
            task_id = _require_str(args, "taskId", maximum=256)
            return getattr(self, name)(task_id)
        if name == "resume":
            _check_no_unknown(args, frozenset({"taskId", "prompt"}), "resume")
            return self.resume(
                _require_str(args, "taskId", maximum=256),
                _require_str(args, "prompt"),
            )
        if name == "approve":
            _check_no_unknown(
                args, frozenset({"taskId", "approvalId", "requirementId", "choiceId"}), "approve"
            )
            if "requirementId" not in args:
                raise _error("badArgument", "requirementId is required")
            return self.approve(
                _require_str(args, "taskId", maximum=256),
                _require_str(args, "approvalId", maximum=512),
                args["requirementId"],
                _require_str(args, "choiceId", maximum=512),
            )
        raise _error("unknownTool", f"unknown tool {name!r}")


def _fingerprint(source: str, prompt: str, mode: str, model: str | None, commit: str) -> str:
    return json.dumps(
        {"source": source, "prompt": prompt, "mode": mode, "model": model, "commit": commit},
        sort_keys=True,
    )


def _remove_worktree(source: Path, worktree: str) -> None:
    try:
        _run_git(source, "worktree", "remove", "--force", worktree)
    except McpAdapterError:
        pass
