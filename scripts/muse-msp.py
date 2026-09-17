#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Thin local supervisor for Muse's stdio MSP host.

The daemon owns one ``muse serve`` process and exposes a small JSON-lines
control API on a Unix socket.  It deliberately does not auto-approve tools or
answer user-input requests.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def runtime_dir() -> Path:
    base = os.environ.get("XDG_RUNTIME_DIR")
    path = Path(base) / "muse-msp-supervisor" if base else Path(f"/tmp/muse-msp-{os.getuid()}")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


RUNTIME = runtime_dir()
SOCKET = RUNTIME / "control.sock"
PID_FILE = RUNTIME / "daemon.pid"
EVENTS = RUNTIME / "events.ndjson"
DAEMON_LOG = RUNTIME / "daemon.log"

# Session lists and materialized views can exceed asyncio's 64 KiB default
# after a host owns many sessions.  A LimitOverrunError kills the stdout
# reader and leaves every later control request waiting forever, so size the
# framed JSON-lines transport for realistic multi-session supervision.
MSP_STREAM_LIMIT = 16 * 1024 * 1024


# Methods whose params require a client-minted commandId (per the MSP schema's
# required lists). The generic `call` surface injects one automatically.
COMMAND_METHODS = frozenset(
    {
        "account/loginStart",
        "approval/decide",
        "goal/clear",
        "goal/edit",
        "goal/pause",
        "goal/resume",
        "goal/set",
        "session/compact",
        "session/fork",
        "session/rename",
        "session/resume",
        "session/setApprovalMode",
        "session/setModel",
        "session/setReasoningEffort",
        "session/start",
        "session/userShell",
        "subagent/close",
        "subagent/followupTask",
        "subagent/interrupt",
        "subagent/readResult",
        "subagent/reopen",
        "subagent/resume",
        "subagent/sendMessage",
        "subagent/stop",
        "task/background",
        "task/stop",
        "task/stopAll",
        "turn/cancel",
        "turn/interrupt",
        "turn/start",
        "turn/steer",
        "turn/unqueue",
        "userInput/answer",
        "userInput/cancel",
        "userInput/clarify",
        "workflow/cancel",
        "workflow/childControl",
    }
)


__version__ = "0.1.0"


def uuid7() -> str:
    """Mint an RFC 9562 UUIDv7 without depending on Python 3.14."""
    value = ((int(time.time() * 1000) & ((1 << 48) - 1)) << 80)
    value |= 0x7 << 76
    value |= secrets.randbits(12) << 64
    value |= 0b10 << 62
    value |= secrets.randbits(62)
    h = f"{value:032x}"
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def emit_local(record: dict[str, Any]) -> dict[str, Any]:
    record = {"at": time.time(), **record}
    with EVENTS.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    return record


class MspHost:
    def __init__(self) -> None:
        self.proc: asyncio.subprocess.Process | None = None
        self.pending: dict[int, asyncio.Future[Any]] = {}
        self.next_id = 1
        self.sessions: dict[str, dict[str, Any]] = {}
        self.aliases: dict[str, str] = {}
        self.watchers: set[asyncio.StreamWriter] = set()
        self.stopping = asyncio.Event()

    def record(self, record: dict[str, Any]) -> None:
        saved = emit_local(record)
        payload = (json.dumps(saved, separators=(",", ":")) + "\n").encode()
        stale: list[asyncio.StreamWriter] = []
        for writer in self.watchers:
            if writer.is_closing():
                stale.append(writer)
                continue
            writer.write(payload)
        for writer in stale:
            self.watchers.discard(writer)

    async def start(self) -> None:
        self.proc = await asyncio.create_subprocess_exec(
            "muse",
            "serve",
            "--trust-workspace",
            "--disable-sandbox",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={
                **os.environ,
                "MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS": "on",
                "MUSE_EXPERIMENTAL_LOCAL_SESSION_MESSAGING": "1",
                "MUSE_EXPERIMENTAL_MONITOR": "on",
                "MUSE_EXPERIMENTAL_TAG": "on",
            },
            limit=MSP_STREAM_LIMIT,
        )
        asyncio.create_task(self._read_stdout())
        asyncio.create_task(self._read_stderr())
        result = await self.call(
            "initialize",
            {
                "clientInfo": {
                    "name": "codex_muse_supervisor",
                    "title": "Codex Muse supervisor",
                    "version": "0.1.0",
                },
                "capabilities": {
                    "experimentalApi": True,
                    "userInputDialogs": True,
                },
            },
        )
        await self.notify("initialized")
        self.record(
            {
                "kind": "controller.started",
                "server": result.get("serverInfo", {}),
                "schema": result.get("schema", {}),
            }
        )

    async def stop(self) -> None:
        self.stopping.set()
        if self.proc and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.proc.kill()
                await self.proc.wait()
        self.record({"kind": "controller.stopped"})

    async def _write(self, frame: dict[str, Any]) -> None:
        if not self.proc or not self.proc.stdin:
            raise RuntimeError("Muse host is not running")
        self.proc.stdin.write((json.dumps(frame, separators=(",", ":")) + "\n").encode())
        await self.proc.stdin.drain()

    async def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        request_id = self.next_id
        self.next_id += 1
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        frame: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            frame["params"] = params
        await self._write(frame)
        return await asyncio.wait_for(future, timeout=60)

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        frame: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            frame["params"] = params
        await self._write(frame)

    async def _read_stdout(self) -> None:
        assert self.proc and self.proc.stdout
        while line := await self.proc.stdout.readline():
            try:
                frame = json.loads(line)
            except json.JSONDecodeError:
                self.record({"kind": "controller.protocolError", "detail": "non-JSON MSP frame"})
                continue
            if "id" in frame and "method" not in frame:
                future = self.pending.pop(frame["id"], None)
                if future and not future.done():
                    if "error" in frame:
                        future.set_exception(RuntimeError(json.dumps(frame["error"])))
                    else:
                        future.set_result(frame.get("result"))
                continue
            if "id" in frame and "method" in frame:
                await self._server_request(frame)
                continue
            await self._notification(frame.get("method", "unknown"), frame.get("params", {}))
        error = RuntimeError("Muse MSP host exited")
        for future in self.pending.values():
            if not future.done():
                future.set_exception(error)
        self.pending.clear()
        self.stopping.set()

    async def _read_stderr(self) -> None:
        assert self.proc and self.proc.stderr
        while line := await self.proc.stderr.readline():
            text = line.decode(errors="replace").strip()
            if text:
                self.record({"kind": "controller.stderr", "message": text[:1000]})

    async def _server_request(self, frame: dict[str, Any]) -> None:
        method = frame.get("method", "unknown")
        params = frame.get("params", {})
        await self._write({"jsonrpc": "2.0", "id": frame["id"], "result": {}})
        self.record(
            {
                "kind": "blocker",
                "reason": method,
                "sessionId": params.get("sessionId"),
                "requestId": params.get("approvalId") or params.get("userInputId"),
                "summary": summarize_request(method, params),
            }
        )

    async def _notification(self, method: str, params: dict[str, Any]) -> None:
        session_id = params.get("sessionId")
        if session_id:
            state = self.sessions.setdefault(session_id, {"sessionId": session_id})
            if "viewCursor" in params:
                state["viewCursor"] = params["viewCursor"]
            if method == "session/statusChanged":
                state["status"] = params.get("status")
                state["attention"] = params.get("attention", [])
            elif method == "session/tokenUsage":
                state["tokenUsage"] = params.get("cumulative")
        interesting = {
            "session/statusChanged",
            "session/tokenUsage",
            "turn/completed",
            "turn/retryScheduled",
            "view/gap",
            "session/viewHealthChanged",
            "approval/requested",
            "approval/resolved",
            "userInput/requested",
            "userInput/settled",
            "usage/changed",
        }
        if method in interesting:
            self.record({"kind": "msp.event", "method": method, "params": params})

    def resolve(self, reference: str) -> str:
        return self.aliases.get(reference, reference)

    async def launch(self, args: dict[str, Any]) -> dict[str, Any]:
        workspace = str(Path(args["workspace"]).resolve())
        if not Path(workspace).is_dir():
            raise ValueError(f"workspace does not exist: {workspace}")
        alias = args["name"]
        started = await self.call(
            "session/start",
            {
                "commandId": uuid7(),
                "workspaceRoot": workspace,
                "approvalMode": args.get("approvalMode", "allowAll"),
                **({"modelId": args["model"]} if args.get("model") else {}),
            },
        )
        session = started["session"]
        session_id = session["sessionId"]
        self.sessions[session_id] = {**session, "viewCursor": started["viewCursor"], "alias": alias}
        self.aliases[alias] = session_id
        await self.call(
            "session/rename",
            {"commandId": uuid7(), "sessionId": session_id, "name": alias},
        )
        turn = await self.submit(session_id, args["prompt"], args.get("reasoningEffort"))
        self.record({"kind": "session.launched", "sessionId": session_id, "name": alias, "workspace": workspace})
        return {"session": self.sessions[session_id], "turn": turn}

    async def submit(self, reference: str, prompt: str, reasoning: str | None = None) -> dict[str, Any]:
        session_id = self.resolve(reference)
        params: dict[str, Any] = {
            "commandId": uuid7(),
            "sessionId": session_id,
            "input": [{"type": "text", "text": prompt}],
            "displayText": prompt,
            "ifBusy": "queue",
        }
        if reasoning:
            params["reasoningEffort"] = reasoning
        result = await self.call("turn/start", params)
        self.record({"kind": "turn.submitted", "sessionId": session_id, "turnId": result.get("turnId")})
        return result

    async def list_sessions(self) -> dict[str, Any]:
        result = await self.call("session/list", {"limit": 200})
        owned = []
        for item in result.get("sessions", []):
            if item["sessionId"] in self.sessions:
                owned.append({**item, "alias": self.sessions[item["sessionId"]].get("alias")})
        return {"sessions": owned}

    async def supervised_call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        command_id: str = "auto",
    ) -> Any:
        """Generic MSP passthrough covering every schema method.

        Resolves nothing by itself; the caller supplies sessionIds (use the
        daemon's alias table via the `session` request field). A fresh
        commandId is minted for methods that require one, unless the caller
        passes an explicit id or "off".
        """
        merged = dict(params or {})
        if command_id != "off" and method in COMMAND_METHODS and "commandId" not in merged:
            merged["commandId"] = uuid7() if command_id in (None, "auto") else command_id
        result = await self.call(method, merged)
        self.record({"kind": "controller.call", "method": method, "sessionId": merged.get("sessionId")})
        return result


def summarize_request(method: str, params: dict[str, Any]) -> dict[str, Any]:
    if method == "userInput/request":
        return {
            "tool": params.get("toolName"),
            "questions": [q.get("question") for q in params.get("questions", [])],
        }
    if method == "approval/request":
        subject = params.get("subject", {})
        return {
            "tool": params.get("toolName"),
            "kind": subject.get("kind"),
            "command": subject.get("command"),
        }
    return {}


def read_events(after: float = 0.0, limit: int = 200) -> list[dict[str, Any]]:
    if not EVENTS.exists():
        return []
    records: list[dict[str, Any]] = []
    with EVENTS.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("at", 0) > after:
                records.append(record)
    return records[-limit:]


async def dispatch(host: MspHost, request: dict[str, Any]) -> Any:
    command = request.get("command")
    if command == "launch":
        return await host.launch(request)
    if command == "send":
        return await host.submit(request["session"], request["prompt"], request.get("reasoningEffort"))
    if command == "list":
        return await host.list_sessions()
    if command == "events":
        return {"events": read_events(float(request.get("after", 0)), int(request.get("limit", 200)))}
    if command == "pending":
        result = await host.call("approval/listPending", {"sessionId": host.resolve(request["session"])})
        return result
    if command == "call":
        params = dict(request.get("params") or {})
        if request.get("session"):
            params["sessionId"] = host.resolve(request["session"])
        return await host.supervised_call(request["method"], params, request.get("commandId", "auto"))
    if command == "stop":
        host.stopping.set()
        return {"stopping": True}
    raise ValueError(f"unknown command: {command}")


async def handle_client(host: MspHost, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        line = await asyncio.wait_for(reader.readline(), timeout=30)
        request = json.loads(line)
        if request.get("command") == "watch":
            for record in read_events(float(request.get("after", 0)), int(request.get("limit", 200))):
                writer.write((json.dumps(record, separators=(",", ":")) + "\n").encode())
            await writer.drain()
            host.watchers.add(writer)
            try:
                await reader.read()
            finally:
                host.watchers.discard(writer)
            return
        result = await dispatch(host, request)
        response = {"ok": True, "result": result}
    except Exception as exc:  # Control boundary: errors are returned, not fatal.
        response = {"ok": False, "error": str(exc)}
    writer.write((json.dumps(response, separators=(",", ":")) + "\n").encode())
    await writer.drain()
    writer.close()
    await writer.wait_closed()


async def serve() -> int:
    if SOCKET.exists():
        SOCKET.unlink()
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")
    host = MspHost()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, host.stopping.set)
    server: asyncio.AbstractServer | None = None
    try:
        await host.start()
        server = await asyncio.start_unix_server(lambda r, w: handle_client(host, r, w), path=SOCKET)
        os.chmod(SOCKET, 0o600)
        await host.stopping.wait()
    finally:
        if server:
            server.close()
            await server.wait_closed()
        await host.stop()
        SOCKET.unlink(missing_ok=True)
        PID_FILE.unlink(missing_ok=True)
    return 0


def daemon_running() -> bool:
    if not SOCKET.exists() or not PID_FILE.exists():
        return False
    try:
        os.kill(int(PID_FILE.read_text(encoding="utf-8")), 0)
        return True
    except (OSError, ValueError):
        return False


def start_daemon() -> dict[str, Any]:
    if daemon_running():
        return {"running": True, "pid": int(PID_FILE.read_text())}
    SOCKET.unlink(missing_ok=True)
    PID_FILE.unlink(missing_ok=True)
    with DAEMON_LOG.open("ab") as log:
        proc = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "serve"],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            close_fds=True,
        )
    deadline = time.time() + 15
    while time.time() < deadline:
        if daemon_running():
            return {"running": True, "pid": proc.pid}
        if proc.poll() is not None:
            raise RuntimeError(f"controller exited; inspect {DAEMON_LOG}")
        time.sleep(0.1)
    raise TimeoutError("controller socket did not become ready")


async def client(request: dict[str, Any]) -> dict[str, Any]:
    reader, writer = await asyncio.open_unix_connection(SOCKET, limit=MSP_STREAM_LIMIT)
    writer.write((json.dumps(request, separators=(",", ":")) + "\n").encode())
    await writer.drain()
    response = json.loads(await reader.readline())
    writer.close()
    await writer.wait_closed()
    return response


async def watch(after: float, limit: int) -> None:
    reader, writer = await asyncio.open_unix_connection(SOCKET, limit=MSP_STREAM_LIMIT)
    request = {"command": "watch", "after": after, "limit": limit}
    writer.write((json.dumps(request, separators=(",", ":")) + "\n").encode())
    await writer.drain()
    try:
        while line := await reader.readline():
            print(json.dumps(json.loads(line), sort_keys=True), flush=True)
    finally:
        writer.close()
        await writer.wait_closed()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("serve", help=argparse.SUPPRESS)
    sub.add_parser("up", help="start the MSP controller daemon")
    sub.add_parser("down", help="stop the MSP controller daemon")
    sub.add_parser("list", help="list sessions owned by this controller")
    events = sub.add_parser("events", help="read summarized MSP events")
    events.add_argument("--after", type=float, default=0)
    events.add_argument("--limit", type=int, default=200)
    watching = sub.add_parser("watch", help="stream summarized MSP events")
    watching.add_argument("--after", type=float, default=0)
    watching.add_argument("--limit", type=int, default=200)
    launch = sub.add_parser("launch", help="start a served Muse session and its first turn")
    launch.add_argument("--name", required=True)
    launch.add_argument("--workspace", required=True)
    launch.add_argument("--prompt", required=True)
    launch.add_argument("--model")
    launch.add_argument("--reasoning-effort")
    launch.add_argument(
        "--approval-mode",
        choices=("allowAll", "promptUnmatched", "onRequest", "denyUnmatched"),
        default="allowAll",
    )
    send = sub.add_parser("send", help="queue a prompt on a served session")
    send.add_argument("session")
    send.add_argument("prompt")
    send.add_argument("--reasoning-effort")
    pending = sub.add_parser("pending", help="list pending approvals and user input")
    pending.add_argument("session")

    call = sub.add_parser(
        "call",
        help="generic MSP passthrough: call any schema method (covers all 51)",
    )
    call.add_argument("method", help="MSP method, e.g. session/read or workflow/cancel")
    call.add_argument("--session", help="session alias or id; resolved into params.sessionId")
    call.add_argument("--params", default="{}", help="extra JSON params object")
    call.add_argument(
        "--command-id",
        default="auto",
        help="explicit commandId, 'auto' (default) to mint when required, 'off' to skip",
    )

    read = sub.add_parser("read", help="read a session transcript slice (session/read)")
    read.add_argument("session")

    view = sub.add_parser("view", help="page a session's materialized view incl. workflow items (view/page)")
    view.add_argument("session")
    view.add_argument("--limit", type=int, default=50)
    view.add_argument("--cursor")
    view.add_argument("--anchor")
    view.add_argument("--direction")

    goal = sub.add_parser("goal", help="inspect or drive a session goal (goal/*)")
    goal.add_argument("session")
    goal.add_argument("action", choices=("set", "edit", "pause", "resume", "clear"))
    goal.add_argument("objective", nargs="?", help="required for set/edit")

    sub.add_parser("usage", help="read provider usage window (usage/read)")
    sub.add_parser("models", help="list available models (model/list)")
    sub.add_parser("account", help="read account state (account/read)")
    skills = sub.add_parser("skills", help="list skills for a session (skill/list)")
    skills.add_argument("session")

    turn = sub.add_parser("turn", help="steer, cancel, interrupt, or unqueue a turn (turn/*)")
    turn.add_argument("session")
    turn.add_argument("action", choices=("steer", "cancel", "interrupt", "unqueue"))
    turn.add_argument("--turn-id", help="expectedTurnId for steer, turnId for unqueue")
    turn.add_argument("--input", help="steering prompt text for steer")
    turn.add_argument("--reasoning-effort")

    item = sub.add_parser("item", help="read a work-item output slice (item/readOutput)")
    item.add_argument("session")
    item.add_argument("item_id")
    item.add_argument("output_ref")
    item.add_argument("--offset-bytes", type=int, default=0)
    item.add_argument("--length-bytes", type=int)

    workflow = sub.add_parser("workflow", help="cancel a workflow run or control one child (workflow/*)")
    workflow.add_argument("session")
    workflow.add_argument("action", choices=("cancel", "child"))
    workflow.add_argument("run_id")
    workflow.add_argument("--child-id")
    workflow.add_argument("--attempt", type=int)
    workflow.add_argument("--child-action", help="server-validated child action, e.g. skip/retry")

    subagent = sub.add_parser("subagent", help="message or control a subagent (subagent/*)")
    subagent.add_argument("session")
    subagent.add_argument("subagent_id")
    subagent.add_argument(
        "action",
        choices=("send", "followup", "interrupt", "stop", "close", "resume", "reopen", "read-result"),
    )
    subagent.add_argument("--body", help="message body for send/followup")
    subagent.add_argument("--reason", help="reason for interrupt/stop/close")

    task = sub.add_parser("task", help="stop a background task (task/stop, task/stopAll)")
    task.add_argument("session")
    task.add_argument("action", choices=("stop", "stop-all"))
    task.add_argument("--task-id")

    compact = sub.add_parser("compact", help="compact a session (session/compact)")
    compact.add_argument("session")
    compact.add_argument("--turn-id")
    fork = sub.add_parser("fork", help="fork a session (session/fork)")
    fork.add_argument("session")
    rename = sub.add_parser("rename", help="rename a session (session/rename)")
    rename.add_argument("session")
    rename.add_argument("name")
    resume_session = sub.add_parser("resume-session", help="resume a session (session/resume)")
    resume_session.add_argument("session")
    set_model = sub.add_parser("set-model", help="set a session model (session/setModel)")
    set_model.add_argument("session")
    set_model.add_argument("model")
    set_effort = sub.add_parser("set-effort", help="set reasoning effort (session/setReasoningEffort)")
    set_effort.add_argument("session")
    set_effort.add_argument("effort")
    set_approval = sub.add_parser("set-approval-mode", help="set approval mode (session/setApprovalMode)")
    set_approval.add_argument("session")
    set_approval.add_argument("mode")
    return p


def call_request(
    method: str,
    session: str | None = None,
    params: dict[str, Any] | None = None,
    command_id: str = "auto",
) -> dict[str, Any]:
    """Build a generic-passthrough control request for one MSP method."""
    request: dict[str, Any] = {"command": "call", "method": method, "commandId": command_id}
    if session:
        request["session"] = session
    if params:
        request["params"] = params
    return request


def build_request(args: argparse.Namespace) -> dict[str, Any]:
    """Translate parsed CLI args into a daemon control request."""
    command = args.command
    if command == "down":
        return {"command": "stop"}
    if command == "list":
        return {"command": "list"}
    if command == "events":
        return {"command": "events", "after": args.after, "limit": args.limit}
    if command == "launch":
        return {
            "command": "launch",
            "name": args.name,
            "workspace": args.workspace,
            "prompt": args.prompt,
            "model": args.model,
            "reasoningEffort": args.reasoning_effort,
            "approvalMode": args.approval_mode,
        }
    if command == "send":
        return {
            "command": "send",
            "session": args.session,
            "prompt": args.prompt,
            "reasoningEffort": args.reasoning_effort,
        }
    if command == "pending":
        return {"command": "pending", "session": args.session}
    if command == "call":
        try:
            params = json.loads(args.params)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid --params JSON: {exc}")
        if not isinstance(params, dict):
            raise SystemExit("--params must be a JSON object")
        return call_request(args.method, args.session, params, args.command_id)
    if command == "read":
        return call_request("session/read", args.session)
    if command == "view":
        params: dict[str, Any] = {"limit": args.limit}
        if args.cursor:
            params["cursor"] = args.cursor
        if args.anchor:
            params["anchor"] = args.anchor
        if args.direction:
            params["direction"] = args.direction
        return call_request("view/page", args.session, params)
    if command == "goal":
        if args.action in ("set", "edit") and not args.objective:
            raise SystemExit(f"goal {args.action} requires an objective")
        params = {"objective": args.objective} if args.action in ("set", "edit") else {}
        return call_request(f"goal/{args.action}", args.session, params)
    if command == "usage":
        return call_request("usage/read")
    if command == "models":
        return call_request("model/list")
    if command == "account":
        return call_request("account/read")
    if command == "skills":
        return call_request("skill/list", args.session)
    if command == "turn":
        if args.action == "steer":
            if not args.turn_id or not args.input:
                raise SystemExit("turn steer requires --turn-id and --input")
            params = {"expectedTurnId": args.turn_id, "input": args.input}
            if args.reasoning_effort:
                params["reasoningEffort"] = args.reasoning_effort
            return call_request("turn/steer", args.session, params)
        if args.action == "unqueue":
            if not args.turn_id:
                raise SystemExit("turn unqueue requires --turn-id")
            return call_request("turn/unqueue", args.session, {"turnId": args.turn_id})
        return call_request(f"turn/{args.action}", args.session)
    if command == "item":
        params = {"itemId": args.item_id, "outputRef": args.output_ref, "offsetBytes": args.offset_bytes}
        if args.length_bytes is not None:
            params["lengthBytes"] = args.length_bytes
        return call_request("item/readOutput", args.session, params)
    if command == "workflow":
        if args.action == "cancel":
            return call_request("workflow/cancel", args.session, {"workflowRunId": args.run_id})
        if args.child_id is None or args.attempt is None or not args.child_action:
            raise SystemExit("workflow child requires --child-id, --attempt, and --child-action")
        return call_request(
            "workflow/childControl",
            args.session,
            {
                "workflowRunId": args.run_id,
                "childId": args.child_id,
                "attempt": args.attempt,
                "action": args.child_action,
            },
        )
    if command == "subagent":
        method_map = {
            "send": "subagent/sendMessage",
            "followup": "subagent/followupTask",
            "interrupt": "subagent/interrupt",
            "stop": "subagent/stop",
            "close": "subagent/close",
            "resume": "subagent/resume",
            "reopen": "subagent/reopen",
            "read-result": "subagent/readResult",
        }
        params = {"subagentId": args.subagent_id}
        if args.action in ("send", "followup"):
            if not args.body:
                raise SystemExit(f"subagent {args.action} requires --body")
            params["body"] = args.body
        if args.action in ("interrupt", "stop", "close") and args.reason:
            params["reason"] = args.reason
        return call_request(method_map[args.action], args.session, params)
    if command == "task":
        if args.action == "stop":
            if not args.task_id:
                raise SystemExit("task stop requires --task-id")
            return call_request("task/stop", args.session, {"taskId": args.task_id})
        return call_request("task/stopAll", args.session)
    if command == "compact":
        params = {"turnId": args.turn_id} if args.turn_id else {}
        return call_request("session/compact", args.session, params)
    if command == "fork":
        return call_request("session/fork", args.session)
    if command == "rename":
        return call_request("session/rename", args.session, {"name": args.name})
    if command == "resume-session":
        return call_request("session/resume", args.session)
    if command == "set-model":
        return call_request("session/setModel", args.session, {"model": args.model})
    if command == "set-effort":
        return call_request("session/setReasoningEffort", args.session, {"reasoningEffort": args.effort})
    if command == "set-approval-mode":
        return call_request("session/setApprovalMode", args.session, {"mode": args.mode})
    raise SystemExit(f"unknown command: {command}")


def main() -> int:
    args = parser().parse_args()
    if args.command == "serve":
        return asyncio.run(serve())
    if args.command == "up":
        result = start_daemon()
    else:
        if not daemon_running():
            raise SystemExit("Muse MSP controller is not running; run `muse-msp.py up`")
        if args.command == "watch":
            try:
                asyncio.run(watch(args.after, args.limit))
            except KeyboardInterrupt:
                pass
            return 0
        result = asyncio.run(client(build_request(args)))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
