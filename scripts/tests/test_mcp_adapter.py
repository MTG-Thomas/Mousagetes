#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for the m8s MCP adapter (Codex bridge).

Socket integration with a fake daemon over a real Unix socket: the real
``ControlClient`` framing carries every lane operation, and the fake
records each request, so the suite proves the adapter calls the daemon
only, replays duplicate starts without a second lane, leaves turns alive
across adapter restart, rejects stale approvals and foreign
cancellations, and binds terminal evidence to the admitted turn id.

No live daemon, no muse binary, no network, no tenant writes: git
worktrees are built in throwaway temp repos only.
"""

from __future__ import annotations

import io
import json
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from m8s_acp.daemon import ControlClient  # noqa: E402
from m8s_mcp.adapter import Adapter, McpAdapterError  # noqa: E402
from m8s_mcp.server import Server  # noqa: E402

LANE_A = "11111111-1111-1111-1111-111111111111"
TURN_A = "turn-aaa"


RO_ARGV = ["muse", "serve", "--disable-write", "--disable-shell"]
RW_ARGV = ["muse", "serve", "--trust-workspace", "--disable-sandbox"]


class _UncertainReceipt(Exception):
    """The fake created the lane but lost the reply (for retry tests)."""


class FakeDaemon:
    """A scripted m8s control socket. Records every request on ``requests``."""

    def __init__(self, path: Path, serve_argv: list[str] | None = None) -> None:
        self.path = path
        self.serve_argv = list(serve_argv) if serve_argv is not None else list(RW_ARGV)
        self.requests: list[dict[str, Any]] = []
        self.lanes: dict[str, dict[str, Any]] = {}
        self.pending_map: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.decides: list[dict[str, Any]] = []
        #: Live-shape view frames per session: {"method", "params": {"item"}}.
        self.items: dict[str, list[dict[str, Any]]] = {}
        #: When True, the next launch creates its lane and then answers
        #: ok:false -- an uncertain receipt (lane exists, caller unsure).
        self.fail_next_launch_after_create = False
        self.fail_roster = False
        self.fail_launch_before_create = False
        self.replace_turn_on_cancel = False
        self._lock = threading.Lock()
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._socket.bind(str(path))
        self._socket.listen(16)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self._socket.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        with conn:
            data = bytearray()
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                data.extend(chunk)
                if data.find(b"\n") != -1:
                    break
            try:
                request = json.loads(bytes(data).decode("utf-8"))
            except ValueError:
                conn.sendall(b'{"ok": false, "error": "bad json"}\n')
                return
            with self._lock:
                self.requests.append(request)
            try:
                result = self._answer(request)
            except _UncertainReceipt:
                conn.sendall(
                    (json.dumps({"ok": False, "error": "uncertain: receipt lost"}) + "\n").encode()
                )
                return
            except Exception as exc:  # keep the fake alive; surface as daemon error
                conn.sendall(
                    (json.dumps({"ok": False, "error": str(exc)}) + "\n").encode()
                )
                return
            conn.sendall((json.dumps({"ok": True, "result": result}) + "\n").encode())

    def _answer(self, request: dict[str, Any]) -> Any:
        command = request.get("command")
        if command == "health":
            with self._lock:
                lanes = list(self.lanes.values())
            return {"sessions": lanes, "serveArgv": list(self.serve_argv)}
        if command == "list":
            if self.fail_roster:
                raise RuntimeError("roster unavailable")
            with self._lock:
                lanes = list(self.lanes.values())
            return {"sessions": lanes}
        if command == "launch":
            if self.fail_launch_before_create:
                raise RuntimeError("launch unavailable")
            session_id = str(uuid.uuid4())
            turn_id = f"turn-{session_id[:8]}"
            lane = {
                "sessionId": session_id,
                "alias": request.get("name"),
                "workspace": request.get("workspace"),
                "status": "running",
                "approvalMode": request.get("approvalMode"),
                "activeTurnId": turn_id,
                "launchTurnId": turn_id,
            }
            with self._lock:
                lane["lastTurn"] = {"turnId": turn_id, "terminal": None}
                self.lanes[session_id] = lane
                uncertain = self.fail_next_launch_after_create
                self.fail_next_launch_after_create = False
            if uncertain:
                raise _UncertainReceipt()
            return {"session": lane, "turn": {"turnId": turn_id}}
        if command == "send":
            turn_id = f"turn-{uuid.uuid4().hex[:8]}"
            with self._lock:
                lane = self.lanes.get(request.get("session"))
                if lane is not None:
                    lane["activeTurnId"] = turn_id
                    lane["lastTurn"] = {"turnId": turn_id, "terminal": None}
            return {"turnId": turn_id}
        if command == "pending":
            with self._lock:
                pending = self.pending_map.get(request.get("session"), {"approvals": [], "userInputs": []})
            return pending
        if command == "events":
            return {"events": list(self.events)}
        if command == "call":
            return self._answer_call(request)
        raise AssertionError(f"unexpected control command: {command}")

    def _answer_call(self, request: dict[str, Any]) -> Any:
        method = request.get("method")
        session = request.get("session")
        params = request.get("params") or {}
        if method == "model/list":
            return {"models": [{"modelId": "muse-test-1", "name": "test"}]}
        if method == "session/read":
            # Live shape: history excluded, lastTurn authoritative.
            with self._lock:
                lane = self.lanes.get(session)
                if lane is None:
                    raise RuntimeError(f"unknown session: {session!r}")
                last_turn = lane.get("lastTurn")
            return {
                "history": {"items": None, "mode": "none", "noneReason": "excluded", "snapshot": None},
                "lastTurn": last_turn,
                "pendingRequests": [],
                "session": {"sessionId": session, "status": lane.get("status", "running")},
                "viewCursor": lane.get("viewCursor", "v:0"),
            }
        if method == "view/page":
            # Live shape: notification frames carrying params.item.
            with self._lock:
                frames = list(self.items.get(session, []))
            return {"events": frames, "nextCursor": None}
        if method == "approval/decide":
            with self._lock:
                self.decides.append({"session": session, **params})
                approvals = self.pending_map.get(session, {}).get("approvals", [])
                self.pending_map[session] = {
                    "approvals": [a for a in approvals if a.get("approvalId") != params.get("approvalId")],
                    "userInputs": [],
                }
            return {"decided": True}
        if method == "turn/cancel":
            with self._lock:
                lane = self.lanes.get(session)
                if self.replace_turn_on_cancel and lane is not None:
                    lane["activeTurnId"] = "foreign-turn"
                turn_id = (lane or {}).get("activeTurnId")
                if params.get("expectedTurnId") != turn_id:
                    raise RuntimeError("turnChanged")
                if lane is not None:
                    lane.pop("activeTurnId", None)
                    lane["lastTurn"] = {"turnId": turn_id, "terminal": "cancelled"}
                self.events.append(
                    {
                        "kind": "msp.event",
                        "method": "turn/completed",
                        "params": {"sessionId": session, "turnId": turn_id, "terminal": "cancelled"},
                    }
                )
            return {"cancelled": True}
        raise AssertionError(f"unexpected call method: {method}")

    def add_item(
        self,
        session: str,
        kind: str,
        text: str,
        turn_id: str,
        revision: int = 1,
        item_id: str | None = None,
    ) -> None:
        """Append a live-shape item/completed frame to a lane's view."""
        with self._lock:
            self.items.setdefault(session, []).append(
                {
                    "method": "item/completed",
                    "params": {
                        "item": {
                            "itemId": item_id or f"item-{len(self.items[session])}",
                            "kind": kind,
                            "text": text,
                            "turnId": turn_id,
                            "revision": revision,
                            "status": "completed",
                        }
                    },
                }
            )

    def close(self) -> None:
        self._socket.close()

    def launches(self) -> list[dict[str, Any]]:
        return [r for r in self.requests if r.get("command") == "launch"]

    def methods(self, method: str) -> list[dict[str, Any]]:
        return [r for r in self.requests if r.get("method") == method]


def make_repo() -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="m8s-mcp-src-"))
    subprocess.run(["git", "init", "-q", str(tmp)], check=True)
    subprocess.run(["git", "-C", str(tmp), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(tmp), "config", "user.name", "t"], check=True)
    (tmp / "base.txt").write_text("base\n")
    subprocess.run(["git", "-C", str(tmp), "add", "."], check=True)
    subprocess.run(["git", "-C", str(tmp), "commit", "-qm", "base"], check=True)
    return tmp


class AdapterCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="m8s-mcp-test-"))
        # self.daemon is the enforced read-only host; self.rw_daemon is the
        # trusted YOLO host. The adapter routes by task mode.
        self.daemon = FakeDaemon(self.tmp / "control.sock", serve_argv=RO_ARGV)
        self.addCleanup(self.daemon.close)
        self.rw_daemon = FakeDaemon(self.tmp / "control-rw.sock", serve_argv=RW_ARGV)
        self.addCleanup(self.rw_daemon.close)
        self.state = self.tmp / "state"
        self.adapter = Adapter(
            client=ControlClient(socket_path=self.rw_daemon.path),
            read_only_socket_path=str(self.daemon.path),
            state_dir=self.state,
        )

    def start_task(self, repo: Path, **kwargs: Any) -> dict[str, Any]:
        args: dict[str, Any] = {"workspace": str(repo), "prompt": "do the thing"}
        args.update(kwargs)
        return self.adapter.dispatch("start", args)

    def answer(self, session_id: str, text: str, turn_id: str) -> None:
        """Seed the lane view with the admitted turn's agent answer."""
        self.daemon.add_item(session_id, "agentMessage", text, turn_id)

    def terminal(self, session_id: str, turn_id: str, terminal: str) -> None:
        # Mirror the server: a completed turn clears the lane's active turn.
        lane = self.daemon.lanes.get(session_id)
        if lane is not None and lane.get("activeTurnId") == turn_id:
            lane.pop("activeTurnId", None)
        with self.daemon._lock:
            lane = self.daemon.lanes.get(session_id)
            if lane is not None:
                lane["lastTurn"] = {"turnId": turn_id, "terminal": terminal}
        self.daemon.events.append(
            {
                "kind": "msp.event",
                "method": "turn/completed",
                "params": {"sessionId": session_id, "turnId": turn_id, "terminal": terminal},
            }
        )


class ProtocolTests(AdapterCase):
    def test_initialize_advertises_tools_only(self) -> None:
        stdin = io.StringIO(
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}) + "\n"
        )
        stdout = io.StringIO()
        Server(self.adapter, stdin, stdout).serve()
        (reply,) = [json.loads(line) for line in stdout.getvalue().splitlines()]
        capabilities = reply["result"]["capabilities"]
        self.assertEqual(capabilities, {"tools": {}})
        self.assertIn("serverInfo", reply["result"])

    def test_tools_list_is_exactly_the_twelve(self) -> None:
        stdin = io.StringIO(
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}) + "\n"
        )
        stdout = io.StringIO()
        Server(self.adapter, stdin, stdout).serve()
        (reply,) = [json.loads(line) for line in stdout.getvalue().splitlines()]
        names = [tool["name"] for tool in reply["result"]["tools"]]
        self.assertEqual(
            names,
            [
                "health", "models", "sessions", "session_read", "start", "tasks",
                "status", "result", "resume", "approve", "cancel", "changes",
            ],
        )
        for tool in reply["result"]["tools"]:
            self.assertIn("inputSchema", tool)

    def test_no_resource_or_prompt_surface(self) -> None:
        stdin = io.StringIO(
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "resources/list", "params": {}}) + "\n"
        )
        stdout = io.StringIO()
        Server(self.adapter, stdin, stdout).serve()
        (reply,) = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(reply["error"]["code"], -32601)


class StartTests(AdapterCase):
    def test_read_only_launches_approval_gated_in_source(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        (launch,) = self.daemon.launches()
        self.assertEqual(launch["approvalMode"], "onRequest")
        self.assertEqual(launch["workspace"], str(repo))
        self.assertIn("taskId", out)
        self.assertFalse((self.state / "worktrees").exists() and any((self.state / "worktrees").iterdir()))

    def test_duplicate_request_id_replays_without_second_lane(self) -> None:
        repo = make_repo()
        request_id = str(uuid.uuid4())
        first = self.start_task(repo, requestId=request_id)
        second = self.start_task(repo, requestId=request_id)
        self.assertEqual(first["taskId"], second["taskId"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(len(self.daemon.launches()), 1)

    def test_duplicate_request_id_with_changed_inputs_rejects(self) -> None:
        repo = make_repo()
        request_id = str(uuid.uuid4())
        self.start_task(repo, requestId=request_id)
        with self.assertRaisesRegex(McpAdapterError, "duplicateRequestId"):
            self.start_task(repo, prompt="a different task", requestId=request_id)
        self.assertEqual(len(self.daemon.launches()), 1)

    def test_unknown_ref_rejects_before_any_lane(self) -> None:
        repo = make_repo()
        with self.assertRaises(McpAdapterError):
            self.start_task(repo, ref="no-such-ref-xyz")
        self.assertEqual(self.daemon.launches(), [])

    def test_worktree_uses_exact_ref_and_ignores_dirty_source(self) -> None:
        repo = make_repo()
        (repo / "dirty.txt").write_text("uncommitted\n")
        head = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        out = self.start_task(repo, mode="worktree")
        (launch,) = self.rw_daemon.launches()
        self.assertEqual(self.daemon.launches(), [])
        self.assertEqual(launch["approvalMode"], "allowAll")
        worktree = Path(launch["workspace"])
        self.assertNotEqual(worktree, repo)
        self.assertEqual(
            subprocess.run(
                ["git", "-C", str(worktree), "rev-parse", "HEAD"],
                capture_output=True, text=True, check=True,
            ).stdout.strip(),
            head,
        )
        self.assertFalse((worktree / "dirty.txt").exists())
        record = self.adapter.dispatch("changes", {"taskId": out["taskId"]})
        self.assertEqual(record["commit"], head)
        # Source checkout keeps only its own dirty file; no task artifacts.
        self.assertEqual(
            subprocess.run(
                ["git", "-C", str(repo), "status", "--short"],
                capture_output=True, text=True, check=True,
            ).stdout.strip(),
            "?? dirty.txt",
        )

    def test_active_cap_is_four(self) -> None:
        repo = make_repo()
        for _ in range(4):
            self.start_task(repo)
        with self.assertRaisesRegex(McpAdapterError, "tooManyActive"):
            self.start_task(repo)
        self.assertEqual(len(self.daemon.launches()), 4)


class LifecycleTests(AdapterCase):
    def test_disconnect_leaves_turn_alive_and_never_replays(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        session_id = out["sessionId"]
        # Simulate adapter death: a fresh Adapter on the same state.
        restarted = Adapter(
            client=ControlClient(socket_path=self.rw_daemon.path),
            read_only_socket_path=str(self.daemon.path),
            state_dir=self.state,
        )
        wire = [r for r in self.daemon.requests if r.get("command") in ("send", "retire")]
        wire += self.daemon.methods("turn/cancel") + self.daemon.methods("turn/unqueue")
        self.assertEqual(wire, [])
        self.assertIn(session_id, self.daemon.lanes)
        # The still-live turn reconciles to running, not interrupted.
        status = restarted.dispatch("status", {"taskId": out["taskId"]})
        self.assertEqual(status["status"], "running")
        self.assertTrue(status["lanePresent"])
        # The daemon finishes the turn; the restarted process binds it.
        turn_id = json.loads((self.state / "tasks" / f"{out['taskId']}.json").read_text())["turnId"]
        self.terminal(session_id, turn_id, "completed")
        done = restarted.dispatch("result", {"taskId": out["taskId"]})
        self.assertEqual(done["status"], "completed")
        # Explicit resume re-drives the same lane; nothing was automatic.
        resumed = restarted.dispatch("resume", {"taskId": out["taskId"], "prompt": "continue"})
        self.assertEqual(resumed["taskId"], out["taskId"])
        sends = [r for r in self.daemon.requests if r.get("command") == "send"]
        self.assertEqual(len(sends), 1)
        self.assertEqual(sends[0]["session"], session_id)

    def test_terminal_binds_only_to_admitted_turn(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        task_id, session_id = out["taskId"], out["sessionId"]
        record = self.state.joinpath("tasks", f"{task_id}.json")
        turn_id = json.loads(record.read_text())["turnId"]
        # A completed terminal for a *different* turn must not settle the task.
        self.terminal(session_id, "turn-someone-else", "completed")
        live = self.adapter.dispatch("result", {"taskId": task_id})
        self.assertIsNone(live["terminal"])
        # A failed terminal for the admitted turn settles as failed, never completed.
        # Foreign-turn text in the view must not leak into our evidence.
        self.answer(session_id, "someone else's answer", "turn-someone-else")
        self.answer(session_id, "our admitted answer", turn_id)
        self.terminal(session_id, turn_id, "failed")
        failed = self.adapter.dispatch("result", {"taskId": task_id})
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["terminal"]["terminal"], "failed")
        self.assertIn("our admitted answer", failed["evidence"])
        self.assertNotIn("someone else's answer", failed["evidence"])

    def test_completed_terminal_carries_evidence(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        record = self.state.joinpath("tasks", f"{out['taskId']}.json")
        turn_id = json.loads(record.read_text())["turnId"]
        self.answer(out["sessionId"], "the completed answer", turn_id)
        self.terminal(out["sessionId"], turn_id, "completed")
        done = self.adapter.dispatch("result", {"taskId": out["taskId"]})
        self.assertEqual(done["status"], "completed")
        self.assertIn("the completed answer", done["evidence"])

    def test_cancel_uses_admitted_turn_and_confirms_via_result(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        record = self.state.joinpath("tasks", f"{out['taskId']}.json")
        turn_id = json.loads(record.read_text())["turnId"]
        # Fake daemon appends the cancelled terminal for the admitted turn.
        cancelled = self.adapter.dispatch("cancel", {"taskId": out["taskId"]})
        self.assertEqual(cancelled["turnId"], turn_id)
        self.assertTrue(cancelled["cancelRequested"])
        cancels = self.daemon.methods("turn/cancel")
        self.assertEqual(len(cancels), 1)
        self.assertEqual(cancels[0]["session"], out["sessionId"])
        done = self.adapter.dispatch("result", {"taskId": out["taskId"]})
        self.assertEqual(done["status"], "cancelled")

    def test_foreign_cancellation_rejects_with_no_wire_call(self) -> None:
        with self.assertRaisesRegex(McpAdapterError, "unknownTask"):
            self.adapter.dispatch("cancel", {"taskId": str(uuid.uuid4())})
        self.assertEqual(self.daemon.methods("turn/cancel"), [])

    def test_foreign_approve_and_resume_reject(self) -> None:
        foreign = str(uuid.uuid4())
        with self.assertRaisesRegex(McpAdapterError, "unknownTask"):
            self.adapter.dispatch(
                "approve",
                {"taskId": foreign, "approvalId": "a", "requirementId": {}, "choiceId": "c"},
            )
        with self.assertRaisesRegex(McpAdapterError, "unknownTask"):
            self.adapter.dispatch("resume", {"taskId": foreign, "prompt": "hi"})
        self.assertEqual(self.daemon.methods("approval/decide"), [])
        self.assertEqual([r for r in self.daemon.requests if r.get("command") == "send"], [])


class ApprovalTests(AdapterCase):
    def _task_with_approval(self, repo: Path) -> tuple[str, str, dict[str, Any]]:
        out = self.start_task(repo)
        requirement = {"approvalId": "ap-1", "sourceIndex": 0}
        self.daemon.pending_map[out["sessionId"]] = {
            "approvals": [
                {
                    "approvalId": "ap-1",
                    "currentRequirementId": requirement,
                    "availableChoices": [{"choiceId": "allow"}, {"choiceId": "deny"}],
                }
            ],
            "userInputs": [],
        }
        return out["taskId"], out["sessionId"], requirement

    def test_stale_approval_rejects_without_wire_decide(self) -> None:
        repo = make_repo()
        task_id, _, requirement = self._task_with_approval(repo)
        with self.assertRaisesRegex(McpAdapterError, "approvalNotFound"):
            self.adapter.dispatch(
                "approve",
                {"taskId": task_id, "approvalId": "ap-gone", "requirementId": requirement,
                 "choiceId": "allow"},
            )
        self.assertEqual(self.daemon.methods("approval/decide"), [])

    def test_unoffered_choice_rejects_and_stays_answerable(self) -> None:
        repo = make_repo()
        task_id, _, requirement = self._task_with_approval(repo)
        with self.assertRaisesRegex(McpAdapterError, "invalidChoice"):
            self.adapter.dispatch(
                "approve",
                {"taskId": task_id, "approvalId": "ap-1", "requirementId": requirement,
                 "choiceId": "nuke"},
            )
        self.assertEqual(self.daemon.methods("approval/decide"), [])
        # The refused answer was never marked settled: the valid choice works.
        decided = self.adapter.dispatch(
            "approve",
            {"taskId": task_id, "approvalId": "ap-1", "requirementId": requirement,
             "choiceId": "allow"},
        )
        self.assertEqual(decided["approvalId"], "ap-1")
        (decide,) = self.daemon.methods("approval/decide")
        self.assertEqual(decide["params"]["choiceId"], "allow")

    def test_changed_requirement_rejects(self) -> None:
        repo = make_repo()
        task_id, _, _ = self._task_with_approval(repo)
        with self.assertRaisesRegex(McpAdapterError, "requirementChanged"):
            self.adapter.dispatch(
                "approve",
                {"taskId": task_id, "approvalId": "ap-1",
                 "requirementId": {"approvalId": "ap-1", "sourceIndex": 9}, "choiceId": "allow"},
            )
        self.assertEqual(self.daemon.methods("approval/decide"), [])

    def test_supported_approval_choice_formats_keep_exact_requirement(self) -> None:
        task_id, session_id, _ = self._task_with_approval(make_repo())
        for field, identity, choices in (
            ("approvals", "approvalId", ["allow"]),
            ("pendingApprovals", "requestId", [{"id": "allow"}]),
            ("pending", "id", [{"choiceId": "allow"}]),
        ):
            with self.subTest(field=field):
                self.daemon.pending_map[session_id] = {field: [{identity: "ap-1",
                    "currentRequirementId": 0, "availableChoices": choices}]}
                self.adapter.approve(task_id, "ap-1", 0, "allow")
        self.assertEqual(len(self.daemon.decides), 3)
        self.assertTrue(all(d["requirementId"] == 0 for d in self.daemon.decides))

    def test_explicit_malformed_choices_reject(self) -> None:
        task_id, session_id, requirement = self._task_with_approval(make_repo())
        self.daemon.pending_map[session_id]["approvals"][0]["availableChoices"] = {"allow": True}
        with self.assertRaisesRegex(McpAdapterError, "invalidChoice"):
            self.adapter.approve(task_id, "ap-1", requirement, "allow")
        self.assertEqual(self.daemon.decides, [])


class ReadOnlyTests(AdapterCase):
    def test_roster_merges_both_hosts_and_read_falls_back(self) -> None:
        repo = make_repo()
        ro_task = self.start_task(repo, prompt="ro work")
        rw_task = self.start_task(repo, prompt="rw work", mode="worktree")
        roster = self.adapter.dispatch("sessions", {})["sessions"]
        by_session = {entry["sessionId"]: entry["daemon"] for entry in roster if "sessionId" in entry}
        self.assertEqual(by_session.get(ro_task["sessionId"]), "ro")
        self.assertEqual(by_session.get(rw_task["sessionId"]), "rw")
        ro_before = len([r for r in self.rw_daemon.requests if r.get("method") == "session/read"])
        ro_read = self.adapter.dispatch("session_read", {"sessionId": ro_task["sessionId"]})
        self.assertEqual(ro_read["daemon"], "ro")
        # Owned ro sessions never touch the wrong host first.
        self.assertEqual(
            len([r for r in self.rw_daemon.requests if r.get("method") == "session/read"]),
            ro_before,
        )
        self.assertEqual(ro_read["snapshot"]["session"]["sessionId"], ro_task["sessionId"])
        rw_read = self.adapter.dispatch("session_read", {"sessionId": rw_task["sessionId"]})
        self.assertEqual(rw_read["daemon"], "rw")

    def test_history_paths_take_no_lease(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        self.adapter.dispatch("sessions", {})
        self.adapter.dispatch("session_read", {"sessionId": out["sessionId"]})
        self.adapter.dispatch("session_read", {"sessionId": out["sessionId"], "includeItems": True})
        commands = {r.get("command") for r in self.daemon.requests}
        methods = {r.get("method") for r in self.daemon.requests}
        self.assertNotIn("send", commands)
        self.assertNotIn("adopt", commands)
        self.assertNotIn("session/resume", methods)


class FailClosedTests(AdapterCase):
    def unconfigured_adapter(self) -> Adapter:
        return Adapter(
            client=ControlClient(socket_path=self.rw_daemon.path), state_dir=self.state
        )

    def test_read_only_without_ro_socket_fails_closed(self) -> None:
        repo = make_repo()
        with self.assertRaisesRegex(McpAdapterError, "unsupportedReadOnly"):
            self.unconfigured_adapter().dispatch("start", {"workspace": str(repo), "prompt": "x"})
        self.assertEqual(self.rw_daemon.launches(), [])
        self.assertEqual(self.daemon.launches(), [])

    def test_read_only_routes_to_enforced_socket(self) -> None:
        repo = make_repo()
        self.start_task(repo)
        (launch,) = self.daemon.launches()
        self.assertEqual(self.rw_daemon.launches(), [])
        self.assertEqual(launch["approvalMode"], "onRequest")
        self.assertEqual(launch["workspace"], str(repo))
        health = self.adapter.dispatch("health", {})
        read_only = health["readOnly"]
        self.assertTrue(read_only["configured"])
        self.assertTrue(read_only["enforced"])
        self.assertIn("--disable-write", read_only["serveArgv"])
        self.assertIn("--disable-shell", read_only["serveArgv"])

    def test_read_only_misconfigured_socket_rejects(self) -> None:
        bad = FakeDaemon(self.tmp / "control-bad.sock", serve_argv=["muse", "serve"])
        self.addCleanup(bad.close)
        adapter = Adapter(
            client=ControlClient(socket_path=self.rw_daemon.path),
            read_only_socket_path=str(bad.path),
            state_dir=self.state,
        )
        repo = make_repo()
        with self.assertRaisesRegex(McpAdapterError, "readOnlyMisconfigured"):
            adapter.dispatch("start", {"workspace": str(repo), "prompt": "x"})
        self.assertEqual(bad.launches(), [])
        self.assertEqual(self.rw_daemon.launches(), [])

    def test_health_reports_unconfigured_read_only(self) -> None:
        health = self.unconfigured_adapter().dispatch("health", {})
        self.assertFalse(health["readOnly"]["configured"])
        self.assertIsNone(health["readOnly"]["enforced"])

    def test_legacy_approval_gated_record_stays_visible(self) -> None:
        # A pre-enforcement read_only task (no daemon pointer, lane on the
        # trusted host) keeps reporting instead of failing or hiding.
        repo = make_repo()
        out = self.adapter.dispatch(
            "start", {"workspace": str(repo), "prompt": "legacy", "mode": "worktree"}
        )
        path = self.state / "tasks" / f"{out['taskId']}.json"
        record = json.loads(path.read_text())
        record["mode"] = "read_only"
        del record["daemon"]
        path.write_text(json.dumps(record))
        status = self.adapter.dispatch("status", {"taskId": out["taskId"]})
        self.assertEqual(status["enforcement"], "approval-gated-legacy")


class ReconnectRecoveryTests(AdapterCase):
    def fresh(self) -> Adapter:
        return Adapter(
            client=ControlClient(socket_path=self.rw_daemon.path),
            read_only_socket_path=str(self.daemon.path),
            state_dir=self.state,
        )

    def test_reconnect_recovers_live_then_completed_turn(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        task_id, session_id = out["taskId"], out["sessionId"]
        turn_id = json.loads((self.state / "tasks" / f"{task_id}.json").read_text())["turnId"]
        second = self.fresh()
        status = second.dispatch("status", {"taskId": task_id})
        self.assertEqual(status["status"], "running")
        live = second.dispatch("result", {"taskId": task_id})
        self.assertIsNone(live["terminal"])
        # The daemon finishes the admitted turn; the second process binds it.
        self.answer(session_id, "recovered answer", turn_id)
        self.terminal(session_id, turn_id, "completed")
        done = second.dispatch("result", {"taskId": task_id})
        self.assertEqual(done["status"], "completed")
        self.assertIn("recovered answer", done["evidence"])

    def test_lane_gone_with_no_terminal_becomes_interrupted(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        del self.daemon.lanes[out["sessionId"]]
        second = self.fresh()
        status = second.dispatch("status", {"taskId": out["taskId"]})
        self.assertEqual(status["status"], "interrupted")
        self.assertFalse(status["lanePresent"])


class AtomicStartTests(AdapterCase):
    def test_concurrent_same_request_starts_one_lane(self) -> None:
        import threading

        repo = make_repo()
        request_id = str(uuid.uuid4())
        barrier = threading.Barrier(8)
        outcomes: list[Any] = []

        def go() -> None:
            barrier.wait()
            try:
                outcomes.append(
                    self.adapter.dispatch(
                        "start",
                        {"workspace": str(repo), "prompt": "race", "requestId": request_id},
                    )
                )
            except Exception as exc:  # noqa: BLE001 -- collected, then asserted
                outcomes.append(exc)

        threads = [threading.Thread(target=go) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(outcomes), 8)
        task_ids = set()
        for outcome in outcomes:
            self.assertIsInstance(outcome, dict)
            task_ids.add(outcome["taskId"])
        self.assertEqual(len(task_ids), 1)
        self.assertEqual(len(self.daemon.launches()), 1)
        self.assertEqual(len(self.daemon.lanes), 1)

    def test_uncertain_launch_receipt_adopts_single_lane(self) -> None:
        repo = make_repo()
        request_id = str(uuid.uuid4())
        self.daemon.fail_next_launch_after_create = True
        out = self.adapter.dispatch(
            "start", {"workspace": str(repo), "prompt": "unsure", "requestId": request_id}
        )
        # The orphaned lane was adopted, not duplicated.
        self.assertEqual(len(self.daemon.launches()), 1)
        self.assertEqual(len(self.daemon.lanes), 1)
        (session_id,) = list(self.daemon.lanes)
        self.assertEqual(out["sessionId"], session_id)
        again = self.adapter.dispatch(
            "start", {"workspace": str(repo), "prompt": "unsure", "requestId": request_id}
        )
        self.assertEqual(again["taskId"], out["taskId"])
        self.assertEqual(len(self.daemon.launches()), 1)

    def test_uncertain_launch_and_roster_failure_preserves_worktree(self) -> None:
        repo = make_repo()
        request_id = str(uuid.uuid4())
        self.rw_daemon.fail_next_launch_after_create = True
        self.rw_daemon.fail_roster = True
        args = {"workspace": str(repo), "prompt": "unsure", "mode": "worktree", "requestId": request_id}
        with self.assertRaises(McpAdapterError) as ctx:
            self.adapter.dispatch("start", args)
        self.assertEqual(ctx.exception.kind, "admissionUncertain")
        record = self.adapter._store.find_by_request(request_id)
        self.assertEqual(record["status"], "starting")
        worktree = Path(record["workspace"])
        self.assertTrue(worktree.is_dir())
        (worktree / "valuable.txt").write_text("do not destroy")
        restarted = Adapter(client=ControlClient(self.rw_daemon.path), state_dir=self.state)
        with self.assertRaises(McpAdapterError):
            restarted.dispatch("start", args)
        self.assertEqual(len(self.rw_daemon.launches()), 1)
        self.rw_daemon.fail_roster = False
        # A later foreign turn must not replace the launch receipt.
        lane = next(iter(self.rw_daemon.lanes.values()))
        admitted = lane["launchTurnId"]
        lane["activeTurnId"] = "foreign-turn"
        out = restarted.dispatch("start", args)
        self.assertEqual(out["taskId"], record["taskId"])
        self.assertEqual(restarted._owned(out["taskId"])["turnId"], admitted)
        self.assertEqual((worktree / "valuable.txt").read_text(), "do not destroy")
        self.assertEqual(len(self.rw_daemon.launches()), 1)

    def test_absent_roster_does_not_replay_uncertain_prompt(self) -> None:
        repo = make_repo()
        request_id = str(uuid.uuid4())
        self.daemon.fail_launch_before_create = True
        args = {"workspace": str(repo), "prompt": "unsure", "requestId": request_id}
        with self.assertRaises(McpAdapterError):
            self.adapter.dispatch("start", args)
        self.daemon.fail_launch_before_create = False
        with self.assertRaises(McpAdapterError) as ctx:
            self.adapter.dispatch("start", args)
        self.assertEqual(ctx.exception.kind, "admissionUncertain")
        self.assertEqual(len(self.daemon.launches()), 1)

    def test_cancel_rejects_foreign_turn_and_roster_race(self) -> None:
        out = self.start_task(make_repo())
        lane = self.daemon.lanes[out["sessionId"]]
        admitted = lane["activeTurnId"]
        lane["activeTurnId"] = "foreign-turn"
        with self.assertRaises(McpAdapterError) as ctx:
            self.adapter.cancel(out["taskId"])
        self.assertEqual(ctx.exception.kind, "turnChanged")
        self.assertEqual(self.daemon.methods("turn/cancel"), [])
        lane["activeTurnId"] = admitted
        self.daemon.replace_turn_on_cancel = True
        with self.assertRaises(McpAdapterError):
            self.adapter.cancel(out["taskId"])
        self.assertEqual(lane["activeTurnId"], "foreign-turn")
        self.assertFalse(self.adapter._owned(out["taskId"])["cancelRequested"])

    def test_read_only_posture_is_reverified_for_status_and_resume(self) -> None:
        out = self.start_task(make_repo())
        self.terminal(out["sessionId"], self.adapter._owned(out["taskId"])["turnId"], "completed")
        self.daemon.serve_argv = list(RW_ARGV)
        self.assertEqual(self.adapter.status(out["taskId"])["enforcement"], "read-only-unverified")
        with self.assertRaises(McpAdapterError) as ctx:
            self.adapter.resume(out["taskId"], "continue")
        self.assertEqual(ctx.exception.kind, "readOnlyMisconfigured")
        self.assertFalse(any(r.get("command") == "send" for r in self.daemon.requests))

    def test_completed_task_cannot_resume_into_foreign_active_turn(self) -> None:
        out = self.start_task(make_repo())
        self.terminal(out["sessionId"], self.adapter._owned(out["taskId"])["turnId"], "completed")
        self.daemon.lanes[out["sessionId"]]["activeTurnId"] = "foreign-turn"
        with self.assertRaisesRegex(McpAdapterError, "turnBusy"):
            self.adapter.resume(out["taskId"], "continue")
        self.assertFalse(any(r.get("command") == "send" for r in self.daemon.requests))

    def test_full_request_uuid_keeps_shared_prefix_aliases_unique(self) -> None:
        repo = make_repo()
        for suffix in ("000000000001", "000000000002"):
            self.start_task(repo, requestId=f"aaaaaaaa-0000-0000-0000-{suffix}")
        self.assertEqual(len({r["name"] for r in self.daemon.launches()}), 2)


class LiveShapeTests(AdapterCase):
    """Regression for the staged smoke finding (task 3cfe1f0a…).

    The daemon excludes history in ``session/read``
    (``history.mode: none``) and carries transcript content as
    ``view/page`` notification frames whose ``params.item`` holds the
    turn binding. Evidence must come from those items for exactly the
    admitted turn — never empty on a real completed turn, never foreign
    text, never the wrong host's answer.
    """

    def seed_turn(self, session_id: str, turn_id: str) -> None:
        # A prior brief turn plus the admitted turn, each with an answer,
        # plus a stale revision of the admitted answer (highest rev wins).
        self.answer(session_id, "brief answer", "turn-brief")
        self.daemon.add_item(
            session_id, "agentMessage", "stale draft", turn_id, revision=1, item_id="ans"
        )
        self.daemon.add_item(
            session_id, "agentMessage", "# Mousagetes (m8s)\n", turn_id, revision=2, item_id="ans"
        )
        self.daemon.add_item(session_id, "toolCall", "read_file", turn_id)

    def test_result_evidence_is_admitted_turn_answer(self) -> None:
        repo = make_repo()
        out = self.start_task(repo, prompt="Read README.md only")
        record = self.state.joinpath("tasks", f"{out['taskId']}.json")
        turn_id = json.loads(record.read_text())["turnId"]
        self.seed_turn(out["sessionId"], turn_id)
        status = self.adapter.dispatch("status", {"taskId": out["taskId"]})
        self.assertIn("# Mousagetes (m8s)", status["preview"])
        self.assertNotIn("brief answer", status["preview"])
        self.assertNotIn("stale draft", status["preview"])
        self.terminal(out["sessionId"], turn_id, "completed")
        done = self.adapter.dispatch("result", {"taskId": out["taskId"]})
        self.assertEqual(done["status"], "completed")
        self.assertIn("# Mousagetes (m8s)", done["evidence"])
        self.assertFalse(done["evidenceTruncated"])

    def test_include_items_returns_plain_item_dicts(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        record = self.state.joinpath("tasks", f"{out['taskId']}.json")
        turn_id = json.loads(record.read_text())["turnId"]
        self.seed_turn(out["sessionId"], turn_id)
        read = self.adapter.dispatch(
            "session_read", {"sessionId": out["sessionId"], "includeItems": True}
        )
        self.assertEqual(read["daemon"], "ro")
        kinds = [item.get("kind") for item in read["items"]]
        self.assertIn("agentMessage", kinds)
        for item in read["items"]:
            self.assertNotIn("method", item)
            self.assertIn("kind", item)
            self.assertIn("turnId", item)

    def test_last_turn_fallback_binds_preview_without_record_turn(self) -> None:
        # A record that lost its turn pointer still previews via lastTurn.
        repo = make_repo()
        out = self.start_task(repo)
        record_path = self.state.joinpath("tasks", f"{out['taskId']}.json")
        record = json.loads(record_path.read_text())
        turn_id = record["turnId"]
        self.answer(out["sessionId"], "fallback answer", turn_id)
        record["turnId"] = None
        record_path.write_text(json.dumps(record))
        status = self.adapter.dispatch("status", {"taskId": out["taskId"]})
        self.assertIn("fallback answer", status["preview"])


if __name__ == "__main__":
    unittest.main()
