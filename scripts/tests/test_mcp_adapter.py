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


class FakeDaemon:
    """A scripted m8s control socket. Records every request on ``requests``."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.requests: list[dict[str, Any]] = []
        self.lanes: dict[str, dict[str, Any]] = {}
        self.pending_map: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.decides: list[dict[str, Any]] = []
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
            self.requests.append(request)
            try:
                result = self._answer(request)
            except Exception as exc:  # keep the fake alive; surface as daemon error
                conn.sendall(
                    (json.dumps({"ok": False, "error": str(exc)}) + "\n").encode()
                )
                return
            conn.sendall((json.dumps({"ok": True, "result": result}) + "\n").encode())

    def _answer(self, request: dict[str, Any]) -> Any:
        command = request.get("command")
        if command == "health":
            return {"sessions": list(self.lanes.values())}
        if command == "list":
            return {"sessions": list(self.lanes.values())}
        if command == "launch":
            session_id = str(uuid.uuid4())
            turn_id = f"turn-{session_id[:8]}"
            lane = {
                "sessionId": session_id,
                "alias": request.get("name"),
                "workspace": request.get("workspace"),
                "status": "running",
                "approvalMode": request.get("approvalMode"),
                "activeTurnId": turn_id,
            }
            self.lanes[session_id] = lane
            return {"session": lane, "turn": {"turnId": turn_id}}
        if command == "send":
            turn_id = f"turn-{uuid.uuid4().hex[:8]}"
            lane = self.lanes.get(request.get("session"))
            if lane is not None:
                lane["activeTurnId"] = turn_id
            return {"turnId": turn_id}
        if command == "pending":
            return self.pending_map.get(request.get("session"), {"approvals": [], "userInputs": []})
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
            return {"messages": [{"role": "assistant", "content": "hello from lane"}]}
        if method == "view/page":
            return {"events": [], "nextCursor": None}
        if method == "approval/decide":
            self.decides.append({"session": session, **params})
            approvals = self.pending_map.get(session, {}).get("approvals", [])
            self.pending_map[session] = {
                "approvals": [a for a in approvals if a.get("approvalId") != params.get("approvalId")],
                "userInputs": [],
            }
            return {"decided": True}
        if method == "turn/cancel":
            lane = self.lanes.get(session)
            turn_id = (lane or {}).get("activeTurnId")
            if lane is not None:
                lane.pop("activeTurnId", None)
            self.events.append(
                {
                    "kind": "msp.event",
                    "method": "turn/completed",
                    "params": {"sessionId": session, "turnId": turn_id, "terminal": "cancelled"},
                }
            )
            return {"cancelled": True}
        raise AssertionError(f"unexpected call method: {method}")

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
        self.daemon = FakeDaemon(self.tmp / "control.sock")
        self.addCleanup(self.daemon.close)
        self.state = self.tmp / "state"
        self.adapter = Adapter(
            client=ControlClient(socket_path=self.daemon.path), state_dir=self.state
        )

    def start_task(self, repo: Path, **kwargs: Any) -> dict[str, Any]:
        args: dict[str, Any] = {"workspace": str(repo), "prompt": "do the thing"}
        args.update(kwargs)
        return self.adapter.dispatch("start", args)

    def terminal(self, session_id: str, turn_id: str, terminal: str) -> None:
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
        (launch,) = self.daemon.launches()
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
            client=ControlClient(socket_path=self.daemon.path), state_dir=self.state
        )
        wire = [r for r in self.daemon.requests if r.get("command") in ("send", "retire")]
        wire += self.daemon.methods("turn/cancel") + self.daemon.methods("turn/unqueue")
        self.assertEqual(wire, [])
        self.assertIn(session_id, self.daemon.lanes)
        status = restarted.dispatch("status", {"taskId": out["taskId"]})
        self.assertEqual(status["status"], "interrupted")
        result = restarted.dispatch("result", {"taskId": out["taskId"]})
        self.assertEqual(result["status"], "interrupted")
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
        self.terminal(session_id, turn_id, "failed")
        failed = self.adapter.dispatch("result", {"taskId": task_id})
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["terminal"]["terminal"], "failed")
        self.assertIn("hello from lane", failed["evidence"])

    def test_completed_terminal_carries_evidence(self) -> None:
        repo = make_repo()
        out = self.start_task(repo)
        record = self.state.joinpath("tasks", f"{out['taskId']}.json")
        turn_id = json.loads(record.read_text())["turnId"]
        self.terminal(out["sessionId"], turn_id, "completed")
        done = self.adapter.dispatch("result", {"taskId": out["taskId"]})
        self.assertEqual(done["status"], "completed")
        self.assertIn("hello from lane", done["evidence"])

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


class ReadOnlyTests(AdapterCase):
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


if __name__ == "__main__":
    unittest.main()

