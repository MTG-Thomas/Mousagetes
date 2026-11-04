# SPDX-License-Identifier: AGPL-3.0-or-later
"""Local JSON-RPC MSP contract peer; no model, network, or filesystem work."""
import json
import sys

sessions = {}
next_turn = 0
advance_before_cancel = False
for line in sys.stdin:
    frame = json.loads(line)
    method = frame["method"]
    params = frame.get("params", {})
    result = {}
    error = None
    sid = params.get("sessionId")
    if method == "session/list":
        result = {"sessions": list(sessions.values())}
    elif method == "session/start":
        sid = f"session-{len(sessions) + 1}"
        session = {"sessionId": sid, "workspace": params["workspaceRoot"], "status": "idle"}
        sessions[sid] = session
        result = {"session": session, "viewCursor": "v:0"}
    elif method == "session/rename":
        sessions[sid]["name"] = params["name"]
    elif method == "turn/start":
        next_turn += 1
        turn_id = f"turn-{next_turn}"
        sessions[sid]["activeTurnId"] = turn_id
        result = {"turnId": turn_id, "startedNewTurn": True}
    elif method == "session/read":
        result = {"session": dict(sessions[sid])}
    elif method == "test/advanceBeforeCancel":
        advance_before_cancel = True
    elif method == "turn/cancel":
        if advance_before_cancel:
            sessions[sid]["activeTurnId"] = "foreign-turn"
            advance_before_cancel = False
        if params.get("turnId") != sessions[sid].get("activeTurnId"):
            error = {"code": -32000, "message": "turnChanged"}
        else:
            sessions[sid].pop("activeTurnId")
            result = {"cancelledTurnId": params["turnId"]}
    else:
        error = {"code": -32601, "message": method}
    response = {"jsonrpc": "2.0", "id": frame["id"]}
    response["error" if error else "result"] = error if error else result
    print(json.dumps(response), flush=True)
