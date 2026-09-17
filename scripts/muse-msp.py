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


# All 51 MSP methods on the experimental surface (exported via
# `muse schema generate-json-schema --out DIR --experimental`; the method
# index plus the parity gate's deferral register is the authoritative list).
# The generic `call` surface validates against this snapshot before hitting
# the wire so typos fail fast instead of as daemon round-trip failures.
MSP_METHODS = frozenset(
    {
        "account/loginCancel",
        "account/loginStart",
        "account/logout",
        "account/read",
        "approval/decide",
        "approval/listPending",
        "goal/clear",
        "goal/edit",
        "goal/pause",
        "goal/resume",
        "goal/set",
        "initialize",
        "item/readOutput",
        "model/list",
        "session/compact",
        "session/fork",
        "session/list",
        "session/read",
        "session/rename",
        "session/resume",
        "session/setApprovalMode",
        "session/setModel",
        "session/setReasoningEffort",
        "session/start",
        "session/userShell",
        "skill/list",
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
        "usage/read",
        "userInput/answer",
        "userInput/cancel",
        "userInput/clarify",
        "view/page",
        "view/subscribe",
        "view/unsubscribe",
        "workflow/cancel",
        "workflow/childControl",
    }
)

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


__version__ = "0.5.0"


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


BUDGETS_FILE = RUNTIME / "budgets.json"

# Spend is a paged decision-class event (moonshot stewardship): budget breach
# records carry this marker so supervisors page instead of scrolling past.
SPEND_DECISION_CLASS = "spend"

# Idle threshold for stuck-lane detection. Overridable for tests and hosts via
# M8S_STUCK_AFTER_SECONDS; invalid values fall back to the default.
STUCK_IDLE_SECONDS = 30 * 60


def stuck_after_seconds() -> int:
    try:
        return max(1, int(os.environ.get("M8S_STUCK_AFTER_SECONDS", STUCK_IDLE_SECONDS)))
    except (TypeError, ValueError):
        return STUCK_IDLE_SECONDS


def load_budgets() -> dict[str, dict[str, Any]]:
    try:
        raw = json.loads(BUDGETS_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def save_budgets(budgets: dict[str, dict[str, Any]]) -> None:
    BUDGETS_FILE.write_text(json.dumps(budgets, sort_keys=True), encoding="utf-8")


# Retired sessions (issue #21): served sessions whose duty is complete stay
# on the roster as idle members and accrue `stuck` flags. Retiring drops a
# session from supervision; the retired ids persist here so a daemon bounce
# never resurrects them into the roster, health, or stuck accounting.
RETIRED_FILE = RUNTIME / "retired.json"


def load_retired() -> dict[str, dict[str, Any]]:
    try:
        raw = json.loads(RETIRED_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def save_retired(retired: dict[str, dict[str, Any]]) -> None:
    RETIRED_FILE.write_text(json.dumps(retired, sort_keys=True), encoding="utf-8")


def worktree_dirty(workspace: str | None, run: Any = None) -> bool | None:
    """Uncommitted-work signal for a lane worktree (never raises).

    True means `git status --porcelain` reports entries; False means clean.
    None means unknown (no workspace, not a repo, or git unavailable) —
    callers judge by the remaining signals alone. `run` injects the
    subprocess runner (tests); default is `subprocess.run`.
    """
    if not workspace:
        return None
    runner = run or subprocess.run
    try:
        proc = runner(
            ["git", "-C", str(workspace), "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    return bool((proc.stdout or "").strip())


def normalize_budget(spec: dict[str, Any]) -> dict[str, Any]:
    """Coerce a lane budget to {maxTokens?, maxContextTokens?, models?}.

    Raises ValueError on non-positive caps, empty model lists, or unknown
    keys so misconfiguration fails fast instead of silently not enforcing.
    """
    allowed = {"maxTokens", "maxContextTokens", "models"}
    unknown = set(spec) - allowed
    if unknown:
        raise ValueError(f"unknown budget keys: {sorted(unknown)}")
    budget: dict[str, Any] = {}
    for key in ("maxTokens", "maxContextTokens"):
        value = spec.get(key)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"budget {key} must be a positive integer")
        budget[key] = value
    models = spec.get("models")
    if models is not None:
        if isinstance(models, str):
            models = [m.strip() for m in models.split(",")]
        if (
            not isinstance(models, list)
            or not models
            or any(not isinstance(m, str) or not m for m in models)
        ):
            raise ValueError("budget models must be a non-empty list of model ids")
        budget["models"] = list(models)
    if not budget:
        raise ValueError("budget needs at least one of maxTokens, maxContextTokens, models")
    return budget


def usage_total(token_usage: Any) -> int | None:
    """Latest cumulative token total, or None when no usage was reported."""
    if isinstance(token_usage, dict):
        total = token_usage.get("totalTokens")
        return total if isinstance(total, int) else None
    return token_usage if isinstance(token_usage, int) else None


def budget_breach(state: dict[str, Any]) -> dict[str, Any] | None:
    """Return the breached budget limit for a lane state, if any.

    Pure check over the lane's configured budget plus its latest reported
    token/context usage and model. Returns {"limit": ...} or None.
    """
    budget = state.get("budget") or {}
    if not budget:
        return None
    total = usage_total(state.get("tokenUsage"))
    if (
        budget.get("maxTokens") is not None
        and total is not None
        and total >= budget["maxTokens"]
    ):
        return {"limit": "maxTokens", "total": total, "cap": budget["maxTokens"]}
    context = state.get("contextUsage") or {}
    used = context.get("usedTokens")
    if (
        budget.get("maxContextTokens") is not None
        and isinstance(used, int)
        and used >= budget["maxContextTokens"]
    ):
        return {"limit": "maxContextTokens", "total": used, "cap": budget["maxContextTokens"]}
    models = budget.get("models")
    if models and state.get("modelId") and state["modelId"] not in models:
        return {"limit": "models", "model": state["modelId"], "allowed": models}
    return None


def lane_stuck(state: dict[str, Any], now: float, after: int | None = None) -> dict[str, Any] | None:
    """Flag a stuck lane: failed/cancelled turn awaiting owner action, or no
    transcript/repo activity past the threshold.

    Pure check returning {"reason": ...} or None. Lanes never observed
    (no lastActivity) are not stuck — avoids flagging fresh or foreign lanes.
    """
    limit = STUCK_IDLE_SECONDS if after is None else after
    if state.get("needsOwnerAction"):
        terminal = state.get("lastTerminal", "failed")
        return {"reason": f"turn{str(terminal).capitalize()}", "needsOwnerAction": True}
    candidates = [state.get("lastActivity"), state.get("lastRepoActivity")]
    latest = max((t for t in candidates if isinstance(t, (int, float))), default=None)
    if latest is None:
        return None
    idle = now - latest
    if idle >= limit:
        return {"reason": "idle", "idleForSeconds": idle}
    return None


def repo_activity_ts(workspace: str | None) -> float | None:
    """Cheapest honest repo-activity signal: newest mtime of the worktree's
    git HEAD/branch ref (commits, checkouts). Returns None when unknown —
    callers then judge by transcript activity alone. Never raises."""
    if not workspace:
        return None
    try:
        workdir = Path(workspace)
        if not workdir.is_dir():
            return None
        proc = subprocess.run(
            ["git", "-C", str(workdir), "rev-parse", "--git-dir"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if proc.returncode != 0:
            return None
        git_dir = Path(proc.stdout.strip())
        if not git_dir.is_absolute():
            git_dir = workdir / git_dir
        stamps: list[float] = []
        for ref in (git_dir / "HEAD", git_dir / "refs"):
            try:
                if ref.is_file():
                    stamps.append(ref.stat().st_mtime)
                elif ref.is_dir():
                    newest = max(
                        (p.stat().st_mtime for p in ref.rglob("*") if p.is_file()),
                        default=None,
                    )
                    if newest is not None:
                        stamps.append(newest)
            except OSError:
                continue
        return max(stamps) if stamps else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


# ---------------------------------------------------------------------------
# Moonshot P3: second host over SSH with registry and namespaced identity.
#
# Transport rule (docs/moonshot.md): `muse serve` is stdio-only. MSP never
# goes on a raw network listener — SSH (OpenSSH CLI, stdlib subprocess) is
# the only sanctioned carrier. A remote host is therefore a local `ssh`
# subprocess whose stdio carries the peer's `muse serve` frames.
# ---------------------------------------------------------------------------

# Local `muse serve` argv owned by this agent. Remote hosts wrap the same
# argv in ssh (see build_ssh_serve_argv); no TCP/port/bind option exists.
SERVE_ARGV = ("muse", "serve", "--trust-workspace", "--disable-sandbox")

HOSTS_FILE = RUNTIME / "hosts.json"

# Liveness TTL for enrolled hosts. Overridable via M8S_HOST_TTL_SECONDS;
# invalid values fall back to the default.
HOST_ALIVE_TTL_SECONDS = 120

# Host names double as lane-namespace prefixes (`host/alias`), so they must
# not contain the `/` separator.
HOST_NAME_RE = r"[A-Za-z0-9][A-Za-z0-9_.-]*"


def host_ttl_seconds() -> int:
    try:
        return max(1, int(os.environ.get("M8S_HOST_TTL_SECONDS", HOST_ALIVE_TTL_SECONDS)))
    except (TypeError, ValueError):
        return HOST_ALIVE_TTL_SECONDS


def agent_id() -> str:
    """Stable id of this agent: hostname plus its own control socket path.

    One m8s agent owns exactly one serve process per host; the registry
    records this id so a second agent cannot silently share the host —
    federation is agent-to-agent, never by sharing a host.
    """
    import socket

    return f"{socket.gethostname()}:{SOCKET}"


class HostError(ValueError):
    """Typed registry/carrier error.

    ``kind`` is machine-readable: "badHostName", "hostExists",
    "hostNotFound", "hostOwned", "hostUnreachable", "noCapacity",
    "noRemoteMsp".
    """

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def check_host_name(name: str) -> None:
    import re

    if not isinstance(name, str) or not re.fullmatch(HOST_NAME_RE, name):
        raise HostError(
            "badHostName",
            f"invalid host name {name!r}: use letters, digits, '_', '-', '.' (no '/')",
        )


def build_ssh_serve_argv(target: str, ssh_port: int | None = None) -> list[str]:
    """Argv carrying a peer's `muse serve` stdio over SSH.

    stdlib subprocess with the OpenSSH CLI, no shell, no network listener:
    `ssh [port] target -- muse serve ...`. Raises HostError on an empty
    target or an invalid port.
    """
    if not isinstance(target, str) or not target.strip() or any(
        ch.isspace() for ch in target
    ):
        raise HostError("badHostName", f"invalid SSH target: {target!r}")
    if ssh_port is not None and (
        isinstance(ssh_port, bool) or not isinstance(ssh_port, int) or not 1 <= ssh_port <= 65535
    ):
        raise HostError("badHostName", f"invalid SSH port: {ssh_port!r}")
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
    if ssh_port is not None:
        argv += ["-p", str(ssh_port)]
    argv += [target, "--", *SERVE_ARGV]
    return argv


def ssh_probe(
    target: str,
    ssh_port: int | None = None,
    timeout: int = 20,
    run: Any = None,
) -> dict[str, Any]:
    """Verify a host is reachable and carries `muse` before enrolling it.

    Runs `command -v muse && muse --version` over SSH (BatchMode: fail fast
    instead of prompting). Returns {"target", "musePath", "version"} or
    raises HostError("hostUnreachable"). ``run`` injects the subprocess
    runner for tests; defaults to subprocess.run.
    """
    import subprocess as _subprocess

    runner = run or _subprocess.run
    if not isinstance(target, str) or not target.strip():
        raise HostError("badHostName", f"invalid SSH target: {target!r}")
    argv = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={timeout}"]
    if ssh_port is not None:
        argv += ["-p", str(ssh_port)]
    argv += [target, "command -v muse && muse --version"]
    try:
        proc = runner(argv, capture_output=True, text=True, timeout=timeout + 10)
    except (OSError, _subprocess.SubprocessError) as exc:
        raise HostError("hostUnreachable", f"SSH probe to {target!r} failed: {exc}")
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()[:300]
        raise HostError("hostUnreachable", f"SSH probe to {target!r} failed: {detail}")
    lines = (proc.stdout or "").strip().splitlines()
    return {
        "target": target,
        "musePath": lines[0].strip() if lines else "",
        "version": lines[1].strip() if len(lines) > 1 else "",
    }


def load_hosts() -> dict[str, dict[str, Any]]:
    try:
        raw = json.loads(HOSTS_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def save_hosts(hosts: dict[str, dict[str, Any]]) -> None:
    HOSTS_FILE.write_text(json.dumps(hosts, sort_keys=True), encoding="utf-8")


def host_alive(record: dict[str, Any], now: float, ttl: int | None = None) -> bool:
    """A host is live when its last heartbeat is inside the TTL window."""
    limit = HOST_ALIVE_TTL_SECONDS if ttl is None else ttl
    if not record.get("alive", False):
        return False
    seen = record.get("lastHeartbeat")
    return isinstance(seen, (int, float)) and (now - seen) <= limit


def enroll_host(
    name: str,
    ssh_target: str,
    max_lanes: int = 4,
    ssh_port: int | None = None,
    remote_msp: str | None = None,
    owner: str | None = None,
    probe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Enroll a host after a successful SSH probe. Pure registry write.

    ``probe`` is the ssh_probe() result (injected by callers/tests; when
    None the caller already probed). Re-enroll by the same owner updates the
    record; a different live owner is refused — exactly one agent per host.
    """
    check_host_name(name)
    if isinstance(max_lanes, bool) or not isinstance(max_lanes, int) or max_lanes < 1:
        raise HostError("badHostName", f"maxLanes must be a positive integer: {max_lanes!r}")
    hosts = load_hosts()
    me = owner or agent_id()
    existing = hosts.get(name)
    if existing and existing.get("agentId") not in (None, me):
        if host_alive(existing, time.time()):
            raise HostError(
                "hostOwned",
                f"host {name!r} is owned by live agent {existing['agentId']}; "
                "federate agent-to-agent instead of sharing the host",
            )
    record = {
        "name": name,
        "sshTarget": ssh_target,
        "maxLanes": max_lanes,
        "agentId": me,
        "enrolledAt": existing.get("enrolledAt", time.time()) if existing else time.time(),
        "lastHeartbeat": time.time(),
        "alive": True,
    }
    if ssh_port is not None:
        record["sshPort"] = ssh_port
    if remote_msp is not None:
        record["remoteMsp"] = remote_msp
    elif existing and existing.get("remoteMsp"):
        record["remoteMsp"] = existing["remoteMsp"]
    if probe:
        record["musePath"] = probe.get("musePath", "")
        record["museVersion"] = probe.get("version", "")
    hosts[name] = record
    save_hosts(hosts)
    return record


def remove_host(name: str) -> dict[str, Any]:
    hosts = load_hosts()
    if name not in hosts:
        raise HostError("hostNotFound", f"unknown host: {name!r}")
    record = hosts.pop(name)
    save_hosts(hosts)
    return record


def heartbeat_host(name: str, alive: bool = True) -> dict[str, Any]:
    hosts = load_hosts()
    record = hosts.get(name)
    if record is None:
        raise HostError("hostNotFound", f"unknown host: {name!r}")
    record["alive"] = bool(alive)
    record["lastHeartbeat"] = time.time()
    hosts[name] = record
    save_hosts(hosts)
    return record


def split_lane_ref(reference: str) -> tuple[str | None, str]:
    """Split a lane reference into (host, alias).

    Unqualified aliases address this host's lanes; `host/alias` addresses a
    peer host's lane with no cross-host alias collisions. Raises HostError
    ("badHostName") on empty parts or extra separators.
    """
    if not isinstance(reference, str) or not reference:
        raise HostError("badHostName", f"invalid lane reference: {reference!r}")
    if "/" not in reference:
        return None, reference
    host, alias = reference.split("/", 1)
    if not host or not alias or "/" in alias:
        raise HostError(
            "badHostName",
            f"invalid lane reference {reference!r}: want 'alias' or 'host/alias'",
        )
    check_host_name(host)
    return host, alias


def qualify_lane(host: str, alias: str) -> str:
    """Namespaced lane identity: `host/alias`, unique across the federation."""
    check_host_name(host)
    if not alias or "/" in alias:
        raise HostError("badHostName", f"invalid lane alias: {alias!r}")
    return f"{host}/{alias}"


def peer_cli_argv(record: dict[str, Any], cli_args: list[str]) -> list[str]:
    """Argv invoking the peer agent's own m8s CLI over SSH (agent-to-agent).

    Never touches the peer's serve stdio — the peer agent owns its serve
    process and this side only speaks to its control CLI. Requires the
    host's `remoteMsp` path (never guessed: remote home layouts are unknown).
    """
    remote = record.get("remoteMsp")
    if not remote:
        raise HostError(
            "noRemoteMsp",
            f"host {record.get('name')!r} has no remoteMsp path; "
            "re-enroll with --remote-msp pointing at its muse-msp.py",
        )
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
    if record.get("sshPort") is not None:
        argv += ["-p", str(record["sshPort"])]
    argv += [record["sshTarget"], "--", "python3", remote, *cli_args]
    return argv


def place_lane(
    spec: dict[str, Any],
    hosts: dict[str, dict[str, Any]],
    lanes_by_host: dict[str, list[dict[str, Any]]],
    now: float | None = None,
) -> list[dict[str, Any]]:
    """Rank enrolled hosts for a lane spec (pure placement function).

    Considers, per docs/moonshot.md Scheduler row: collision zone (shared
    branch or overlapping files lose big), checkout affinity (a host already
    holding this checkout wins), and capacity (full or dead hosts are out;
    free slots score). Returns [{host, score, reasons}] best-first; empty
    when nothing can take the lane.
    """
    at = time.time() if now is None else now
    branch = spec.get("branch")
    files = [str(f) for f in (spec.get("files") or [])]
    checkout = spec.get("checkout")
    ranked: list[dict[str, Any]] = []
    for name in sorted(hosts):
        record = hosts[name]
        reasons: list[str] = []
        if not host_alive(record, at):
            continue
        lanes = lanes_by_host.get(name, [])
        free = int(record.get("maxLanes", 0)) - len(lanes)
        if free <= 0:
            continue
        score = 10 * free
        reasons.append(f"capacity {len(lanes)}/{record.get('maxLanes')} ({free} free)")
        collision = None
        for lane in lanes:
            if branch and lane.get("branch") == branch:
                collision = f"branch {branch!r} already held on {name}"
                break
            for other in (lane.get("files") or []):
                other = str(other)
                if any(
                    f == other or f.startswith(other + "/") or other.startswith(f + "/")
                    for f in files
                ):
                    collision = f"file overlap {other!r} on {name}"
                    break
            if collision:
                break
        if collision:
            score -= 1000
            reasons.append(f"collision: {collision}")
        if checkout and any(lane.get("checkout") == checkout for lane in lanes):
            score += 50
            reasons.append(f"checkout affinity: {checkout} already on {name}")
        ranked.append({"host": name, "score": score, "reasons": reasons})
    ranked.sort(key=lambda item: (-item["score"], item["host"]))
    return ranked


def pick_host(
    spec: dict[str, Any],
    hosts: dict[str, dict[str, Any]],
    lanes_by_host: dict[str, list[dict[str, Any]]],
    now: float | None = None,
) -> dict[str, Any]:
    """Best placement for a lane spec, or HostError("noCapacity")."""
    ranked = place_lane(spec, hosts, lanes_by_host, now)
    if not ranked or ranked[0]["score"] < 0:
        raise HostError(
            "noCapacity",
            "no enrolled host can take the lane without a collision zone conflict",
        )
    return ranked[0]


# ---------------------------------------------------------------------------
# Moonshot P4: claim / heartbeat / intent bus between cooperative nodes.
#
# Transport choice: a NATS-subject-shaped file-backed local bus. Every bus
# record is {"subject", "message"} appended to bus.ndjson in the daemon
# runtime dir (the same pattern as hosts.json/budgets.json/events.ndjson),
# and the lease table lives in claims.json keyed by branch. Agent-to-agent
# gossip reuses the P3 SSH peer CLI (`peer_cli_argv`): a node runs
# `bus read` locally and replays it at the peer — no new carrier, no TCP
# listener (MSP stays stdio-only), stdlib only. Git-as-mailbox was
# considered and rejected: leases need second-granularity expiry and cheap
# heartbeats, and a commit/push per heartbeat would spam history and need
# the network. Graduation path: replace publish()/read_bus() with NATS
# publish/subscribe on the same subjects — every message already carries
# type+version, so that is a transport change, not a redesign.
# ---------------------------------------------------------------------------

BUS_SUBJECTS = ("m8s.claims", "m8s.heartbeat", "m8s.intent")

# Protocol version for every message type. A future v2 is a deliberate
# upgrade: validate_bus_message() rejects unknown versions outright.
BUS_PROTOCOL_VERSION = 1

CLAIM_MESSAGE_TYPE = "m8s.claim"
HEARTBEAT_MESSAGE_TYPE = "m8s.heartbeat"
INTENT_MESSAGE_TYPE = "m8s.intent"

SUBJECT_MESSAGE_TYPE = {
    "m8s.claims": CLAIM_MESSAGE_TYPE,
    "m8s.heartbeat": HEARTBEAT_MESSAGE_TYPE,
    "m8s.intent": INTENT_MESSAGE_TYPE,
}

CLAIMS_FILE = RUNTIME / "claims.json"
BUS_LOG = RUNTIME / "bus.ndjson"

# Branch-lease TTL. Overridable via M8S_LEASE_TTL_SECONDS; invalid values
# fall back to the default.
LEASE_TTL_SECONDS = 300


def lease_ttl_seconds() -> int:
    try:
        return max(1, int(os.environ.get("M8S_LEASE_TTL_SECONDS", LEASE_TTL_SECONDS)))
    except (TypeError, ValueError):
        return LEASE_TTL_SECONDS


def local_host() -> str:
    """Default claim/intent host: this machine's hostname."""
    import socket

    return socket.gethostname()


class BusError(ValueError):
    """Typed bus/lease error.

    ``kind`` is machine-readable: "badSubject", "badMessage", "badVersion",
    "leaseHeld", "checkoutBusy", "leaseNotFound", "notOwner".
    """

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def make_claim(
    host: str,
    lane: str,
    branch: str,
    checkout: str | None = None,
    session_id: str | None = None,
    ttl: int | None = None,
    now: float | None = None,
    claim_id: str | None = None,
) -> dict[str, Any]:
    """Build a versioned ownership claim ("host B holds lane X on branch Y").

    Protocol-shaped and transport-independent: plain JSON with type+version
    fields, publishable on subject ``m8s.claims`` via any carrier.
    """
    at = time.time() if now is None else now
    ttl_s = lease_ttl_seconds() if ttl is None else ttl
    claim: dict[str, Any] = {
        "type": CLAIM_MESSAGE_TYPE,
        "version": BUS_PROTOCOL_VERSION,
        "claimId": claim_id or uuid7(),
        "host": host,
        "lane": lane,
        "branch": branch,
        "issuedAt": at,
        "expiresAt": at + ttl_s,
    }
    if checkout is not None:
        claim["checkout"] = checkout
    if session_id is not None:
        claim["sessionId"] = session_id
    return claim


def make_heartbeat(
    host: str,
    agent: str,
    claims: list[dict[str, Any]],
    now: float | None = None,
) -> dict[str, Any]:
    """Build a versioned heartbeat re-gossiping this host's ownership claims.

    Publishable on subject ``m8s.heartbeat``; the embedded claims are the
    re-gossip (presence broadcast), not a second liveness feed — host
    liveness itself still comes from the P3 registry heartbeat.
    """
    return {
        "type": HEARTBEAT_MESSAGE_TYPE,
        "version": BUS_PROTOCOL_VERSION,
        "host": host,
        "agentId": agent,
        "at": time.time() if now is None else now,
        "claims": claims,
    }


def make_intent(
    host: str,
    verb: str,
    lane: str | None = None,
    branch: str | None = None,
    detail: str | None = None,
    session_id: str | None = None,
    ttl: int | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Build a versioned intent message, publishable on ``m8s.intent``."""
    at = time.time() if now is None else now
    ttl_s = lease_ttl_seconds() if ttl is None else ttl
    intent: dict[str, Any] = {
        "type": INTENT_MESSAGE_TYPE,
        "version": BUS_PROTOCOL_VERSION,
        "intentId": uuid7(),
        "host": host,
        "verb": verb,
        "at": at,
        "expiresAt": at + ttl_s,
    }
    if lane is not None:
        intent["lane"] = lane
    if branch is not None:
        intent["branch"] = branch
    if detail is not None:
        intent["detail"] = detail
    if session_id is not None:
        intent["sessionId"] = session_id
    return intent


BUS_REQUIRED_FIELDS = {
    "m8s.claims": ("claimId", "host", "lane", "branch", "issuedAt", "expiresAt"),
    "m8s.heartbeat": ("host", "agentId", "at", "claims"),
    "m8s.intent": ("intentId", "host", "verb", "at"),
}


def validate_bus_message(subject: str, message: Any) -> None:
    """Validate a bus message against its subject's protocol shape.

    Raises BusError ("badSubject" / "badMessage" / "badVersion"). Pure check:
    performs no I/O.
    """
    if subject not in BUS_SUBJECTS:
        raise BusError(
            "badSubject",
            f"unknown bus subject {subject!r}: want one of {list(BUS_SUBJECTS)}",
        )
    if not isinstance(message, dict):
        raise BusError("badMessage", f"bus message on {subject} must be an object")
    want = SUBJECT_MESSAGE_TYPE[subject]
    if message.get("type") != want:
        raise BusError(
            "badMessage",
            f"subject {subject} carries {want}, got {message.get('type')!r}",
        )
    if message.get("version") != BUS_PROTOCOL_VERSION:
        raise BusError(
            "badVersion",
            f"unsupported {want} version {message.get('version')!r}: "
            f"this bus speaks version {BUS_PROTOCOL_VERSION}",
        )
    missing = [key for key in BUS_REQUIRED_FIELDS[subject] if message.get(key) in (None, "")]
    if missing:
        raise BusError(
            "badMessage", f"{want} message missing required fields: {missing}"
        )


def publish(subject: str, message: dict[str, Any]) -> dict[str, Any]:
    """Append a validated message to the local bus log. Returns the message.

    The transport primitive: graduating to NATS later means swapping this
    append (and read_bus) for publish/subscribe on the same subjects —
    message shapes do not change.
    """
    validate_bus_message(subject, message)
    with BUS_LOG.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps({"subject": subject, "message": message}, separators=(",", ":"))
            + "\n"
        )
    return message


def read_bus(
    subject: str | None = None, limit: int = 200, after: float = 0.0
) -> list[dict[str, Any]]:
    """Read bus records, optionally filtered by subject and message time."""
    if subject is not None and subject not in BUS_SUBJECTS:
        raise BusError(
            "badSubject",
            f"unknown bus subject {subject!r}: want one of {list(BUS_SUBJECTS)}",
        )
    if not BUS_LOG.exists():
        return []
    records: list[dict[str, Any]] = []
    with BUS_LOG.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if subject is not None and record.get("subject") != subject:
                continue
            message = record.get("message") or {}
            stamp = message.get("issuedAt", message.get("at", 0))
            if isinstance(stamp, (int, float)) and stamp > after:
                records.append(record)
    return records[-limit:]


def load_claims() -> dict[str, dict[str, Any]]:
    try:
        raw = json.loads(CLAIMS_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def save_claims(claims: dict[str, dict[str, Any]]) -> None:
    CLAIMS_FILE.write_text(json.dumps(claims, sort_keys=True), encoding="utf-8")


def claim_live(
    claim: dict[str, Any], now: float, hosts: dict[str, dict[str, Any]] | None = None
) -> bool:
    """A branch lease is live while unexpired — and while its host is live.

    Host liveness reuses the P3 registry feed: a claim whose enrolled host
    reads dead is treated as expired (reassignable), so a crashed owner
    cannot squat a branch past its heartbeat. Claims from hosts unknown to
    the registry fall back to wall-clock expiry alone.
    """
    expires = claim.get("expiresAt")
    if not isinstance(expires, (int, float)) or now >= expires:
        return False
    if hosts is None:
        return True
    record = hosts.get(claim.get("host", ""))
    if record is None:
        return True
    return host_alive(record, now, host_ttl_seconds())


def claim_branch(
    host: str,
    lane: str,
    branch: str,
    checkout: str | None = None,
    session_id: str | None = None,
    ttl: int | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Acquire (or refresh) a branch-level lease: one lane per branch, one
    writer per checkout.

    Re-claiming your own (host, lane, branch) refreshes the lease (this is
    the heartbeat path). A live foreign lease on the branch raises
    BusError("leaseHeld"); a live foreign lease on the checkout raises
    BusError("checkoutBusy"). Expired (or dead-host) leases are
    reassigned — requeue on expiry. Publishes the claim on ``m8s.claims``.
    """
    if not host or not lane or not branch:
        raise BusError("badMessage", "claim requires non-empty host, lane, and branch")
    if ttl is not None and (
        isinstance(ttl, bool) or not isinstance(ttl, int) or ttl < 1
    ):
        raise BusError("badMessage", f"claim ttl must be a positive integer: {ttl!r}")
    at = time.time() if now is None else now
    claims = load_claims()
    hosts = load_hosts()
    for other_branch, existing in claims.items():
        if not claim_live(existing, at, hosts):
            continue
        if other_branch == branch and (existing.get("host"), existing.get("lane")) != (
            host,
            lane,
        ):
            raise BusError(
                "leaseHeld",
                f"branch {branch!r} held by {existing.get('host')}/{existing.get('lane')} "
                f"until {existing.get('expiresAt')}",
            )
        if (
            checkout
            and existing.get("checkout")
            and existing["checkout"] == checkout
            and (existing.get("host"), existing.get("lane")) != (host, lane)
        ):
            raise BusError(
                "checkoutBusy",
                f"checkout {checkout!r} has its writer: "
                f"{existing.get('host')}/{existing.get('lane')} on {other_branch!r}",
            )
    previous = claims.get(branch)
    keep_id = (
        previous.get("claimId")
        if previous and (previous.get("host"), previous.get("lane")) == (host, lane)
        else None
    )
    claim = make_claim(host, lane, branch, checkout, session_id, ttl, at, keep_id)
    claims[branch] = claim
    save_claims(claims)
    publish("m8s.claims", claim)
    return claim


def refresh_claims(
    host: str, now: float | None = None, ttl: int | None = None
) -> list[dict[str, Any]]:
    """Heartbeat for one host's leases: extend expiry and re-gossip each
    claim on ``m8s.claims``. Returns the refreshed claims (empty when the
    host holds none — a heartbeat that gossips nothing publishes nothing).
    """
    at = time.time() if now is None else now
    ttl_s = lease_ttl_seconds() if ttl is None else ttl
    claims = load_claims()
    refreshed: list[dict[str, Any]] = []
    for branch, claim in claims.items():
        if claim.get("host") != host:
            continue
        claim["expiresAt"] = at + ttl_s
        refreshed.append(claim)
    if refreshed:
        save_claims(claims)
        for claim in refreshed:
            publish("m8s.claims", claim)
    return refreshed


def release_claim(branch: str, host: str | None = None) -> dict[str, Any]:
    """Release a branch lease. With ``host`` given, only that host's claim
    may be released (BusError "notOwner" otherwise); unknown branches raise
    BusError("leaseNotFound")."""
    claims = load_claims()
    claim = claims.get(branch)
    if claim is None:
        raise BusError("leaseNotFound", f"no claim on branch {branch!r}")
    if host is not None and claim.get("host") != host:
        raise BusError(
            "notOwner",
            f"branch {branch!r} is claimed by {claim.get('host')!r}, not {host!r}",
        )
    del claims[branch]
    save_claims(claims)
    return claim


def sweep_claims(
    now: float | None = None, hosts: dict[str, dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Report newly-expired leases (requeue on expiry) exactly once each.

    Returns the expired claims not previously announced and stamps them so
    the next sweep stays quiet; callers record the ``claim.expired`` events.
    Expired claims stay in the table (visible requeue state) until released
    or reassigned.
    """
    at = time.time() if now is None else now
    if hosts is None:
        hosts = load_hosts()
    claims = load_claims()
    newly: list[dict[str, Any]] = []
    changed = False
    for claim in claims.values():
        if not claim_live(claim, at, hosts) and not claim.get("expiryAnnounced"):
            claim["expiryAnnounced"] = True
            newly.append(claim)
            changed = True
    if changed:
        save_claims(claims)
    return newly


def claim_table(
    now: float | None = None, hosts: dict[str, dict[str, Any]] | None = None
) -> dict[str, dict[str, Any]]:
    """Lease table: every claim annotated with live/requeue lease state."""
    at = time.time() if now is None else now
    if hosts is None:
        try:
            hosts = load_hosts()
        except (OSError, ValueError):
            hosts = {}
    table: dict[str, dict[str, Any]] = {}
    try:
        claims = load_claims()
    except (OSError, ValueError):
        claims = {}
    for branch, claim in claims.items():
        live = claim_live(claim, at, hosts)
        table[branch] = {**claim, "live": live, "requeue": not live}
    return table


# ---------------------------------------------------------------------------
# Moonshot P5: board-to-lane compiler (desired state -> lane specs).
#
# The compiler reads a file-based board snapshot (issues + milestones) and
# emits lane specs with deterministic priority arbitration, collision
# queuing on top of the P4 branch-lease/claim table (no parallel ownership
# system), and decision-blocked stops that page a human instead of
# guessing. A `gh`-driven exporter builds the snapshot read-only; the
# compiler itself never touches the network. All `board` commands run
# daemonless (they read the same file-backed state `list`/`events` show),
# so planning never needs the controller socket.
# ---------------------------------------------------------------------------

BOARD_SCHEMA_VERSION = 1

# Priority label -> weight. `p0..p3` are accepted as aliases for the
# critical..low scale; anything else weighs 1 (unprioritized, not ignored).
PRIORITY_WEIGHTS = {"critical": 4, "high": 3, "medium": 2, "low": 1}

# Staleness divisor: score grows by 1/30 per idle day, so a month-old
# issue at the same priority/depth outranks a fresh one without
# ever dwarfing the priority term.
STALENESS_DIVISOR_DAYS = 30.0

# Decision classes that stop a lane and page a human (moonshot
# stewardship). "spend" reuses the P2 budget-breaching decisionClass value
# so paging surfaces stay uniform.
DECISION_CLASSES = ("product", "auth", "spend")

DECISION_LABELS = {
    "product": (
        "decision-needed",
        "needs-decision",
        "needs-product-call",
        "product-call",
    ),
    "auth": ("needs-auth", "auth-boundary", "needs-credentials", "needs-access"),
    "spend": ("needs-spend", "spend", "needs-budget", "over-budget"),
}

DECISION_BODY_MARKERS = (
    "decision required",
    "needs product call",
    "waiting on human",
    "needs owner",
    "auth boundary",
)


# Host capacity planning (issue #19): a single host holds ~20-32 owned
# sessions before `session/start` begins rejecting with loaded-session
# capacity exhausted (retryable). The ceiling below is the dispatch-side
# guardrail — deliberately under the observed failure band — and every
# value is overridable per host without a code change.
HOST_SESSION_CEILING_DEFAULT = 20

# Pre-ceiling alert fraction: warn when loaded reaches this share of the
# ceiling so supervisors shed load before launches start failing.
HOST_SESSION_WARN_FRACTION = 0.75

CAPACITY_POSTURES = ("open", "limited", "closed")

# Surfaced event kind for the pre-ceiling alert (signal, not rejection).
CAPACITY_WARNING_KIND = "host.capacity.warning"

# Standby window before an idle lane may unload: duty-complete lanes stay
# resident this long for review/CI follow-up, then become unload-eligible.
UNLOAD_STANDBY_SECONDS = 30 * 60


def session_ceiling(raw: Any = None) -> int:
    """Owned-session ceiling for this host. Explicit ``raw`` wins, then
    ``M8S_SESSION_CEILING``, then the default. Invalid values fall back."""
    if raw is None:
        raw = os.environ.get("M8S_SESSION_CEILING")
    try:
        ceiling = int(raw) if raw is not None else HOST_SESSION_CEILING_DEFAULT
    except (TypeError, ValueError):
        return HOST_SESSION_CEILING_DEFAULT
    return max(1, ceiling)


def capacity_warn_at(ceiling: int, raw: Any = None) -> int:
    """Pre-ceiling alert threshold for ``ceiling`` loaded sessions. Explicit
    ``raw`` wins, then ``M8S_SESSION_WARN_AT``, then the warn fraction of
    the ceiling. Clamped to ``1..ceiling``; invalid values fall back."""
    if raw is None:
        raw = os.environ.get("M8S_SESSION_WARN_AT")
    try:
        warn_at = int(raw) if raw is not None else None
    except (TypeError, ValueError):
        warn_at = None
    if warn_at is None:
        import math

        warn_at = max(1, math.ceil(ceiling * HOST_SESSION_WARN_FRACTION))
    return max(1, min(int(ceiling), warn_at))


def capacity_loaded_from_claims(
    claims: dict[str, dict[str, Any]] | None,
) -> int:
    """Daemonless loaded-session proxy: live P4 leases with a session id.

    ``board plan``/``reconcile`` run without the controller socket, so the
    true roster count is unreachable; each live lease holds a session, which
    makes this a conservative floor. Pass ``--loaded`` for the true count.
    """
    count = 0
    for claim in (claims or {}).values():
        if isinstance(claim, dict) and claim.get("live", True) and claim.get("sessionId"):
            count += 1
    return count


def capacity_posture(
    loaded: int,
    ceiling: int | None = None,
    warn_at: int | None = None,
) -> dict[str, Any]:
    """Admission posture from loaded sessions vs ceiling (pure function).

    ``open`` (below the warn threshold): propose freely. ``limited`` (warn
    threshold reached, ceiling not): admit at most one new lane per pass —
    supervisors should already be shedding load. ``closed`` (at/above the
    ceiling): refuse every new lane until a slot frees. Every posture cites
    its numbers in ``evidence`` so a gated lane can say exactly why.
    """
    try:
        loaded_count = max(0, int(loaded))
    except (TypeError, ValueError):
        raise BoardError("badCapacity", f"loaded session count must be an integer: {loaded!r}")
    ceiling_count = session_ceiling(ceiling)
    warn_count = capacity_warn_at(ceiling_count, warn_at)
    if loaded_count >= ceiling_count:
        posture = "closed"
    elif loaded_count >= warn_count:
        posture = "limited"
    else:
        posture = "open"
    return {
        "loaded": loaded_count,
        "ceiling": ceiling_count,
        "warnAt": warn_count,
        "posture": posture,
        "evidence": (
            f"{loaded_count} loaded session(s) vs ceiling {ceiling_count} "
            f"(warn at {warn_count}): admission {posture}"
        ),
    }


def unload_standby_seconds(raw: Any = None) -> int:
    """Unload standby window. Explicit ``raw`` wins, then
    ``M8S_UNLOAD_STANDBY_SECONDS``, then the default. Invalid falls back."""
    if raw is None:
        raw = os.environ.get("M8S_UNLOAD_STANDBY_SECONDS")
    try:
        seconds = int(raw) if raw is not None else UNLOAD_STANDBY_SECONDS
    except (TypeError, ValueError):
        return UNLOAD_STANDBY_SECONDS
    return max(1, seconds)


class BoardError(ValueError):
    """Typed board-compiler error. ``kind`` is machine-readable."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def board_priority(labels: list[str]) -> tuple[int, str]:
    """Weight an issue's priority labels. Highest label wins; default 1."""
    best = 1
    reason = "no priority label (weight 1)"
    for label in labels:
        name = label.strip().lower()
        if name.startswith("priority-"):
            name = name[len("priority-"):]
        weight = PRIORITY_WEIGHTS.get(name)
        if weight is None and len(name) == 2 and name[0] == "p" and name[1] in "0123":
            weight = 4 - int(name[1])
            name = {"p0": "critical", "p1": "high", "p2": "medium", "p3": "low"}[name]
        if weight is not None and weight > best:
            best = weight
            reason = f"label {label!r} (weight {weight})"
    return best, reason


def board_dependency_depth(issues: list[dict[str, Any]]) -> dict[int, int]:
    """Unblock depth per open issue number: longest chain of open issues
    transitively depending on it (0 when nothing depends on it). Blockers
    therefore outrank the blocked, which keeps scheduling order
    dependency-safe. Cycles resolve deterministically (back edge scores 0)
    and are reported by :func:`board_dependency_cycles`."""
    open_numbers = {
        int(item["number"]) for item in issues if item.get("state", "open") == "open"
    }
    dependents: dict[int, list[int]] = {number: [] for number in open_numbers}
    for item in issues:
        number = int(item["number"])
        if number not in open_numbers:
            continue
        for dep in item.get("dependsOn") or []:
            try:
                dep_number = int(dep)
            except (TypeError, ValueError):
                continue
            if dep_number in dependents and number != dep_number:
                dependents[dep_number].append(number)
    depth: dict[int, int] = {}

    def visit(number: int, trail: tuple[int, ...]) -> int:
        if number in depth:
            return depth[number]
        best = 0
        for child in sorted(dependents.get(number, ())):
            if child in trail:
                continue
            best = max(best, 1 + visit(child, trail + (number,)))
        if number not in trail:
            depth[number] = best
        return best

    for number in sorted(open_numbers):
        visit(number, ())
    return depth


def board_dependency_cycles(issues: list[dict[str, Any]]) -> list[list[int]]:
    """Closed dependsOn loops among open issues (each sorted, list sorted)."""
    open_numbers = {
        int(item["number"]) for item in issues if item.get("state", "open") == "open"
    }
    edges: dict[int, list[int]] = {}
    for item in issues:
        number = int(item["number"])
        if number not in open_numbers:
            continue
        targets = set()
        for dep in item.get("dependsOn") or []:
            try:
                dep_number = int(dep)
            except (TypeError, ValueError):
                continue
            if dep_number in open_numbers and dep_number != number:
                targets.add(dep_number)
        edges[number] = sorted(targets)
    cycles: set[tuple[int, ...]] = set()
    for start in sorted(edges):
        stack: list[tuple[int, list[int]]] = [(start, [start])]
        while stack:
            node, path = stack.pop()
            for target in edges.get(node, ()):
                if target == start and len(path) > 1:
                    cycles.add(tuple(sorted(path)))
                elif target not in path and target > start:
                    stack.append((target, path + [target]))
    return [list(cycle) for cycle in sorted(cycles)]


def parse_board_time(value: Any) -> float | None:
    """Epoch seconds from a snapshot timestamp: epoch numbers as-is,
    ISO-8601 strings (the ``gh`` shape) parsed, anything else None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            pass
        try:
            from datetime import datetime, timezone

            moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
            return moment.timestamp()
        except ValueError:
            return None
    return None


def board_staleness(updated_at: Any, now: float) -> tuple[float, float]:
    """(idle days, staleness factor 1 + days/30). Unparseable timestamps
    read as fresh (0 days) rather than failing the whole plan."""
    parsed = parse_board_time(updated_at)
    idle = max(0.0, (now - parsed) / 86400.0) if parsed is not None else 0.0
    return idle, 1.0 + idle / STALENESS_DIVISOR_DAYS


def classify_decision(issue: dict[str, Any]) -> dict[str, str] | None:
    """Decision-blocked classification, or None when the lane may proceed.

    Signals: an explicit ``decisionBlocked: {class, reason}`` field on the
    snapshot issue, decision labels (see DECISION_LABELS), or body markers
    (see DECISION_BODY_MARKERS). Explicit field beats labels beats markers;
    the first matching class in DECISION_CLASSES order wins.
    """
    explicit = issue.get("decisionBlocked")
    if isinstance(explicit, dict) and explicit.get("class") in DECISION_CLASSES:
        return {
            "class": str(explicit["class"]),
            "reason": str(explicit.get("reason") or "marked decision-blocked"),
        }
    labels = {str(label).strip().lower() for label in (issue.get("labels") or [])}
    for cls in DECISION_CLASSES:
        for marker in DECISION_LABELS[cls]:
            if marker in labels:
                return {"class": cls, "reason": f"label {marker!r}"}
    body = str(issue.get("body") or "").lower()
    for marker in DECISION_BODY_MARKERS:
        if marker in body:
            for cls in DECISION_CLASSES:
                if cls in marker or (cls == "product" and "product" in marker):
                    return {"class": cls, "reason": f"body marker {marker!r}"}
            return {"class": "product", "reason": f"body marker {marker!r}"}
    return None


def slugify(text: str, words: int = 5) -> str:
    """Lowercase alphanumeric slug of the first few title words."""
    import re

    parts = re.findall(r"[a-z0-9]+", text.lower())[:words]
    return "-".join(parts) or "untitled"


def scope_overlap(first: dict[str, Any], second: dict[str, Any]) -> str | None:
    """Collision-zone overlap between two lane scopes (P3 rule reused):
    same branch, same checkout, or overlapping files (equal or nested).
    Returns a human-readable reason or None."""
    if first.get("branch") and first.get("branch") == second.get("branch"):
        return f"branch {first['branch']!r}"
    if first.get("checkout") and first.get("checkout") == second.get("checkout"):
        return f"checkout {first['checkout']!r}"
    first_files = [str(f) for f in (first.get("files") or [])]
    second_files = [str(f) for f in (second.get("files") or [])]
    for mine in first_files:
        for other in second_files:
            if mine == other or mine.startswith(other + "/") or other.startswith(mine + "/"):
                return f"file overlap {mine!r} vs {other!r}"
    return None


def compile_board(
    board: dict[str, Any], now: float | None = None
) -> dict[str, Any]:
    """Compile a board snapshot into ranked lane specs (pure function).

    Score is deterministic: ``priority x (1 + dependency depth) x
    staleness``; ties break by issue number. Every spec cites its
    components in ``priority.why`` so a scheduled lane can say why it was
    scheduled. Closed issues are skipped; decision-blocked issues compile
    to ``status: decision-blocked`` and never schedule.
    """
    at = time.time() if now is None else now
    raw_issues = board.get("issues") or []
    if not isinstance(raw_issues, list):
        raise BoardError("badBoard", "board snapshot needs an 'issues' list")
    issues = [item for item in raw_issues if isinstance(item, dict)]
    depth = board_dependency_depth(issues)
    cycles = board_dependency_cycles(issues)
    open_blockers: dict[int, list[int]] = {}
    open_numbers = {
        int(item["number"]) for item in issues if item.get("state", "open") == "open"
    }
    for item in issues:
        number = int(item.get("number", 0))
        blockers = []
        for dep in item.get("dependsOn") or []:
            try:
                dep_number = int(dep)
            except (TypeError, ValueError):
                continue
            if dep_number in open_numbers and dep_number != number:
                blockers.append(dep_number)
        open_blockers[number] = sorted(blockers)
    specs: list[dict[str, Any]] = []
    for item in issues:
        if item.get("state", "open") != "open":
            continue
        try:
            number = int(item["number"])
        except (TypeError, ValueError, KeyError) as exc:
            raise BoardError("badBoard", f"issue needs an integer number: {exc}")
        labels = [str(label) for label in (item.get("labels") or [])]
        weight, weight_why = board_priority(labels)
        issue_depth = depth.get(number, 0)
        idle_days, stale_factor = board_staleness(item.get("updatedAt"), at)
        score = round(weight * (1 + issue_depth) * stale_factor, 3)
        scope = item.get("scope") or {}
        branch = scope.get("branch") or f"lane/m8s-{number}-{slugify(str(item.get('title', '')))}"
        title = str(item.get("title") or f"issue #{number}")
        body = str(item.get("body") or "").strip()
        brief = body.splitlines()[0].strip() if body else title
        blocked = classify_decision(item)
        spec: dict[str, Any] = {
            "lane": f"m8s-{number}",
            "issue": number,
            "title": title,
            "branch": branch,
            "checkout": scope.get("checkout"),
            "files": [str(f) for f in (scope.get("files") or [])],
            "brief": brief[:280],
            "priority": {
                "score": score,
                "weight": weight,
                "depth": issue_depth,
                "stalenessDays": round(idle_days, 2),
                "why": [
                    weight_why,
                    f"dependency depth {issue_depth}",
                    f"stale {idle_days:.1f}d (factor {stale_factor:.3f})",
                ],
            },
            "blockedBy": open_blockers.get(number, []),
            "status": "candidate",
        }
        if blocked is not None:
            spec["status"] = "decision-blocked"
            spec["decisionBlocked"] = blocked
            spec["pageHuman"] = True
        specs.append(spec)
    specs.sort(key=lambda spec: (-spec["priority"]["score"], spec["issue"]))
    return {
        "schemaVersion": BOARD_SCHEMA_VERSION,
        "repo": board.get("repo"),
        "compiledAt": at,
        "dependencyCycles": cycles,
        "specs": specs,
    }


def apply_collisions(
    specs: list[dict[str, Any]],
    live_claims: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Collision pass over ranked specs (pure function). The first
    schedulable spec in rank order wins each collision zone; later specs
    overlapping an already-scheduled spec — or a live P4 lease on the
    branch/checkout — queue with ``queuedBehind`` citing the winner.
    Decision-blocked specs pass through untouched (they never schedule).
    """
    claims = live_claims or {}
    live_by_branch = {
        branch: claim
        for branch, claim in claims.items()
        if claim.get("live", True)
    }
    live_checkouts = {
        str(claim.get("checkout")): claim
        for claim in live_by_branch.values()
        if claim.get("checkout")
    }
    scheduled: list[dict[str, Any]] = []
    for spec in specs:
        if spec.get("status") == "decision-blocked":
            continue
        conflict: str | None = None
        holder = live_by_branch.get(spec.get("branch", ""))
        if holder is not None and holder.get("lane") != spec.get("lane"):
            conflict = (
                f"branch {spec['branch']!r} leased to "
                f"{holder.get('host')}/{holder.get('lane')}"
            )
        if conflict is None and spec.get("checkout"):
            busy = live_checkouts.get(str(spec["checkout"]))
            if busy is not None and busy.get("lane") != spec.get("lane"):
                conflict = (
                    f"checkout {spec['checkout']!r} has its writer: "
                    f"{busy.get('host')}/{busy.get('lane')}"
                )
        if conflict is None:
            for winner in scheduled:
                overlap = scope_overlap(spec, winner)
                if overlap is not None:
                    conflict = (
                        f"scope collision ({overlap}) with "
                        f"scheduled issue #{winner['issue']}"
                    )
                    break
        if conflict is None:
            spec["status"] = "scheduled"
            scheduled.append(spec)
        else:
            spec["status"] = "queued"
            spec["queuedBehind"] = conflict
    return specs


def apply_capacity_gate(
    specs: list[dict[str, Any]],
    capacity: dict[str, Any],
    live_claims: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Load-based dispatch gate over ranked specs (pure function, issue #19).

    Past the gate no new lane proposes: under a ``closed`` posture every
    ``scheduled`` spec parks as ``queued``; under ``limited`` only the
    top-ranked scheduled spec keeps its slot and the rest park. Parked
    specs cite the capacity evidence in ``queuedBehind`` (loaded vs ceiling
    with the posture) and carry ``gatedByCapacity``, so the refusal always
    says why and what frees it. The gate stops *new* admissions only: a
    spec already holding its branch lease stays scheduled (reconcile still
    maps it to ``in-sync``). Collision-queued and decision-blocked specs
    pass through untouched.
    """
    posture = capacity.get("posture", "open")
    if posture == "open":
        return specs
    claims = live_claims or {}
    admitted = 0
    for spec in specs:
        if spec.get("status") != "scheduled":
            continue
        lease = claims.get(spec.get("branch", "")) or {}
        if lease.get("live") and lease.get("lane") == spec.get("lane"):
            continue
        if posture == "limited" and admitted == 0:
            admitted += 1
            continue
        spec["status"] = "queued"
        spec["gatedByCapacity"] = True
        spec["queuedBehind"] = (
            f"capacity gate ({posture}): {capacity.get('evidence')}; "
            "parked until a slot frees — unload idle lanes per docs/capacity.md"
        )
    return specs


def plan_board(
    board: dict[str, Any],
    live_claims: dict[str, dict[str, Any]] | None = None,
    now: float | None = None,
    loaded: int | None = None,
    ceiling: int | None = None,
    warn_at: int | None = None,
) -> dict[str, Any]:
    """Full planning pass: compile, collide, then gate on host capacity.

    ``live_claims`` defaults to the real P4 lease table (reuse, not a
    parallel ownership system); pass an explicit table (or {}) in tests and
    dry runs. ``loaded`` defaults to the live-lease count (a conservative
    daemonless floor — pass the true roster count via ``--loaded``);
    ``ceiling``/``warn_at`` default to the host policy with env overrides.
    """
    at = time.time() if now is None else now
    compiled = compile_board(board, at)
    if live_claims is None:
        try:
            live_claims = claim_table(at)
        except (OSError, ValueError):
            live_claims = {}
    compiled["specs"] = apply_collisions(compiled["specs"], live_claims)
    if loaded is None:
        loaded = capacity_loaded_from_claims(live_claims)
    capacity = capacity_posture(loaded, ceiling, warn_at)
    compiled["specs"] = apply_capacity_gate(compiled["specs"], capacity, live_claims)
    compiled["capacity"] = capacity
    return compiled


def load_board_snapshot(path: str) -> dict[str, Any]:
    """Read and validate a board snapshot file."""
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise BoardError("badBoard", f"board snapshot not found: {path}")
    except (OSError, ValueError) as exc:
        raise BoardError("badBoard", f"board snapshot unreadable: {exc}")
    if not isinstance(raw, dict):
        raise BoardError("badBoard", "board snapshot must be a JSON object")
    if not isinstance(raw.get("issues"), list):
        raise BoardError("badBoard", "board snapshot needs an 'issues' list")
    return raw


def export_board_snapshot(
    repo: str, run: Any = None, limit: int = 100
) -> dict[str, Any]:
    """Build a board snapshot with read-only ``gh`` calls (issues plus
    milestones). ``run`` injects the subprocess runner (tests); default is
    ``subprocess.run``. Never writes to GitHub — the coordinator owns
    board writes.

    Projects v2 board columns are deliberately NOT read here: the v2 API
    is GraphQL-only with cursor pagination that fits poorly behind
    stdlib/gh one-liners, so the file snapshot is the compiler's input
    contract and project-column state travels as issue labels/milestones.
    """
    import subprocess

    runner = run or subprocess.run

    def gh(*argv: str) -> Any:
        proc = runner(
            ["gh", *argv], capture_output=True, text=True, timeout=60
        )
        if proc.returncode != 0:
            raise BoardError(
                "ghFailed", f"gh {' '.join(argv)} failed: {proc.stderr.strip()}"
            )
        try:
            return json.loads(proc.stdout or "null")
        except ValueError as exc:
            raise BoardError("ghFailed", f"gh output unparseable: {exc}")

    raw_issues = gh(
        "issue",
        "list",
        "--repo",
        repo,
        "--state",
        "open",
        "--limit",
        str(limit),
        "--json",
        "number,title,labels,milestone,updatedAt,body",
    )
    try:
        raw_milestones = gh(
            "api", f"repos/{repo}/milestones?state=open&per_page=100"
        )
    except BoardError:
        raw_milestones = []
    milestones = []
    if isinstance(raw_milestones, list):
        for milestone in raw_milestones:
            if isinstance(milestone, dict):
                milestones.append(
                    {
                        "number": milestone.get("number"),
                        "title": milestone.get("title"),
                        "dueOn": milestone.get("due_on"),
                    }
                )
    issues = []
    for entry in raw_issues if isinstance(raw_issues, list) else []:
        if not isinstance(entry, dict):
            continue
        labels = entry.get("labels") or []
        names = [
            label.get("name") if isinstance(label, dict) else label
            for label in labels
        ]
        milestone = entry.get("milestone") or {}
        issues.append(
            {
                "number": entry.get("number"),
                "title": entry.get("title"),
                "labels": [str(name) for name in names if name],
                "milestone": milestone.get("title") if isinstance(milestone, dict) else None,
                "state": "open",
                "updatedAt": entry.get("updatedAt"),
                "body": entry.get("body"),
            }
        )
    return {
        "schemaVersion": BOARD_SCHEMA_VERSION,
        "repo": repo,
        "exportedAt": time.time(),
        "milestones": milestones,
        "issues": issues,
    }


def board_reconcile(
    board: dict[str, Any],
    live_claims: dict[str, dict[str, Any]] | None = None,
    recent_events: list[dict[str, Any]] | None = None,
    now: float | None = None,
    loaded: int | None = None,
    ceiling: int | None = None,
    warn_at: int | None = None,
) -> dict[str, Any]:
    """Drive actual lane state toward the planned desired state (pure core).

    Compares the plan against live P4 leases and recent supervision events
    (the same state ``list``/``events`` show: stuck lanes, spend breaches,
    expired leases). Emits one action per spec — ``propose`` (free to
    schedule: operator runs ``bus claim`` + ``launch``), ``in-sync``
    (already leased), ``queued`` (collision or capacity gate),
    ``page-human`` (decision-blocked) — plus ``requeue`` entries for
    expired leases on desired branches and an ``attention`` list for
    stuck/over-budget signals. Past the capacity gate no new lane
    proposes: gated specs queue with the loaded-vs-ceiling evidence cited.
    Reconcile never claims, launches, or guesses: it proposes and pages.
    """
    at = time.time() if now is None else now
    plan = plan_board(board, live_claims, at, loaded, ceiling, warn_at)
    claims = live_claims if live_claims is not None else {}
    actions: list[dict[str, Any]] = []
    for spec in plan["specs"]:
        if spec.get("status") == "decision-blocked":
            actions.append(
                {
                    "lane": spec["lane"],
                    "issue": spec["issue"],
                    "action": "page-human",
                    "decisionClass": spec["decisionBlocked"]["class"],
                    "reason": spec["decisionBlocked"]["reason"],
                }
            )
        elif spec.get("status") == "queued":
            actions.append(
                {
                    "lane": spec["lane"],
                    "issue": spec["issue"],
                    "action": "queued",
                    "reason": spec.get("queuedBehind"),
                }
            )
        else:
            lease = claims.get(spec.get("branch", "")) or {}
            if lease.get("live"):
                actions.append(
                    {
                        "lane": spec["lane"],
                        "issue": spec["issue"],
                        "action": "in-sync",
                        "reason": (
                            f"leased to {lease.get('host')}/{lease.get('lane')}"
                        ),
                    }
                )
            else:
                actions.append(
                    {
                        "lane": spec["lane"],
                        "issue": spec["issue"],
                        "action": "propose",
                        "branch": spec["branch"],
                        "brief": spec["brief"],
                        "priority": spec["priority"],
                        "reason": (
                            "no live lease on branch; run `bus claim` + `launch`"
                        ),
                    }
                )
    requeue = sorted(
        {
            branch
            for branch, claim in claims.items()
            if not claim.get("live", True)
            and any(spec.get("branch") == branch for spec in plan["specs"])
        }
    )
    attention: list[dict[str, Any]] = []
    for event in recent_events or []:
        kind = event.get("kind")
        if kind in ("lane.stuck", "lane.attention"):
            attention.append(
                {
                    "kind": kind,
                    "sessionId": event.get("sessionId"),
                    "reason": event.get("reason") or event.get("detail"),
                }
            )
        elif kind == "budget.exceeded" or event.get("decisionClass") == SPEND_DECISION_CLASS:
            attention.append(
                {
                    "kind": "budget.exceeded",
                    "sessionId": event.get("sessionId"),
                    "limit": event.get("limit") or (event.get("detail") or {}).get("limit"),
                }
            )
    plan["actions"] = actions
    plan["requeue"] = requeue
    plan["attention"] = attention
    return plan


def board_main(args: Any) -> dict[str, Any]:
    """Daemonless `board` commands: export (gh read-only), plan (compile +
    collide), reconcile (desired vs actual + paging events)."""
    action = args.board_action
    if action == "export":
        if not args.repo:
            raise SystemExit("board export requires --repo OWNER/REPO")
        snapshot = export_board_snapshot(args.repo, limit=args.limit)
        if args.out:
            Path(args.out).write_text(
                json.dumps(snapshot, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        return {"snapshot": snapshot, "wrote": args.out}
    if action == "plan":
        if not args.board:
            raise SystemExit("board plan requires --board FILE")
        board = load_board_snapshot(args.board)
        return {
            "plan": plan_board(
                board,
                loaded=getattr(args, "loaded", None),
                ceiling=getattr(args, "ceiling", None),
                warn_at=getattr(args, "warn_at", None),
            )
        }
    if action == "reconcile":
        if not args.board:
            raise SystemExit("board reconcile requires --board FILE")
        board = load_board_snapshot(args.board)
        try:
            claims = claim_table()
        except (OSError, ValueError):
            claims = {}
        try:
            events = read_events(0, args.events_limit)
        except (OSError, ValueError):
            events = []
        plan = board_reconcile(
            board,
            claims,
            events,
            loaded=getattr(args, "loaded", None),
            ceiling=getattr(args, "ceiling", None),
            warn_at=getattr(args, "warn_at", None),
        )
        capacity = plan.get("capacity", {})
        if capacity.get("loaded", 0) >= capacity.get("warnAt", 0) and capacity.get("warnAt"):
            # Pre-ceiling alert (issue #19): a signal, not a rejection —
            # it fires at/above the warn threshold, i.e. below the point
            # where the gate starts refusing new lanes.
            emit_local(
                {
                    "kind": CAPACITY_WARNING_KIND,
                    "loaded": capacity.get("loaded"),
                    "ceiling": capacity.get("ceiling"),
                    "warnAt": capacity.get("warnAt"),
                    "posture": capacity.get("posture"),
                    "summary": (
                        f"host load {capacity.get('loaded')}/{capacity.get('ceiling')} "
                        f"(warn at {capacity.get('warnAt')}, admission "
                        f"{capacity.get('posture')}): shed load before launches fail"
                    ),
                }
            )
        for item in plan["actions"]:
            if item["action"] == "page-human":
                emit_local(
                    {
                        "kind": "board.decisionBlocked",
                        "decisionClass": item["decisionClass"],
                        "lane": item["lane"],
                        "issue": item["issue"],
                        "summary": (
                            f"lane {item['lane']} decision-blocked "
                            f"({item['decisionClass']}): human call required"
                        ),
                    }
                )
        return {"reconcile": plan}
    raise SystemExit(f"unknown board action: {action}")


class CallValidationError(ValueError):
    """Typed client-side error for the generic `call` passthrough.

    Raised before any wire round-trip. ``kind`` is machine-readable:
    "unknownMethod" or "missingCommandId".
    """

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class BudgetExceededError(ValueError):
    """Typed enforcement error: the lane is over budget and paused.

    ``kind`` is "overBudget" (token/context cap hit) or "modelNotAllowed"
    (model outside the lane's allowlist).
    """

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class RetireError(ValueError):
    """Typed retire error (issue #21): refusing to end a served session.

    ``kind`` is machine-readable: "sessionNotFound" (unknown alias/id, or
    already retired), "sessionRetired" (new work addressed to a retired
    session), "sessionBusy" (pending approvals/inputs, a dead turn
    awaiting owner action, or — unload only — a live turn), "uncommittedWork"
    (dirty worktree), "openPR" (an open PR on the lane branch), "standby"
    (unload only: transcript activity inside the review/CI standby window,
    or no activity signal yet). Retire's busy/dirty/PR refusals lift with
    the explicit supervisor override (``retire --force``); unload has no
    override — guarded lanes never unload.
    """

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def validate_call(
    method: str,
    params: dict[str, Any] | None = None,
    command_id: str = "auto",
) -> None:
    """Validate generic-passthrough params against the exported MSP schema.

    Raises CallValidationError for an unknown method, or when the method
    requires a commandId and the caller disabled minting without supplying
    one. Pure check: performs no I/O.
    """
    if method not in MSP_METHODS:
        raise CallValidationError("unknownMethod", f"unknown MSP method: {method}")
    if (
        command_id == "off"
        and method in COMMAND_METHODS
        and not (params or {}).get("commandId")
    ):
        raise CallValidationError(
            "missingCommandId",
            f"MSP method {method} requires commandId; pass --command-id, not 'off'",
        )


class MspHost:
    def __init__(self) -> None:
        self.proc: asyncio.subprocess.Process | None = None
        self.pending: dict[int, asyncio.Future[Any]] = {}
        self.next_id = 1
        self.sessions: dict[str, dict[str, Any]] = {}
        self.aliases: dict[str, str] = {}
        self.watchers: set[asyncio.StreamWriter] = set()
        self.stopping = asyncio.Event()
        self.budgets: dict[str, dict[str, Any]] = load_budgets()
        self.retired: dict[str, dict[str, Any]] = load_retired()

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

    def serve_argv(self) -> list[str]:
        """Argv whose stdio carries this host's `muse serve` frames."""
        return list(SERVE_ARGV)

    async def start(self) -> None:
        self.proc = await asyncio.create_subprocess_exec(
            *self.serve_argv(),
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
                    "version": "0.4.0",
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

    def refresh_budget_flag(self, state: dict[str, Any]) -> None:
        """Reconcile the lane's over-budget flag without raising.

        Breaches record a spend decision-class event (paged, never buried);
        a raised budget clears the flag with a matching event.
        """
        breach = budget_breach(state)
        session_id = state.get("sessionId")
        if breach and not state.get("overBudget"):
            state["overBudget"] = True
            state["overBudgetLimit"] = breach["limit"]
            self.record(
                {
                    "kind": "budget.exceeded",
                    "decisionClass": SPEND_DECISION_CLASS,
                    "sessionId": session_id,
                    "limit": breach["limit"],
                    "detail": breach,
                    "summary": f"lane over budget ({breach['limit']}): spend decision required",
                }
            )
        elif not breach and state.get("overBudget"):
            state["overBudget"] = False
            state.pop("overBudgetLimit", None)
            self.record(
                {
                    "kind": "budget.cleared",
                    "decisionClass": SPEND_DECISION_CLASS,
                    "sessionId": session_id,
                }
            )

    def _retired_ids(self) -> set[str]:
        """Retired session ids (empty when the host predates retire state)."""
        retired = getattr(self, "retired", None)
        return set(retired) if isinstance(retired, dict) else set()

    async def _notification(self, method: str, params: dict[str, Any]) -> None:
        session_id = params.get("sessionId")
        if session_id and session_id in self._retired_ids():
            # A retired lane stays retired: served frames for it are still
            # logged below, but never re-tracked into the roster (no
            # resurrection across daemon restarts).
            session_id = None
        if session_id:
            state = self.sessions.setdefault(session_id, {"sessionId": session_id})
            state["lastActivity"] = time.time()
            if "viewCursor" in params:
                state["viewCursor"] = params["viewCursor"]
            if method == "session/statusChanged":
                state["status"] = params.get("status")
                state["attention"] = params.get("attention", [])
            elif method == "session/tokenUsage":
                state["tokenUsage"] = params.get("cumulative")
                self.refresh_budget_flag(state)
            elif method == "session/contextUsage":
                state["contextUsage"] = {
                    "usedTokens": params.get("usedTokens"),
                    "windowTokens": params.get("windowTokens"),
                    "pressure": params.get("pressure"),
                }
                self.refresh_budget_flag(state)
            elif method == "session/modelChanged":
                if params.get("modelId"):
                    state["modelId"] = params["modelId"]
                self.refresh_budget_flag(state)
            elif method == "turn/completed":
                terminal = params.get("terminal")
                state["lastTerminal"] = terminal
                state.pop("activeTurn", None)
                if terminal in ("failed", "cancelled"):
                    state["needsOwnerAction"] = True
                    self.record(
                        {
                            "kind": "lane.attention",
                            "sessionId": session_id,
                            "turnId": params.get("turnId"),
                            "reason": f"turn{str(terminal).capitalize()}",
                            "summary": f"turn {terminal} with no owner action yet",
                        }
                    )
        interesting = {
            "session/statusChanged",
            "session/tokenUsage",
            "session/contextUsage",
            "session/modelChanged",
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
        self.sessions[session_id] = {
            **session,
            "viewCursor": started["viewCursor"],
            "alias": alias,
            "workspace": workspace,
            "lastActivity": time.time(),
        }
        self.aliases[alias] = session_id
        if args.get("budget"):
            await self.set_budget(session_id, args["budget"])
        await self.call(
            "session/rename",
            {"commandId": uuid7(), "sessionId": session_id, "name": alias},
        )
        turn = await self.submit(session_id, args["prompt"], args.get("reasoningEffort"))
        self.record({"kind": "session.launched", "sessionId": session_id, "name": alias, "workspace": workspace})
        return {"session": self.sessions[session_id], "turn": turn}

    def enforce_budget(self, session_id: str) -> None:
        """Pause over-budget lanes: refuse new work with a typed error.

        Reconciles the flag first so a freshly raised budget unpauses the
        lane without waiting for the next usage event.
        """
        state = self.sessions.get(session_id)
        if state is None:
            return
        self.refresh_budget_flag(state)
        breach = budget_breach(state)
        if breach:
            raise BudgetExceededError(
                "overBudget",
                f"lane over budget ({breach['limit']}): raise the budget before sending more work",
            )

    async def submit(self, reference: str, prompt: str, reasoning: str | None = None) -> dict[str, Any]:
        session_id = self.resolve(reference)
        if session_id in self._retired_ids():
            raise RetireError("sessionRetired", f"session is retired, not supervised: {reference!r}")
        self.enforce_budget(session_id)
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
        self.owner_acted(session_id)
        state = self.sessions.get(session_id)
        if state is not None:
            # A turn is now live on this lane. Unload refuses while the
            # marker stands (uncertain external effects); turn/completed —
            # or an explicit turn cancel/unqueue — clears it.
            state["activeTurn"] = result.get("turnId") or params["commandId"]
        self.record({"kind": "turn.submitted", "sessionId": session_id, "turnId": result.get("turnId")})
        return result

    def owner_acted(self, session_id: str) -> None:
        """Record an owner action: clears stuck/attention flags for the lane."""
        state = self.sessions.get(session_id)
        if state is None:
            return
        state["lastActivity"] = time.time()
        state["needsOwnerAction"] = False
        state.pop("lastTerminal", None)
        state.pop("stuckFlagged", None)

    async def _departure_checks(self, reference: str) -> dict[str, Any]:
        """Shared guard assessment for retire/unload (issues #21/#19).

        One code path judges both departures — retire and unload reuse
        these guards instead of duplicating them. Returns the report
        (session id, state, pending counts, worktree signals); ``blocking``
        lists ``(kind, message)`` refusals in check order — idle/busy
        first, then uncommitted work, then open PR — and empty means the
        shared guards are clear. Raises RetireError("sessionNotFound")
        for unknown or already-retired references.
        """
        session_id = self.resolve(reference)
        state = self.sessions.get(session_id)
        if state is None:
            if session_id in self._retired_ids():
                raise RetireError("sessionNotFound", f"session already retired: {reference!r}")
            raise RetireError("sessionNotFound", f"unknown session: {reference!r}")
        try:
            pending_result = await self.call("approval/listPending", {"sessionId": session_id})
        except Exception:
            pending_result = {}
        counts = pending_counts(pending_result, state.get("attention"))
        blocking: list[tuple[str, str]] = []
        if counts["approvals"] + counts["inputs"] > 0 or state.get("needsOwnerAction"):
            blocking.append(
                (
                    "sessionBusy",
                    f"session {reference!r} is not idle "
                    f"(approvals={counts['approvals']}, inputs={counts['inputs']}, "
                    f"needsOwnerAction={bool(state.get('needsOwnerAction'))}); "
                    "stand it down first or retire with --force",
                )
            )
        workspace = state.get("workspace")
        dirty = worktree_dirty(workspace) if workspace else None
        if dirty:
            blocking.append(
                (
                    "uncommittedWork",
                    f"session {reference!r} has uncommitted work in {workspace}; "
                    "land it first or retire with --force",
                )
            )
        branch = health_progress_for_workspace(workspace).get("branch") if workspace else None
        pr = health_pr_for_branch(branch, workspace) if branch else None
        if pr is not None:
            blocking.append(
                (
                    "openPR",
                    f"session {reference!r} has an open PR on {branch} "
                    f"(PR #{pr.get('number')}); merge or close it first or retire with --force",
                )
            )
        return {
            "session_id": session_id,
            "state": state,
            "counts": counts,
            "workspace": workspace,
            "dirty": dirty,
            "branch": branch,
            "pr": pr,
            "blocking": blocking,
        }

    def _drop_departed(
        self,
        session_id: str,
        state: dict[str, Any],
        *,
        forced: bool,
        event_kind: str,
        summary: str,
        event_extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Shared departure removal for retire/unload (issues #21/#19).

        Releases the lane's branch lease (if any), drops the session from
        the roster/aliases/budgets, persists the id in the retired set so
        restarts never resurrect it, and records the departure event. The
        caller supplies the event kind/summary/attribution — the removal
        itself is one code path.
        """
        released: dict[str, Any] | None = None
        try:
            for leased_branch, claim in load_claims().items():
                if claim.get("sessionId") == session_id:
                    release_claim(leased_branch)
                    released = {
                        "branch": leased_branch,
                        "lane": claim.get("lane"),
                        "host": claim.get("host"),
                    }
                    break
        except Exception:
            released = None
        alias = state.get("alias")
        self.sessions.pop(session_id, None)
        for name in [name for name, owned in self.aliases.items() if owned == session_id]:
            del self.aliases[name]
        self.budgets.pop(session_id, None)
        save_budgets(self.budgets)
        retired = dict(getattr(self, "retired", None) or {})
        retired[session_id] = {
            "alias": alias,
            "retiredAt": time.time(),
            "forced": bool(forced),
        }
        self.retired = retired
        save_retired(retired)
        self.record(
            {
                "kind": event_kind,
                "sessionId": session_id,
                "alias": alias,
                "forced": bool(forced),
                "releasedClaim": released,
                "summary": summary,
                **(event_extra or {}),
            }
        )
        return {"sessionId": session_id, "alias": alias, "releasedClaim": released}

    async def retire(self, reference: str, force: bool = False) -> dict[str, Any]:
        """End a served session whose duty is complete (issue #21).

        Judges the shared departure guards (idle: no pending
        approvals/inputs, no dead turn awaiting owner action; clean
        worktree; no open PR), refusing with a typed RetireError unless
        ``force`` carries the explicit supervisor override. On success the
        session leaves the roster, its alias and budget slots free up, its
        branch lease (if any) is released, and its id persists in the
        retired set so restarts never resurrect it into the roster, health,
        or stuck accounting.

        Raises RetireError ("sessionNotFound" / "sessionBusy" /
        "uncommittedWork" / "openPR").
        """
        checks = await self._departure_checks(reference)
        session_id = checks["session_id"]
        state = checks["state"]
        if checks["blocking"] and not force:
            kind, message = checks["blocking"][0]
            raise RetireError(kind, message)
        alias = state.get("alias")
        dropped = self._drop_departed(
            session_id,
            state,
            forced=force,
            event_kind="lane.retired",
            summary=f"lane {alias or session_id} retired" + (" (forced)" if force else ""),
        )
        return {
            **dropped,
            "forced": bool(force),
            "idle": checks["counts"],
            "worktree": {
                "workspace": checks["workspace"],
                "dirty": checks["dirty"],
                "branch": checks["branch"],
                "pr": checks["pr"],
            },
            "retired": True,
        }

    async def unload(
        self, reference: str, reason: str = "capacity", by: str | None = None
    ) -> dict[str, Any]:
        """Unload an idle owned session to free host capacity (issue #19).

        The capacity-attributed departure path: judges the shared retire
        guards with NO override — lanes with uncommitted work, pending
        approvals/inputs, a dead turn awaiting owner action, or an open PR
        never unload — then refuses lanes with a live turn, or with
        transcript activity inside the standby window (review/CI follow-up;
        any serve notification, including child-activity relays, refreshes
        activity and re-arms standby). On success the session leaves the
        roster exactly like a retire, and a ``lane.unloaded`` event records
        who ordered it, why, and the roster count before/after — explicit
        and attributable, never unattended deletion.

        Raises RetireError ("sessionNotFound" / "sessionBusy" /
        "uncommittedWork" / "openPR" / "standby").
        """
        checks = await self._departure_checks(reference)
        if checks["blocking"]:
            kind, message = checks["blocking"][0]
            raise RetireError(kind, message)
        session_id = checks["session_id"]
        state = checks["state"]
        live_turn = state.get("activeTurn")
        if live_turn:
            raise RetireError(
                "sessionBusy",
                f"session {reference!r} has a live turn ({live_turn}); "
                "wait for turn/completed (or cancel it) before unloading",
            )
        standby = unload_standby_seconds()
        last = state.get("lastActivity")
        if not isinstance(last, (int, float)):
            raise RetireError(
                "standby",
                f"session {reference!r} has no activity signal yet; "
                "cannot prove the standby window — observe it first",
            )
        idle_for = time.time() - last
        if idle_for < standby:
            raise RetireError(
                "standby",
                f"session {reference!r} idle {idle_for:.0f}s "
                f"is inside the {standby}s standby window for review/CI "
                "follow-up; unload after it goes quiet",
            )
        operator = by or agent_id()
        loaded_before = len(self.sessions)
        alias = state.get("alias")
        dropped = self._drop_departed(
            session_id,
            state,
            forced=False,
            event_kind="lane.unloaded",
            summary=f"lane {alias or session_id} unloaded by {operator} ({reason})",
            event_extra={
                "by": operator,
                "reason": reason,
                "loadedBefore": loaded_before,
                "loadedAfter": loaded_before - 1,
            },
        )
        return {
            **dropped,
            "forced": False,
            "by": operator,
            "reason": reason,
            "idle": checks["counts"],
            "worktree": {
                "workspace": checks["workspace"],
                "dirty": checks["dirty"],
                "branch": checks["branch"],
                "pr": checks["pr"],
            },
            "unloaded": True,
        }

    async def list_sessions(self) -> dict[str, Any]:
        result = await self.call("session/list", {"limit": 200})
        now = time.time()
        after = stuck_after_seconds()
        owned = []
        for item in result.get("sessions", []):
            session_id = item["sessionId"]
            if session_id in self._retired_ids():
                continue
            if session_id not in self.sessions:
                continue
            state = self.sessions[session_id]
            merged = {**item, "alias": state.get("alias")}
            for key in ("tokenUsage", "contextUsage", "modelId", "budget", "lastActivity"):
                merged.setdefault(key, state.get(key))
            merged["overBudget"] = bool(state.get("overBudget"))
            merged["needsOwnerAction"] = bool(state.get("needsOwnerAction"))
            stuck = lane_stuck(state, now, after)
            if stuck and stuck.get("reason") == "idle" and state.get("workspace"):
                repo_ts = await asyncio.to_thread(repo_activity_ts, state.get("workspace"))
                if repo_ts is not None:
                    state["lastRepoActivity"] = repo_ts
                    stuck = lane_stuck(state, now, after)
            merged["stuck"] = stuck
            if stuck and not state.get("stuckFlagged"):
                state["stuckFlagged"] = True
                self.record(
                    {
                        "kind": "lane.stuck",
                        "sessionId": session_id,
                        "reason": stuck.get("reason"),
                        "detail": stuck,
                        "summary": f"lane stuck ({stuck.get('reason')}): owner attention required",
                    }
                )
            elif not stuck and state.get("stuckFlagged"):
                state.pop("stuckFlagged", None)
                self.record({"kind": "lane.recovered", "sessionId": session_id})
            owned.append(merged)
        return {"sessions": owned}

    async def health(self, events_limit: int = 2000) -> dict[str, Any]:
        """Fuse one swarm health screen (issue #11).

        Reuses `list_sessions` (P2 stuck/budget flags plus the P4 lease
        on each row — no second ownership system) and the `events` log,
        then adds per-member pending approvals/inputs plus worktree
        progress (git branch-ahead, read-only `gh` PR checks). One bad
        lane never sinks the screen: per-member MSP/git/gh failures
        degrade that member's cells to unknown.
        """
        listed = await self.list_sessions()
        items = listed.get("sessions", [])
        # P4 lease state rides along, same shape as `list`: the live
        # lease (if any) on each owned session.
        live_by_session = {
            claim.get("sessionId"): claim
            for claim in claim_table().values()
            if claim.get("live") and claim.get("sessionId")
        }
        events = read_events(0.0, max(1, int(events_limit)))
        blockers: dict[str, list[dict[str, Any]]] = {}
        for record in events:
            if isinstance(record, dict) and record.get("kind") == "blocker":
                sid = event_session(record)
                if sid is not None:
                    blockers.setdefault(sid, []).append(record)
        inputs: list[dict[str, Any]] = []
        pending_by: dict[str, dict[str, int]] = {}
        progress_by: dict[str, dict[str, Any]] = {}
        for item in items:
            sid = str(item.get("sessionId") or "")
            state = self.sessions.get(sid, {})
            entry = dict(item)
            entry.setdefault("lastRepoActivity", state.get("lastRepoActivity"))
            lease = live_by_session.get(sid)
            if lease is not None:
                entry["lease"] = {
                    "host": lease.get("host"),
                    "lane": lease.get("lane"),
                    "branch": lease.get("branch"),
                    "checkout": lease.get("checkout"),
                    "expiresAt": lease.get("expiresAt"),
                }
            inputs.append(entry)
            try:
                pending_result = await self.call(
                    "approval/listPending", {"sessionId": sid}
                )
            except Exception:
                pending_result = {}
            counts = pending_counts(
                pending_result, item.get("attention"), blockers.get(sid, [])
            )
            pending_by[sid] = counts
            workspace = item.get("workspace") or state.get("workspace")
            try:
                progress = await asyncio.to_thread(
                    health_progress_for_workspace, workspace
                )
            except Exception:
                progress = {"branch": None, "ahead": None}
            if progress.get("branch"):
                try:
                    pr = await asyncio.to_thread(
                        health_pr_for_branch, progress["branch"], workspace
                    )
                except Exception:
                    pr = None
                if pr is not None:
                    progress["pr"] = pr
            progress["pending"] = counts
            progress_by[sid] = progress
        return fuse_health(
            inputs,
            events,
            pending_by,
            progress_by,
            time.time(),
            stuck_after_seconds(),
            down_after_seconds(),
        )

    async def set_budget(self, reference: str, spec: dict[str, Any]) -> dict[str, Any]:
        session_id = self.resolve(reference)
        budget = normalize_budget(spec)
        state = self.sessions.setdefault(session_id, {"sessionId": session_id})
        state["budget"] = budget
        self.budgets[session_id] = budget
        save_budgets(self.budgets)
        self.refresh_budget_flag(state)
        self.record({"kind": "budget.updated", "sessionId": session_id, "budget": budget})
        return {"sessionId": session_id, "budget": budget}

    async def get_budget(self, reference: str) -> dict[str, Any]:
        session_id = self.resolve(reference)
        state = self.sessions.get(session_id, {})
        return {
            "sessionId": session_id,
            "budget": state.get("budget", self.budgets.get(session_id)),
            "tokenUsage": state.get("tokenUsage"),
            "contextUsage": state.get("contextUsage"),
            "overBudget": bool(state.get("overBudget")),
        }

    async def clear_budget(self, reference: str) -> dict[str, Any]:
        session_id = self.resolve(reference)
        state = self.sessions.get(session_id)
        if state is not None:
            state.pop("budget", None)
            self.refresh_budget_flag(state)
        self.budgets.pop(session_id, None)
        save_budgets(self.budgets)
        self.record({"kind": "budget.removed", "sessionId": session_id})
        return {"sessionId": session_id, "budget": None}

    def enforce_model(self, session_id: str, params: dict[str, Any]) -> None:
        state = self.sessions.get(session_id)
        models = ((state or {}).get("budget") or {}).get("models")
        if not models:
            return
        wanted = params.get("model", params.get("modelId"))
        if wanted is not None and wanted not in models:
            raise BudgetExceededError(
                "modelNotAllowed",
                f"model {wanted} is outside the lane budget {models}",
            )

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

        Validates against the exported schema before hitting the wire
        (unknown method / missing-but-required commandId raises
        CallValidationError with no round-trip), and enforces lane budgets:
        new turns on over-budget lanes and setModel outside the allowlist
        raise BudgetExceededError.
        """
        validate_call(method, params, command_id)
        merged = dict(params or {})
        if command_id != "off" and method in COMMAND_METHODS and "commandId" not in merged:
            merged["commandId"] = uuid7() if command_id in (None, "auto") else command_id
        session_id = merged.get("sessionId")
        if session_id:
            if session_id in self._retired_ids():
                raise RetireError(
                    "sessionRetired", f"session is retired, not supervised: {session_id!r}"
                )
            if method == "turn/start":
                self.enforce_budget(session_id)
            elif method == "session/setModel":
                self.enforce_model(session_id, merged)
        result = await self.call(method, merged)
        if session_id:
            self.owner_acted(session_id)
            if method in ("turn/cancel", "turn/unqueue"):
                ended = self.sessions.get(session_id)
                if ended is not None:
                    ended.pop("activeTurn", None)
        self.record({"kind": "controller.call", "method": method, "sessionId": merged.get("sessionId")})
        return result


class RemoteMspHost(MspHost):
    """An MspHost whose serve stdio is carried over SSH.

    Identical supervision (budgets, stuck detection, call validation) to a
    local host: only the spawn argv differs — `ssh target -- muse serve …`
    instead of a local `muse serve`. MSP itself never sees a socket or a
    port; SSH is the carrier. The peer host still runs its own m8s agent
    owning that serve process (one agent, one serve per host).
    """

    def __init__(self, ssh_target: str, ssh_port: int | None = None) -> None:
        super().__init__()
        self.ssh_target = ssh_target
        self.ssh_port = ssh_port

    def serve_argv(self) -> list[str]:
        return build_ssh_serve_argv(self.ssh_target, self.ssh_port)


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


# ---------------------------------------------------------------------------
# Moonshot P2+: `m8s health` — fused swarm health screen (issue #11).
#
# One screen for the whole swarm. Per member it fuses liveness (`list`
# status + last-event age from `events`), responsiveness (recent turn
# activity), and progress (branch-ahead commits, open-PR check state,
# pending approvals/inputs). Exactly three flags exist — `down` (listed
# but unresponsive), `stuck` (running with no events and no commits),
# `blocked` (waiting on approval/user input) — and nothing else is a
# flag: unflagged members render as `-`. P2 stuck/budget surfacing is
# reused (lane_stuck, overBudget) and P4 lease state rides along on each
# row; neither is regressed. `gh` is read-only (pr list); any git/gh
# failure degrades that cell to unknown, never the screen.
# ---------------------------------------------------------------------------

# Long-silence horizon for `down`. A listed lane with no events for this
# long is unresponsive rather than merely stuck. Overridable via
# M8S_DOWN_AFTER_SECONDS; invalid values fall back to the default.
DOWN_AFTER_SECONDS = 2 * 60 * 60

# The only health flags. Priority on a member is blocked > down > stuck:
# a lane awaiting input is blocked even when silent, and long silence is
# down (cannot confirm it is running) rather than stuck.
HEALTH_FLAGS = ("down", "stuck", "blocked")

# Event kinds that count as turn activity (responsiveness).
TURN_EVENT_KINDS = frozenset({"turn.submitted", "lane.attention"})


def down_after_seconds() -> int:
    try:
        return max(1, int(os.environ.get("M8S_DOWN_AFTER_SECONDS", DOWN_AFTER_SECONDS)))
    except (TypeError, ValueError):
        return DOWN_AFTER_SECONDS


def event_session(record: dict[str, Any]) -> str | None:
    """Owning session of an event record.

    Daemon-local records carry `sessionId` at top level; served MSP
    frames are wrapped as `msp.event` with the id inside `params`.
    Returns None for swarm-wide records with no owner.
    """
    sid = record.get("sessionId")
    if isinstance(sid, str) and sid:
        return sid
    params = record.get("params")
    if isinstance(params, dict):
        sid = params.get("sessionId")
        if isinstance(sid, str) and sid:
            return sid
    return None


def is_turn_event(record: dict[str, Any]) -> bool:
    """Whether an event record signals turn activity (responsiveness)."""
    if record.get("kind") in TURN_EVENT_KINDS:
        return True
    if record.get("kind") == "msp.event":
        method = record.get("method")
        return isinstance(method, str) and method.startswith("turn/")
    return False


def pending_counts(
    result: Any,
    attention: Any,
    blocker_events: list[dict[str, Any]] | None = None,
) -> dict[str, int]:
    """Fuse pending approvals/inputs from every honest signal.

    `result` is the raw `approval/listPending` reply (whose exact shape
    is server-defined, so list-valued fields are counted defensively);
    `attention` is the live `session/statusChanged` attention array;
    `blocker_events` are unresolved approval/user-input request records.
    Never raises — unparseable inputs count as zero for that signal.
    """
    approvals = 0
    inputs = 0
    if isinstance(result, dict):
        for key, value in result.items():
            if not isinstance(value, list):
                continue
            lowered = str(key).lower()
            if any(hint in lowered for hint in ("input", "question")):
                inputs = max(inputs, len(value))
            else:
                approvals = max(approvals, len(value))
    try:
        names = [str(entry).lower() for entry in (attention or [])]
    except TypeError:
        names = []
    if any("approval" in name for name in names):
        approvals = max(approvals, 1)
    if any("input" in name or "question" in name for name in names):
        inputs = max(inputs, 1)
    for record in blocker_events or []:
        if not isinstance(record, dict):
            continue
        reason = str(record.get("reason") or "")
        if reason.startswith("approval"):
            approvals += 1
        elif reason.startswith("userInput"):
            inputs += 1
    return {"approvals": approvals, "inputs": inputs}


def pr_check_state(rollup: Any) -> str | None:
    """Map a `gh` statusCheckRollup to passing/pending/failing.

    None (no PR, or gh unavailable) stays None (unknown); an empty
    rollup means no checks have reported yet, i.e. pending.
    """
    if rollup is None:
        return None
    if not isinstance(rollup, list) or not rollup:
        return "pending"
    worst = "passing"
    seen = False
    for check in rollup:
        if not isinstance(check, dict):
            continue
        seen = True
        value = str(
            check.get("conclusion")
            or check.get("status")
            or check.get("state")
            or ""
        ).upper()
        if value in ("FAILURE", "FAILED", "ERROR", "TIMED_OUT"):
            return "failing"
        if value in ("SUCCESS", "SUCCEEDED", "SKIPPED", "NEUTRAL"):
            continue
        worst = "pending"
    return worst if seen else "pending"


def health_progress_for_workspace(
    workspace: str | None, run: Any = None
) -> dict[str, Any]:
    """Branch + ahead-count for a lane worktree (never raises).

    `ahead` counts commits on HEAD past `@{upstream}`; it is None when
    the worktree has no determinable upstream. `run` injects the
    subprocess runner (tests); default is `subprocess.run`.
    """
    progress: dict[str, Any] = {"branch": None, "ahead": None}
    if not workspace:
        return progress
    runner = run or subprocess.run

    def git(*argv: str) -> str:
        proc = runner(
            ["git", "-C", str(workspace), *argv],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            raise OSError(f"git {' '.join(argv)} failed")
        return (proc.stdout or "").strip()

    try:
        branch = git("rev-parse", "--abbrev-ref", "HEAD")
        if branch and branch != "HEAD":
            progress["branch"] = branch
        try:
            progress["ahead"] = int(git("rev-list", "--count", "@{upstream}..HEAD"))
        except (OSError, ValueError, subprocess.SubprocessError):
            progress["ahead"] = None
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return progress


def health_pr_for_branch(
    branch: str | None, workspace: str | None = None, run: Any = None
) -> dict[str, Any] | None:
    """Open-PR check state for a lane branch via read-only `gh` (never raises).

    Returns {"number", "url", "checks"} or None when there is no open PR
    or `gh` is unavailable/fails. Never writes to GitHub — the
    coordinator owns board writes. `run` injects the runner (tests).
    """
    if not branch:
        return None
    runner = run or subprocess.run
    try:
        proc = runner(
            [
                "gh", "pr", "list",
                "--head", branch,
                "--limit", "1",
                "--json", "number,url,statusCheckRollup",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(workspace) if workspace else None,
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    try:
        entries = json.loads(proc.stdout or "null")
    except ValueError:
        return None
    if not isinstance(entries, list) or not entries:
        return None
    entry = entries[0]
    if not isinstance(entry, dict):
        return None
    return {
        "number": entry.get("number"),
        "url": entry.get("url"),
        "checks": pr_check_state(entry.get("statusCheckRollup")),
    }


def fuse_health(
    sessions: list[dict[str, Any]],
    events: list[dict[str, Any]] | None = None,
    pending_by_session: dict[str, dict[str, int]] | None = None,
    progress_by_session: dict[str, dict[str, Any]] | None = None,
    now: float | None = None,
    stuck_after: int | None = None,
    down_after: int | None = None,
) -> dict[str, Any]:
    """Fuse liveness + responsiveness + progress into one swarm report (pure).

    `sessions` are `list`-style member states; `events` the `events`-style
    log; `pending_by_session` maps sessionId to pending_counts-style
    {"approvals", "inputs"}; `progress_by_session` maps sessionId to
    {"branch", "ahead", "pr"}. Members sort by alias/sessionId for a
    stable order. Each row carries exactly one of flag None/"down"/
    "stuck"/"blocked" with blocked > down > stuck priority: pending
    input wins over silence; long silence is down; otherwise P2
    lane_stuck (idle on events AND commits, or a dead turn awaiting
    owner action) is stuck.
    """
    at = time.time() if now is None else now
    s_after = STUCK_IDLE_SECONDS if stuck_after is None else stuck_after
    d_after = DOWN_AFTER_SECONDS if down_after is None else down_after
    pending_by_session = pending_by_session or {}
    progress_by_session = progress_by_session or {}
    last_event: dict[str, float] = {}
    last_turn: dict[str, float] = {}
    turns_recent: dict[str, int] = {}
    for record in events or []:
        if not isinstance(record, dict):
            continue
        sid = event_session(record)
        if sid is None:
            continue
        ts = record.get("at")
        if not isinstance(ts, (int, float)):
            continue
        if ts > last_event.get(sid, float("-inf")):
            last_event[sid] = ts
        if is_turn_event(record):
            if ts > last_turn.get(sid, float("-inf")):
                last_turn[sid] = ts
            if ts > at - s_after:
                turns_recent[sid] = turns_recent.get(sid, 0) + 1
    members: list[dict[str, Any]] = []
    counts = {"down": 0, "stuck": 0, "blocked": 0}
    ordered = sorted(sessions, key=lambda s: str(s.get("alias") or s.get("sessionId") or ""))
    for state in ordered:
        sid = str(state.get("sessionId") or "")
        name = str(state.get("alias") or sid)
        event_at = last_event.get(sid)
        event_age = (at - event_at) if event_at is not None else None
        turn_at = last_turn.get(sid)
        pending = dict(pending_by_session.get(sid) or {"approvals": 0, "inputs": 0})
        try:
            names = [str(entry).lower() for entry in (state.get("attention") or [])]
        except TypeError:
            names = []
        if any("approval" in name for name in names):
            pending["approvals"] = max(int(pending.get("approvals", 0)), 1)
        if any("input" in name or "question" in name for name in names):
            pending["inputs"] = max(int(pending.get("inputs", 0)), 1)
        stuck = lane_stuck(state, at, s_after)
        flag: str | None = None
        if int(pending.get("approvals", 0)) + int(pending.get("inputs", 0)) > 0:
            flag = "blocked"
        elif event_at is None or (event_age is not None and event_age >= d_after):
            flag = "down"
        elif stuck is not None:
            flag = "stuck"
        if flag is not None:
            counts[flag] += 1
        progress = progress_by_session.get(sid) or {}
        members.append(
            {
                "member": name,
                "sessionId": sid,
                "status": state.get("status"),
                "lastEventAgeSeconds": event_age,
                "turnsRecent": turns_recent.get(sid, 0),
                "lastTurnAgeSeconds": (at - turn_at) if turn_at is not None else None,
                "branch": progress.get("branch"),
                "ahead": progress.get("ahead"),
                "pr": progress.get("pr"),
                "pending": {
                    "approvals": int(pending.get("approvals", 0)),
                    "inputs": int(pending.get("inputs", 0)),
                },
                "lease": state.get("lease"),
                "overBudget": bool(state.get("overBudget")),
                "stuck": stuck,
                "flag": flag,
            }
        )
    return {
        "members": members,
        "summary": {"total": len(members), **counts},
    }


def health_age(seconds: Any) -> str:
    """Compact age cell: 12s / 3m / 2h / 5d, `-` when unknown."""
    if not isinstance(seconds, (int, float)):
        return "-"
    total = max(0, int(seconds))
    if total < 60:
        return f"{total}s"
    if total < 3600:
        return f"{total // 60}m"
    if total < 86400:
        return f"{total // 3600}h"
    return f"{total // 86400}d"


def format_health(report: dict[str, Any], now: float | None = None) -> str:
    """Render the fused report as one deterministic screen (pure).

    Stable member order (fusion order), one row per member, plus a
    flags summary line. The only flag words that ever appear are
    down/stuck/blocked; unflagged members show `-`.
    """
    del now  # Screens carry precomputed ages; rendering adds no clock reads.
    members = report.get("members") or []
    summary = report.get("summary") or {}
    total = int(summary.get("total", len(members)))
    noun = "member" if total == 1 else "members"
    lines = [f"m8s health: {total} {noun}"]
    if not members:
        lines.append("(swarm empty)")
    else:
        width = max(len(str(member.get("member", ""))) for member in members)
        lines.append(f"{'MEMBER'.ljust(width)}  {'LIVE':<16}  {'TURNS':<18}  PROGRESS  FLAG")
        for member in members:
            status = member.get("status") or "unknown"
            live = f"{status} ev {health_age(member.get('lastEventAgeSeconds'))}"
            turns = (
                f"{int(member.get('turnsRecent', 0))} "
                f"(last {health_age(member.get('lastTurnAgeSeconds'))})"
            )
            parts: list[str] = []
            branch = member.get("branch")
            if branch:
                ahead = member.get("ahead")
                parts.append(f"{branch}+{ahead}" if isinstance(ahead, int) else str(branch))
            pr = member.get("pr") or {}
            if pr.get("number") is not None:
                parts.append(f"#{pr['number']}/{pr.get('checks') or '?'}")
            pending = member.get("pending") or {}
            if int(pending.get("approvals", 0)):
                parts.append(f"approvals:{pending['approvals']}")
            if int(pending.get("inputs", 0)):
                parts.append(f"inputs:{pending['inputs']}")
            progress = " ".join(parts) if parts else "-"
            flag = member.get("flag") or "-"
            lines.append(
                f"{str(member.get('member', '')).ljust(width)}  "
                f"{live:<16}  {turns:<18}  {progress}  {flag}"
            )
    lines.append(
        f"summary: {total} {noun} - "
        f"down: {int(summary.get('down', 0))}, "
        f"stuck: {int(summary.get('stuck', 0))}, "
        f"blocked: {int(summary.get('blocked', 0))}"
    )
    return "\n".join(lines)


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
    if command == "retire":
        return await host.retire(request["session"], force=bool(request.get("force")))
    if command == "unload":
        return await host.unload(
            request["session"],
            reason=str(request.get("reason") or "capacity"),
            by=request.get("by"),
        )
    if command == "list":
        result = await host.list_sessions()
        # P4: lease state rides along — the full claim table plus the live
        # lease (if any) on each owned session.
        table = claim_table()
        result["claims"] = table
        live_by_session = {
            claim.get("sessionId"): claim
            for claim in table.values()
            if claim.get("live") and claim.get("sessionId")
        }
        for item in result.get("sessions", []):
            lease = live_by_session.get(item.get("sessionId"))
            if lease is not None:
                item["lease"] = {
                    "host": lease.get("host"),
                    "lane": lease.get("lane"),
                    "branch": lease.get("branch"),
                    "checkout": lease.get("checkout"),
                    "expiresAt": lease.get("expiresAt"),
                }
        return result
    if command == "events":
        return {"events": read_events(float(request.get("after", 0)), int(request.get("limit", 200)))}
    if command == "pending":
        result = await host.call("approval/listPending", {"sessionId": host.resolve(request["session"])})
        return result
    if command == "health":
        try:
            limit = int(request.get("eventsLimit", 2000))
        except (TypeError, ValueError):
            limit = 2000
        return await host.health(limit)
    if command == "call":
        params = dict(request.get("params") or {})
        if request.get("session"):
            params["sessionId"] = host.resolve(request["session"])
        return await host.supervised_call(request["method"], params, request.get("commandId", "auto"))
    if command == "budget":
        if request.get("clear"):
            return await host.clear_budget(request["session"])
        spec = {
            key: request[key]
            for key in ("maxTokens", "maxContextTokens", "models")
            if request.get(key) is not None
        }
        if spec:
            return await host.set_budget(request["session"], spec)
        return await host.get_budget(request["session"])
    if command == "host":
        action = request.get("action", "list")
        if action == "list":
            now = time.time()
            return {
                "hosts": [
                    {**record, "live": host_alive(record, now, host_ttl_seconds())}
                    for _, record in sorted(load_hosts().items())
                ]
            }
        if action == "enroll":
            if not request.get("name") or not request.get("target"):
                raise HostError("badHostName", "host enroll requires --name and --target")
            max_lanes = request.get("maxLanes", 4)
            probe = ssh_probe(request["target"], request.get("sshPort"), timeout=20)
            record = enroll_host(
                request["name"],
                request["target"],
                max_lanes,
                request.get("sshPort"),
                request.get("remoteMsp"),
                probe=probe,
            )
            host.record({"kind": "host.enrolled", "host": record["name"]})
            return {"host": record}
        if action == "remove":
            record = remove_host(request["name"])
            host.record({"kind": "host.removed", "host": request["name"]})
            return {"host": record}
        if action == "heartbeat":
            known = load_hosts().get(request.get("name", ""))
            if known is None:
                raise HostError("hostNotFound", f"unknown host: {request.get('name')!r}")
            probe = ssh_probe(known["sshTarget"], known.get("sshPort"), timeout=20)
            record = heartbeat_host(request["name"], alive=True)
            host.record(
                {
                    "kind": "host.heartbeat",
                    "host": request["name"],
                    "museVersion": probe.get("version", ""),
                }
            )
            # P4: the P3 registry heartbeat IS the lease heartbeat — re-gossip
            # this host's ownership claims on it. No second heartbeat exists.
            refreshed = refresh_claims(request["name"])
            if refreshed:
                beat = make_heartbeat(request["name"], agent_id(), list(refreshed))
                publish("m8s.heartbeat", beat)
                host.record(
                    {
                        "kind": "claim.heartbeat",
                        "host": request["name"],
                        "claims": [c["branch"] for c in refreshed],
                        "summary": f"re-gossiped {len(refreshed)} claim(s) on heartbeat",
                    }
                )
            return {"host": record}
        raise HostError("badHostName", f"unknown host action: {action!r}")
    if command == "bus":
        action = request.get("action", "list")
        if action == "claim":
            if not request.get("lane") or not request.get("branch"):
                raise BusError(
                    "badMessage", "bus claim requires --lane and --branch"
                )
            session_id = None
            if request.get("session"):
                session_id = host.resolve(request["session"])
            claim = claim_branch(
                request.get("host") or local_host(),
                request["lane"],
                request["branch"],
                request.get("checkout"),
                session_id,
                request.get("ttl"),
            )
            host.record(
                {
                    "kind": "claim.acquired",
                    "host": claim["host"],
                    "lane": claim["lane"],
                    "branch": claim["branch"],
                    "summary": f"{claim['host']} holds lane {claim['lane']} "
                    f"on branch {claim['branch']}",
                }
            )
            return {"claim": claim}
        if action == "release":
            if not request.get("branch"):
                raise BusError("badMessage", "bus release requires --branch")
            claim = release_claim(request["branch"], request.get("host"))
            host.record({"kind": "claim.released", "branch": request["branch"]})
            return {"claim": claim}
        if action == "list":
            for expired in sweep_claims():
                host.record(
                    {
                        "kind": "claim.expired",
                        "host": expired.get("host"),
                        "lane": expired.get("lane"),
                        "branch": expired.get("branch"),
                        "requeue": True,
                        "summary": f"lease on {expired.get('branch')} expired; "
                        "branch requeued for reassignment",
                    }
                )
            return {"claims": claim_table()}
        if action == "heartbeat":
            name = request.get("host") or local_host()
            refreshed = refresh_claims(name, ttl=request.get("ttl"))
            beat = make_heartbeat(name, agent_id(), list(refreshed))
            if refreshed:
                publish("m8s.heartbeat", beat)
            host.record(
                {
                    "kind": "claim.heartbeat",
                    "host": name,
                    "claims": [c["branch"] for c in refreshed],
                    "summary": f"re-gossiped {len(refreshed)} claim(s) on heartbeat",
                }
            )
            return {"host": name, "refreshed": refreshed, "heartbeat": beat}
        if action == "intent":
            if not request.get("verb"):
                raise BusError("badMessage", "bus intent requires --verb")
            session_id = None
            if request.get("session"):
                session_id = host.resolve(request["session"])
            intent = make_intent(
                request.get("host") or local_host(),
                request["verb"],
                request.get("lane"),
                request.get("branch"),
                request.get("detail"),
                session_id,
                request.get("ttl"),
            )
            publish("m8s.intent", intent)
            host.record(
                {
                    "kind": "intent.published",
                    "host": intent["host"],
                    "verb": intent["verb"],
                    "branch": intent.get("branch"),
                }
            )
            return {"intent": intent}
        if action == "read":
            return {
                "records": read_bus(
                    request.get("subject"), int(request.get("limit", 200))
                )
            }
        raise BusError("badMessage", f"unknown bus action: {action!r}")
    if command == "place":
        spec = {
            key: request[key]
            for key in ("branch", "checkout", "files")
            if request.get(key) is not None
        }
        return {
            "ranking": place_lane(spec, load_hosts(), request.get("lanesByHost") or {})
        }
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
        kind = getattr(exc, "kind", None)
        if kind:
            response["errorKind"] = kind
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
    sub.add_parser("list", help="list owned sessions with budget, usage, and stuck flags")
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
    launch.add_argument("--max-tokens", type=int, help="per-lane cumulative token cap")
    launch.add_argument("--max-context-tokens", type=int, help="per-lane context occupancy cap")
    launch.add_argument("--models", help="comma-separated per-lane model allowlist")
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
    retire = sub.add_parser(
        "retire", help="end a served session: confirm idle, drop it from roster/health"
    )
    retire.add_argument("session", help="session alias or id to retire")
    retire.add_argument(
        "--force",
        action="store_true",
        help="explicit supervisor override: retire a busy session or one with "
        "uncommitted work / an open PR",
    )
    unload = sub.add_parser(
        "unload",
        help="unload an idle owned session to free host capacity "
        "(no override: guarded lanes never unload)",
    )
    unload.add_argument("session", help="session alias or id to unload")
    unload.add_argument(
        "--reason",
        default="capacity",
        help="attributed reason recorded on the lane.unloaded event (default: capacity)",
    )
    unload.add_argument(
        "--by",
        default=None,
        help="attributed operator recorded on the event (default: this agent)",
    )
    health_cmd = sub.add_parser(
        "health", help="fused swarm health screen: liveness, turns, progress, down/stuck/blocked"
    )
    health_cmd.add_argument("--json", action="store_true", help="print the raw fusion report as JSON")
    health_cmd.add_argument(
        "--events-limit", type=int, default=2000, help="recent events to scan (default 2000)"
    )

    budget = sub.add_parser("budget", help="show or set a lane's token/context/model budget")
    budget.add_argument("session")
    budget.add_argument("--max-tokens", type=int, help="cumulative token cap")
    budget.add_argument("--max-context-tokens", type=int, help="context occupancy cap")
    budget.add_argument("--models", help="comma-separated model allowlist")
    budget.add_argument("--clear", action="store_true", help="remove the lane budget")

    host_cmd = sub.add_parser("host", help="enroll/list/remove/heartbeat SSH hosts")
    host_cmd.add_argument("action", choices=("enroll", "list", "remove", "heartbeat"))
    host_cmd.add_argument("--name", help="host name (enroll/remove/heartbeat)")
    host_cmd.add_argument("--target", help="SSH target, e.g. user@peer (enroll)")
    host_cmd.add_argument("--ssh-port", type=int, help="SSH port (enroll)")
    host_cmd.add_argument("--max-lanes", type=int, default=4, help="lane capacity (enroll)")
    host_cmd.add_argument("--remote-msp", help="remote path to muse-msp.py (enroll)")

    place = sub.add_parser("place", help="dry-run placement ranking for a lane spec")
    place.add_argument("--branch", help="lane branch (collision zone)")
    place.add_argument("--checkout", help="repo checkout path (affinity)")
    place.add_argument("--files", help="comma-separated files (collision zone)")

    bus_cmd = sub.add_parser(
        "bus", help="claim/heartbeat/intent bus: branch leases and gossip (Moonshot P4)"
    )
    bus_cmd.add_argument(
        "action",
        choices=("claim", "release", "list", "heartbeat", "intent", "read"),
        help="claim/release a branch lease, list leases, heartbeat, publish intent, read log",
    )
    bus_cmd.add_argument("--host", help="claiming host (default: this hostname)")
    bus_cmd.add_argument("--lane", help="lane holding the lease (claim/intent)")
    bus_cmd.add_argument("--branch", help="branch under lease (claim/release/intent)")
    bus_cmd.add_argument("--checkout", help="repo checkout path (claim: one writer each)")
    bus_cmd.add_argument("--session", help="session alias or id tied to the claim/intent")
    bus_cmd.add_argument("--ttl", type=int, help="lease/intent TTL seconds")
    bus_cmd.add_argument("--verb", help="intent verb, e.g. propose-plan (intent)")
    bus_cmd.add_argument("--detail", help="intent detail text (intent)")
    bus_cmd.add_argument(
        "--subject",
        choices=list(BUS_SUBJECTS),
        help="bus subject filter (read)",
    )
    bus_cmd.add_argument("--limit", type=int, default=200, help="bus log lines (read)")

    board_cmd = sub.add_parser(
        "board", help="board-to-lane compiler: export/plan/reconcile (Moonshot P5)"
    )
    board_cmd.add_argument(
        "board_action",
        choices=("export", "plan", "reconcile"),
        help="export a snapshot via gh (read-only), plan lane specs, reconcile desired vs actual",
    )
    board_cmd.add_argument("--repo", help="OWNER/REPO for board export")
    board_cmd.add_argument("--board", help="board snapshot JSON file (plan/reconcile)")
    board_cmd.add_argument(
        "--loaded",
        type=int,
        default=None,
        help="loaded owned sessions for the capacity gate (plan/reconcile; "
        "default: live P4 lease count; see docs/capacity.md)",
    )
    board_cmd.add_argument(
        "--ceiling",
        type=int,
        default=None,
        help="owned-session ceiling for the capacity gate (plan/reconcile; "
        "default: host policy / M8S_SESSION_CEILING)",
    )
    board_cmd.add_argument(
        "--warn-at",
        type=int,
        default=None,
        help="pre-ceiling alert threshold (plan/reconcile; default: host policy "
        "/ M8S_SESSION_WARN_AT)",
    )
    board_cmd.add_argument("--out", help="write exported snapshot here (export)")
    board_cmd.add_argument("--limit", type=int, default=100, help="issues to export (export)")
    board_cmd.add_argument(
        "--events-limit", type=int, default=200, help="recent events to scan (reconcile)"
    )

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
        request = {
            "command": "launch",
            "name": args.name,
            "workspace": args.workspace,
            "prompt": args.prompt,
            "model": args.model,
            "reasoningEffort": args.reasoning_effort,
            "approvalMode": args.approval_mode,
        }
        budget = {}
        if args.max_tokens is not None:
            budget["maxTokens"] = args.max_tokens
        if args.max_context_tokens is not None:
            budget["maxContextTokens"] = args.max_context_tokens
        if args.models:
            budget["models"] = args.models
        if budget:
            try:
                request["budget"] = normalize_budget(budget)
            except ValueError as exc:
                raise SystemExit(f"invalid budget: {exc}")
        return request
    if command == "send":
        return {
            "command": "send",
            "session": args.session,
            "prompt": args.prompt,
            "reasoningEffort": args.reasoning_effort,
        }
    if command == "pending":
        return {"command": "pending", "session": args.session}
    if command == "retire":
        return {"command": "retire", "session": args.session, "force": args.force}
    if command == "unload":
        return {
            "command": "unload",
            "session": args.session,
            "reason": args.reason,
            "by": args.by,
        }
    if command == "health":
        return {"command": "health", "eventsLimit": args.events_limit}
    if command == "host":
        request: dict[str, Any] = {"command": "host", "action": args.action}
        if args.name:
            request["name"] = args.name
        if args.target:
            request["target"] = args.target
        if args.ssh_port is not None:
            request["sshPort"] = args.ssh_port
        if args.max_lanes is not None:
            request["maxLanes"] = args.max_lanes
        if args.remote_msp:
            request["remoteMsp"] = args.remote_msp
        return request
    if command == "bus":
        if args.action == "claim" and (not args.lane or not args.branch):
            raise SystemExit("bus claim requires --lane and --branch")
        if args.action == "release" and not args.branch:
            raise SystemExit("bus release requires --branch")
        if args.action == "intent" and not args.verb:
            raise SystemExit("bus intent requires --verb")
        request = {"command": "bus", "action": args.action}
        for key in (
            "host",
            "lane",
            "branch",
            "checkout",
            "session",
            "ttl",
            "verb",
            "detail",
            "subject",
            "limit",
        ):
            value = getattr(args, key, None)
            if value is not None:
                request[key] = value
        return request
    if command == "place":
        request = {"command": "place"}
        if args.branch:
            request["branch"] = args.branch
        if args.checkout:
            request["checkout"] = args.checkout
        if args.files:
            request["files"] = [f.strip() for f in args.files.split(",") if f.strip()]
        return request
    if command == "budget":
        request = {"command": "budget", "session": args.session}
        if args.clear:
            request["clear"] = True
            return request
        if args.max_tokens is not None:
            request["maxTokens"] = args.max_tokens
        if args.max_context_tokens is not None:
            request["maxContextTokens"] = args.max_context_tokens
        if args.models:
            request["models"] = args.models
        return request
    if command == "call":
        try:
            params = json.loads(args.params)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid --params JSON: {exc}")
        if not isinstance(params, dict):
            raise SystemExit("--params must be a JSON object")
        try:
            validate_call(args.method, params, args.command_id)
        except CallValidationError as exc:
            raise SystemExit(f"{exc.kind}: {exc}")
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
    if args.command == "board":
        # Planning is daemonless: the compiler reads the board snapshot
        # file plus the same file-backed lease/event state list/events
        # show, so it never needs the controller socket.
        try:
            result = board_main(args)
        except BoardError as exc:
            raise SystemExit(f"{exc.kind}: {exc}")
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
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
    if args.command == "health" and not args.json and result.get("ok", True):
        print(format_health(result.get("result", {})))
        return 0
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
