# Moonshot: Kubernetes for Agentic Engineering

Mousagetes (`m8s`) supervises live coding sessions today. The moonshot is a
control plane where a human manages the *project* from GitHub while agents
and their supervisors handle the grunt work: enroll a host, and an
appropriate number of agents become available for designated projects, with
agents and MSPs coordinating toward a GitHub project, its milestones, and
its issues.

## Concept map

| Kubernetes              | m8s moonshot                                              |
| ----------------------- | --------------------------------------------------------- |
| Node                    | Enrolled host running one m8s agent, heartbeating capacity (CPU, model budget, context) |
| Pod                     | Lane = session + worktree + branch, scheduled as one unit |
| Scheduler               | Places lanes by collision zone (files/branches), checkout affinity, and capacity |
| Controllers / reconcile | Heartbeat loops driving actual state toward declared desired state (merge-queue shepherd, CI watcher, stale-lane reaper) |
| Desired state           | GitHub project + milestones + issues compiled into lane specs |
| etcd                    | Git itself: branches, PRs, scope files, issue state       |
| kubectl                 | The `m8s` CLI                                             |

## Transport: one host, one client

`muse serve` is stdio-only by design — the client owns the process's stdin
and stdout and is its only connection, and sandbox posture is fixed at
launch. There is no TCP/port/bind option. Consequences:

- The Unix socket in m8s is ours (the control API), not MSP's. Remote
  enrollment means carrying serve stdio over SSH — the only sanctioned
  carrier until the CLI documents another transport. Never expose MSP on a
  raw network listener.
- A second supervisor cannot attach to the same serve process. Each enrolled
  host runs exactly one m8s agent owning exactly one serve process, and
  federation happens agent-to-agent, never by sharing a host.

## Execution: Sandcastle-shaped pods

Each lane becomes an isolated sandbox + agent + branch, with commits
returning as PRs ([Sandcastle](https://github.com/mattpocock/sandcastle) is
the reference pod runtime: per-task isolated containers, worktree and branch
management, commits back). M8s does not compete with it — m8s drives such
runs as its execution backend, one sandboxed run per lane spec.

## Comms: ownership and intent gossip

Two layers:

- **Directed messaging** — [Google A2A (v1.0)](https://github.com/google/A2A):
  `AgentCard` discovery plus a task lifecycle
  (`submitted → working → completed/failed/canceled`) for delegating,
  querying, or cancelling work node-to-node.
- **Presence broadcast** — gossip-style ownership claims
  ("host B holds lane X on branch Y"), re-gossiped on heartbeat with
  lease expiry into reassignment. [ANP](https://agentnetworkprotocol.com/)
  covers capability advertisement.

Pragmatic order: start with a boring bus (NATS subjects such as
`m8s.claims`, `m8s.heartbeat`, `m8s.intent` — or git itself as the mailbox)
between cooperative, mutually-trusting nodes. Design messages
protocol-shaped from day one (versioned claim/heartbeat/intent types) so
graduating to gossipsub/A2A later is a transport change, not a redesign.

## Self-organization: outrunning human attention

The end-state is a swarm that develops a project further than one human's
attention could steer. Each human job needs a non-human substrate:

- **Direction** — roadmap, lexicon, and ADRs as the constitution, plus
  deterministic priority arbitration (priority × dependency depth ×
  staleness). Auditable beats consensual.
- **Contention** — branch-level leases with heartbeats and expiry. One
  writer per checkout, one lane per branch; requeue on expiry.
- **Stewardship** — gates written as checkable invariants (Free tier, org
  boundaries, single authoritative paths, phase stops). Humans get paged
  only for the decision class: product calls, auth boundaries, spend.
- **Memory** — lane briefs, scope files, review threads, merge history. A
  node joining mid-project reconstructs *why* from the repo alone.

Failure mode to design against is **drift**, not rebellion: busy nodes on
`priority-low` work while the roadmap's center rots. Antidotes are a
planning cadence (nodes re-derive priorities from the board on schedule)
and visible global state (one screen showing swarm sanity).

## Positioning

[OpenAI's Symphony](https://www.infoworld.com/article/4164173/openais-symphony-spec-pushes-coding-agents-from-prompts-to-orchestration.html)
(open spec: issue trackers as control planes for coding agents, separate
workspaces, CI monitoring, human review) describes what supervised lanes
already do. M8s is the backend-agnostic superset: heterogeneous agents via
MSP, multi-host placement, desired-state reconciliation off the project
board.

For truly vibe-coded targets (greenfield, free-tier, fully verified,
totally reversible), trust is cheap and taste is the constraint: the
supervisor's job is editorial — Curator, not guard. Curation (scope files,
roadmaps, decision-blocked stops, steward gates) replaces compliance.

## End-state

The human checks the project board over coffee. Everything green traces
back to a scope file and a merged PR. The only work on their plate is what
the gates escalated. The swarm doesn't replace judgment — it rations it.

## Phased build order

1. More lanes on one host (done — this is m8s today).
2. Host enrollment over SSH with placement-aware dispatch.
3. Claim/heartbeat/intent bus between nodes.
4. Board-compiled lane specs (project → lanes compiler with collision and
   decision-blocked rules).
5. Self-serve planning cadence and global swarm dashboard.
