<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# 0003. Remote clients get no filesystem or terminal capabilities

- Status: Accepted
- Date: 2026-09-19

## Context

ACP lets an agent ask its client to read/write files (`fs/*`) and to run
commands in a client-provided terminal (`terminal/*`). A phone or browser
is remote and less trusted than the host, and a mobile client cannot
usefully proxy host files. Muse already edits files in its own sandbox
and runs its own shell, so the client is not needed for either.

## Decision

The m8s adapter advertises `fs.readTextFile: false`,
`fs.writeTextFile: false`, and no terminal capability, and never issues
`fs/*` or `terminal/*` requests. Remote interaction is limited to
prompts, approvals, user-input answers, control commands, and rendered
output (messages, tool calls, plans, diffs as content).

## Consequences

- The remote attack surface stays small: a leaked token grants session
  steering and approvals, not host filesystem access.
- No client-side editing or review buffers; edits are reported as tool
  output. If editor-style review is wanted later, it is a new decision.
- Aligns with how mobile/web ACP clients already behave (they disable
  filesystem RPCs), so behavior is consistent across clients.
- File contents the agent chooses to show still reach the client as
  message/tool content; this decision constrains RPC capability, not
  what the agent reports.
