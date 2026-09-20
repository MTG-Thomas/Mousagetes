<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# ACP replay fixtures

Recorded MSP traffic for the ACP adapter's replay tests (WS-D in
[docs/acp.md](../../../docs/acp.md)). Nothing here is hand-authored:
capture real daemon output so translation tests assert against what Muse
actually emits.

## Format

- One scenario per pair of files:
  - `<scenario>.msp.ndjson` — the daemon records, one JSON object per line,
    exactly as read from `watch`/`events` for one lane.
  - `<scenario>.acp.ndjson` — the ACP notifications the adapter is expected
    to emit for that input, in order.
- Scenario names describe the behavior, not the session: `turn-stream`,
  `approval-request`, `user-input`, `set-model`, `daemon-bounce`.
- Every fixture records the MSP schema fingerprint from the host
  `initialize` so a schema change is visible, not silent.

## Capture

Record with a real lane, never by construction. `events` is bounded and
exits, so it is safe to redirect; filter the records to one lane by
`sessionId` when writing the fixture:

```bash
scripts/m8s events --limit 10000 > turn-stream.msp.ndjson
```

For a full per-lane transcript slice, use `scripts/m8s read <lane>` from
a lane that already completed the turn.

## Status

Empty until WS-D runs. The stub in `scripts/m8s_acp/stub.py` is the
transport peer used before recorded fixtures exist.
