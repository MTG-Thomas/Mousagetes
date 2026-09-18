<!-- SPDX-License-Identifier: AGPL-3.0-or-later -->

# Transcript fixtures

Verbatim copies of golden MSP wire transcripts from the Meta SDK mirror
(`github.com/meta-models/muse-code-sdk`, `schema/msp/transcripts/`),
mirrored at:

- commit `4dc252c1a31b02ff676dce0358d297ffbc5d8784` (same pin as the
  `muse-code-msp` dependency in `pyproject.toml`)

Each `<scenario>/` directory holds the fixture's `transcript.ndjson`
(wire frames as `{dir, raw}` records) and its `manifest.json`
(provenance). Never hand-edit these files: `test_transcripts.py`
replays the server-direction bytes through the real `MspHost` framing
and event handling instead of hand-mocked wire bytes.

Update procedure: re-copy the scenario directories from the SDK mirror
at the new pin, re-run the suite, and adjust `test_transcripts.py`
only if the wire itself changed.
