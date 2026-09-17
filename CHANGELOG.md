# Changelog

## v0.1.0

- Initial public release (AGPL-3.0-or-later).
- `muse-msp.py` (`m8s`): daemon owning one `muse serve` host with a Unix-socket control API.
- Curated lane commands (`launch`, `send`, `list`, `events`, `watch`, `pending`,
  `read`, `view`, `goal`, `turn`, `workflow`, `subagent`, `task`, session lifecycle).
- Generic `call` passthrough covering all 51 MSP schema methods with automatic
  `commandId` minting.
- `muse-remote.sh` for direct local session messaging without the daemon.
- Stdlib-only test suite (`python3 -m unittest discover -s scripts/tests`).
