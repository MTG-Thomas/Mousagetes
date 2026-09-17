#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-or-later
set -euo pipefail

usage() {
  printf '%s\n' \
    'usage: muse-remote.sh list' \
    '       muse-remote.sh send SESSION_ID_OR_NAME < MESSAGE'
}

if ! command -v muse >/dev/null 2>&1; then
  printf '%s\n' 'muse is not available on PATH' >&2
  exit 127
fi

export MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=on
export MUSE_EXPERIMENTAL_LOCAL_SESSION_MESSAGING=1

case "${1:-}" in
  list)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    exec muse session-message list --json
    ;;
  send)
    if [[ $# -ne 2 || -z "${2:-}" ]]; then
      usage >&2
      exit 2
    fi
    exec muse session-message send --target "$2" --json
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
