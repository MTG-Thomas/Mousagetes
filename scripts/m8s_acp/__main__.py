# SPDX-License-Identifier: AGPL-3.0-or-later
"""Entry point for ``python3 -m m8s_acp``."""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
