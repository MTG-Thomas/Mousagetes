#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Decide the next release version from merged-PR labels (stdlib only).

Label sets come from the release-draft workflow, which passes one
``--pr`` flag per merged PR (comma-separated label names, possibly empty).
Rules: any ``semver:major`` wins; else any ``semver:minor`` — or any PR
with no semver label at all — means minor; all-``semver:patch`` means
patch. No PRs means nothing to release (exit 2).

Usage:
  scripts/next-version.py --base 0.5.0 --pr "semver:minor,bug" --pr ""
"""

from __future__ import annotations

import argparse
import re

SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
KNOWN = frozenset({"semver:major", "semver:minor", "semver:patch"})


def next_version(base: str, label_sets: list[frozenset[str]]) -> str | None:
    """Return the next version, or None when there is nothing to release."""
    match = SEMVER.match(base)
    if not match:
        raise ValueError(f"not semver: {base!r}")
    if not label_sets:
        return None
    major, minor, patch = (int(part) for part in match.groups())
    flat = frozenset().union(*label_sets)
    if "semver:major" in flat:
        return f"{major + 1}.0.0"
    if "semver:minor" in flat or any(not labels & KNOWN for labels in label_sets):
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--pr", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        label_sets = [
            frozenset(part.strip() for part in entry.split(",") if part.strip())
            for entry in args.pr
        ]
        version = next_version(args.base, label_sets)
    except ValueError as exc:
        print(str(exc))
        return 1
    if version is None:
        print("nothing to release")
        return 2
    print(version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
