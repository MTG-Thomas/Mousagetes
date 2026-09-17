#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Single-source version control for Mousagetes (stdlib only).

``__version__`` in ``scripts/muse-msp.py`` is canonical. ``pyproject.toml``
and the newest ``## vX.Y.Z`` CHANGELOG heading must equal it; in-progress
work accumulates under ``## Unreleased``.

Usage:
  scripts/bump-version.py --check                  verify the three agree
  scripts/bump-version.py --check-tag vX.Y.Z       verify a release tag matches
  scripts/bump-version.py --release X.Y.Z          set versions, rename
                                                   ## Unreleased -> ## vX.Y.Z
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
SCRIPT_VERSION = re.compile(r'^__version__\s*=\s*"([^"]+)"', re.MULTILINE)
PYPROJECT_VERSION = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)
CHANGELOG_HEADING = re.compile(r"^##\s+(v\d+\.\d+\.\d+|Unreleased)\s*$", re.MULTILINE)


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def read_script_version(root: Path) -> str:
    match = SCRIPT_VERSION.search((root / "scripts" / "muse-msp.py").read_text())
    if not match:
        raise ValueError("no __version__ found in scripts/muse-msp.py")
    return match.group(1)


def read_pyproject_version(root: Path) -> str:
    match = PYPROJECT_VERSION.search((root / "pyproject.toml").read_text())
    if not match:
        raise ValueError("no version found in pyproject.toml")
    return match.group(1)


def read_changelog_headings(root: Path) -> list[str]:
    text = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    return CHANGELOG_HEADING.findall(text)


def check_versions(root: Path) -> list[str]:
    """Return a list of mismatch descriptions (empty when consistent)."""
    errors: list[str] = []
    try:
        script = read_script_version(root)
    except ValueError as exc:
        return [str(exc)]
    try:
        project = read_pyproject_version(root)
    except ValueError as exc:
        return [str(exc)]
    if script != project:
        errors.append(f"script __version__ {script} != pyproject {project}")
    if not SEMVER.match(script):
        errors.append(f"script __version__ {script} is not semver")
    headings = read_changelog_headings(root)
    if f"v{script}" not in headings:
        errors.append(f"CHANGELOG has no ## v{script} heading (saw {headings[:3]})")
    return errors


def release_version(root: Path, version: str) -> None:
    """Set versions and promote ## Unreleased to ## vX.Y.Z."""
    if not SEMVER.match(version):
        raise ValueError(f"not semver: {version!r}")
    script_path = root / "scripts" / "muse-msp.py"
    text = script_path.read_text(encoding="utf-8")
    updated, count = SCRIPT_VERSION.subn(f'__version__ = "{version}"', text, count=1)
    if not count:
        raise ValueError("no __version__ found in scripts/muse-msp.py")
    script_path.write_text(updated, encoding="utf-8")
    project_path = root / "pyproject.toml"
    text = project_path.read_text(encoding="utf-8")
    updated, count = PYPROJECT_VERSION.subn(f'version = "{version}"', text, count=1)
    if not count:
        raise ValueError("no version found in pyproject.toml")
    project_path.write_text(updated, encoding="utf-8")
    changelog_path = root / "CHANGELOG.md"
    text = changelog_path.read_text(encoding="utf-8")
    if not re.search(r"^##\s+Unreleased\s*$", text, re.MULTILINE):
        raise ValueError("CHANGELOG has no ## Unreleased section to promote")
    changelog_path.write_text(
        re.sub(
            r"^##\s+Unreleased\s*$",
            f"## v{version}",
            text,
            count=1,
            flags=re.MULTILINE,
        ),
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true")
    group.add_argument("--check-tag", metavar="TAG")
    group.add_argument("--release", metavar="VERSION")
    args = parser.parse_args(argv)
    root = repo_root()
    if args.check:
        errors = check_versions(root)
        if errors:
            print("\n".join(errors))
            return 1
        print(f"versions consistent: {read_script_version(root)}")
        return 0
    if args.check_tag:
        expected = f"v{read_script_version(root)}"
        if args.check_tag != expected:
            print(f"tag {args.check_tag} != script version {expected}")
            return 1
        errors = check_versions(root)
        if errors:
            print("\n".join(errors))
            return 1
        print(f"tag ok: {args.check_tag}")
        return 0
    try:
        release_version(root, args.release)
    except ValueError as exc:
        print(str(exc))
        return 1
    print(f"released {args.release}: versions set, CHANGELOG promoted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
