#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Tests for scripts/bump-version.py (stdlib only, fixture trees in /tmp)."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "bump-version.py"


def load_module():
    spec = importlib.util.spec_from_file_location("bump_version", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bump = load_module()


def fixture(root: Path, version: str = "0.5.0", heading: str = "## v0.5.0") -> None:
    (root / "scripts").mkdir(parents=True, exist_ok=True)
    (root / "scripts" / "muse-msp.py").write_text(
        f'__version__ = "{version}"\n', encoding="utf-8"
    )
    (root / "pyproject.toml").write_text(
        f'[project]\nversion = "{version}"\n', encoding="utf-8"
    )
    (root / "CHANGELOG.md").write_text(
        f"# Changelog\n\n{heading}\n\n- Something.\n", encoding="utf-8"
    )


class VersionCheckTest(unittest.TestCase):
    def test_consistent_tree_passes(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture(root)
            self.assertEqual(bump.check_versions(root), [])

    def test_mismatch_reported(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture(root)
            (root / "pyproject.toml").write_text(
                '[project]\nversion = "0.2.0"\n', encoding="utf-8"
            )
            errors = bump.check_versions(root)
            self.assertTrue(any("pyproject" in e for e in errors))

    def test_missing_heading_reported(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture(root, heading="## Unreleased")
            errors = bump.check_versions(root)
            self.assertTrue(any("CHANGELOG" in e for e in errors))


class ReleaseTest(unittest.TestCase):
    def test_release_sets_versions_and_promotes(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture(root, version="0.4.0", heading="## Unreleased")
            bump.release_version(root, "0.5.0")
            self.assertEqual(bump.check_versions(root), [])
            headings = bump.read_changelog_headings(root)
            self.assertIn("v0.5.0", headings)
            self.assertNotIn("Unreleased", headings)

    def test_release_rejects_non_semver(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture(root)
            with self.assertRaises(ValueError):
                bump.release_version(root, "nope")

    def test_release_without_unreleased_fails(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture(root)
            with self.assertRaises(ValueError):
                bump.release_version(root, "0.6.0")


class NextVersionTest(unittest.TestCase):
    def setUp(self) -> None:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "next_version", Path(__file__).resolve().parent.parent / "next-version.py"
        )
        self.mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.mod)

    def test_major_wins(self) -> None:
        self.assertEqual(
            self.mod.next_version(
                "0.5.0", [frozenset({"semver:patch"}), frozenset({"semver:major"})]
            ),
            "1.0.0",
        )

    def test_minor_on_feature_or_unknown(self) -> None:
        self.assertEqual(
            self.mod.next_version("0.5.0", [frozenset({"semver:minor"})]), "0.6.0"
        )
        self.assertEqual(
            self.mod.next_version("0.5.0", [frozenset({"bug"})]), "0.6.0"
        )
        self.assertEqual(self.mod.next_version("0.5.0", [frozenset()]), "0.6.0")

    def test_patch_only_when_all_patch(self) -> None:
        self.assertEqual(
            self.mod.next_version(
                "0.5.0", [frozenset({"semver:patch"}), frozenset({"semver:patch"})]
            ),
            "0.5.1",
        )

    def test_no_prs_means_nothing(self) -> None:
        self.assertIsNone(self.mod.next_version("0.5.0", []))

    def test_bad_base_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.mod.next_version("nope", [frozenset({"semver:patch"})])


if __name__ == "__main__":
    unittest.main()
