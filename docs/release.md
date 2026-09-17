# Release process

Work lands on `main` under a `## Unreleased` CHANGELOG section. In the
steady state nobody cuts releases by hand:

1. **Label merged PRs** with `semver:patch`, `semver:minor`, or
   `semver:major` (unlabeled PRs count as minor). This is the only
   manual step, and it is the version-numbering decision.
2. **Release draft** (`.github/workflows/release-draft.yml`): each push
   to `main` with a non-empty `## Unreleased` recomputes the next
   version from merged-PR labels since the last tag
   (`scripts/next-version.py`), runs `bump-version.py --release`, and
   opens or updates a `Release vNEXT` PR. Superseded drafts close
   themselves.
3. **Merge the release PR** when it looks right — that merge is the
   human gate. `release-tag.yml` then cuts tag `vNEXT`, and
   `release.yml` verifies the tag, extracts the CHANGELOG notes, and
   publishes the GitHub release.

Manual escape hatch (unchanged): `bump-version.py --release X.Y.Z`,
commit `Release vX.Y.Z`, `git tag vX.Y.Z && git push origin vX.Y.Z`.

Version sources: `__version__` in `scripts/muse-msp.py` is canonical;
CI enforces it against `pyproject.toml` and the newest `## vX.Y.Z`
heading on every push and PR.
