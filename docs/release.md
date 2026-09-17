# Release process

Work lands on `main` under a `## Unreleased` CHANGELOG section. Cutting a
release is three operator steps — everything else is automated:

1. `python3 scripts/bump-version.py --release X.Y.Z` — sets
   `__version__` (canonical, in `scripts/muse-msp.py`) and
   `pyproject.toml`, promotes `## Unreleased` to `## vX.Y.Z`.
2. Commit as `Release vX.Y.Z`, push.
3. `git tag vX.Y.Z && git push origin vX.Y.Z` — the release workflow
   verifies the tag against the three version sources, extracts the
   CHANGELOG section as notes, and publishes the GitHub release.

CI enforces version consistency on every push and PR
(`bump-version.py --check`): script, pyproject, and newest
`## vX.Y.Z` heading must agree. The release workflow
(`.github/workflows/release.yml`) refuses tags that do not match.
