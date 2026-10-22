"""Fixtures for the release N independent-review tests (the project's local-mode Spark session).
Run from the repository root, with BASELINE set to a worktree of 390b0bd (the engine before release N):
    BASELINE=<worktree> PYSPARK_PYTHON=$PWD/.venv/bin/python .venv/bin/python -m pytest -q -p no:cacheprovider \\
        docs/reviews/release-n/adversarial
Outside the project's testpaths; review evidence only, changing nothing in the engine."""

from tests.conftest import packaged_capabilities_only_afterwards, spark  # noqa: F401
