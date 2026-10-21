"""Fixtures for the Gate A design's verification prototypes (the project's local-mode Spark session).
Run from the repository root:
    PYSPARK_PYTHON=$PWD/.venv/bin/python .venv/bin/python -m pytest -q -s -p no:cacheprovider \
        docs/reviews/gate-a-remediation-design/verification
Outside the project's testpaths; they prove design assumptions in Spark 3.5.1 and change nothing."""

from tests.conftest import packaged_capabilities_only_afterwards, spark  # noqa: F401
