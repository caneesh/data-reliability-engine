"""Fixtures for the examination's isolated adversarial tests: the project's own local-mode Spark
session (tests/conftest.py). Run from the repository root:

    PYSPARK_PYTHON=$PWD/.venv/bin/python .venv/bin/python -m pytest -q -p no:cacheprovider \
        docs/reviews/reconciliation-examination/adversarial

These tests are outside the project's testpaths: they never run in its suite and change nothing in it.
"""

from tests.conftest import packaged_capabilities_only_afterwards, spark  # noqa: F401
