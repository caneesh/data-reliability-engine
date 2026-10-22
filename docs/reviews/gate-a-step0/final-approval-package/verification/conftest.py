"""Fixtures for the Gate A step 0 final-approval prototypes (the project's local-mode Spark session,
Hive support, Derby metastore in a temporary directory). Run from the repository root:
    PYSPARK_PYTHON=$PWD/.venv/bin/python .venv/bin/python -m pytest -q -s -p no:cacheprovider \
        docs/reviews/gate-a-step0/final-approval-package/verification
Outside the project's testpaths. Verification code only: it proves Spark 3.5.1 behaviour the
specification relies on and changes nothing in the engine."""

from tests.conftest import packaged_capabilities_only_afterwards, spark  # noqa: F401
