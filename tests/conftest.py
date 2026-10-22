"""Shared pytest fixtures: a local-mode Spark session with Hive support.

The warehouse and the Derby metastore live in a temporary directory, so tests
never touch a real cluster and leave nothing behind in the repository.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
PACKAGE_ROOT = SRC_ROOT / "hcsc" / "datalake" / "dre"


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator["SparkSession"]:
    from pyspark.sql import SparkSession

    from hcsc.datalake.dre.session import use_utc_clock

    use_utc_clock()  # as get_spark does: Spark hands back times in the process's local zone
    # Python workers must run the same interpreter as the driver (the test runner).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    base = tmp_path_factory.mktemp("spark")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("dre-tests")
        .config("spark.sql.warehouse.dir", str(base / "warehouse"))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={base / 'metastore_db'};create=true",
        )
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={base}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def packaged_capabilities_only_afterwards():
    """A test that loads a configuration leaves its patterns, templates, calendars and extensions
    behind; the registries go back to the packaged capabilities after every test."""
    yield
    from hcsc.datalake.dre.config.loader import reset_conf_state

    reset_conf_state()
