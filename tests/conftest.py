"""Shared pytest fixtures: a local-mode Spark session with Hive support.

The warehouse and the Derby metastore live in a temporary directory, so tests
never touch a real cluster and leave nothing behind in the repository.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
PACKAGE_ROOT = SRC_ROOT / "hcsc" / "datalake" / "dre"


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator["SparkSession"]:
    from pyspark.sql import SparkSession

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
