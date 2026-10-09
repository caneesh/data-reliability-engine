"""The Spark session the engine runs in."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_spark(app_name: str = "dre") -> SparkSession:
    """The active session (tests, spark-submit) or a new one with Hive support."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
