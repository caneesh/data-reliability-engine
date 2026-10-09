"""The Spark session the engine runs in."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_spark(app_name: str = "dre") -> SparkSession:
    """The active session (tests, spark-submit) or a new one with Hive support, in UTC.

    Event windows and run dates are UTC (spec section 4); a session the engine
    creates itself uses UTC. Under spark-submit, pass
    --conf spark.sql.session.timeZone=UTC.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )
