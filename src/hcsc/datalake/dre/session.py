"""The Spark session the engine runs in."""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def use_utc_clock() -> None:
    """Run this Python process in UTC.

    PySpark turns TIMESTAMP values into naive datetimes in the driver process's local time
    zone (not spark.sql.session.timeZone), and reads naive datetimes it is given the same
    way. The engine treats naive datetimes as UTC (checks/times.as_utc), so on a host whose
    clock is not UTC every time read back from Spark would be shifted by the host's offset.
    """
    if os.environ.get("TZ") != "UTC":
        os.environ["TZ"] = "UTC"
        if hasattr(time, "tzset"):
            time.tzset()


def get_spark(app_name: str = "dre") -> SparkSession:
    """The active session (tests, spark-submit) or a new one with Hive support, in UTC.

    Event windows and run dates are UTC (spec section 4); a session the engine
    creates itself uses UTC, and the driver process runs in UTC (use_utc_clock).
    Under spark-submit, pass --conf spark.sql.session.timeZone=UTC.
    """
    from pyspark.sql import SparkSession

    use_utc_clock()
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )
