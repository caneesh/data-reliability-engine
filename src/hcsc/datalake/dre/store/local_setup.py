"""Create the dq database for tests and local runs.

Only this module may create the database (guard tests enforce it), and no
engine module may import it: `dre run` never creates the database. On the
cluster the database is created by the platform team.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hcsc.datalake.dre.store.names import validate_dq_database

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def create_database(spark: SparkSession, dq_database: str) -> None:
    validate_dq_database(dq_database)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dq_database}")
