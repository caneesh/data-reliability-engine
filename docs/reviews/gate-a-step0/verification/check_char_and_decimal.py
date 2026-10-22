# Gate A step 0: observed Spark 3.5.1 behaviour for Q-2 (CHAR read padding) and Q-3 (DECIMAL to STRING).
# Run: PYSPARK_PYTHON=.venv/bin/python .venv/bin/python <this file> (from an empty scratch directory).
# Observed 2026-10-21: CHAR(3) 'a' read as 'a  ' (3 bytes), readSideCharPadding true;
# DECIMAL: -0.50, 1.00E-8 (scientific notation), 12345678901234567890.12, 0.000.
from pyspark.sql import SparkSession
s = SparkSession.builder.master("local[1]").config("spark.sql.warehouse.dir", "./wh").config("spark.ui.enabled", "false").getOrCreate()
s.sql("DROP TABLE IF EXISTS c3"); s.sql("CREATE TABLE c3 (k CHAR(3)) USING parquet"); s.sql("INSERT INTO c3 VALUES ('a')")
r = s.sql("SELECT k, octet_length(k) AS n FROM c3").collect()[0]
print("CHAR(3) 'a' read as", repr(r.k), r.n, "readSideCharPadding", s.conf.get("spark.sql.readSideCharPadding"))
print(s.sql("SELECT CAST(CAST(-0.5 AS DECIMAL(10,2)) AS STRING) a, CAST(CAST(0.00000001 AS DECIMAL(20,10)) AS STRING) b, "
            "CAST(CAST(12345678901234567890.12 AS DECIMAL(38,2)) AS STRING) c, CAST(CAST(0 AS DECIMAL(10,3)) AS STRING) d").collect()[0])
