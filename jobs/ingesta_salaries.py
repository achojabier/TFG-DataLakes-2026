import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from spark.spark_utils import get_spark_session
CSV_PATH       = "/opt/airflow/jobs/hoopshype_nba_salaries.csv"


spark = get_spark_session("Ingesta_Salaries")

print("Loading salary CSV...")
df_raw = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

df_salaries = df_raw.select(
    trim(col("player")).alias("fullname"),
    col("team").alias("playerteamName"),
    col("`2025-26`").cast("double").alias("2025-26"),
    col("`2026-27`").cast("double").alias("2026-27"),
    col("`2027-28`").cast("double").alias("2027-28"),
    col("`2028-29`").cast("double").alias("2028-29"),
    col("`2029-30`").cast("double").alias("2029-30"),
    col("`2030-31`").cast("double").alias("2030-31")
)

print("Escribiendo a iceberg.landing.dim_salaries_raw...")
df_salaries.writeTo("iceberg.landing.dim_salaries_raw") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

count = spark.sql("SELECT COUNT(*) FROM iceberg.landing.dim_salaries_raw").collect()[0][0]
print(f"dim_salaries_raw tiene {count} filas")

spark.stop()
print("Ingesta cruda de salarios completada.")