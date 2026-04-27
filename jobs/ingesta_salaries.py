import os
from difflib import SequenceMatcher
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, split, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

MINIO_USER     = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin123")
MINIO_ENDPOINT = "http://minio:9000"
CSV_PATH       = "/opt/airflow/jobs/hoopshype_nba_salaries.csv"


paquetes = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
)

spark = SparkSession.builder \
    .appName("Ingesta_Salaries") \
    .config("spark.jars.packages", paquetes) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT) \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Loading salary CSV...")
df_raw = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

df_salaries = df_raw.select(
    trim(col("player")).alias("fullname"),
    col("`2025-26`").cast("double").alias("salary_usd")
).filter(
    col("salary_usd").isNotNull() & (col("salary_usd") > 0)
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