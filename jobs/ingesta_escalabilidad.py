import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

MINIO_USER     = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin123")
MINIO_ENDPOINT = "http://minio:9000"
CSV_PATH       = "/home/iceberg/jobs/PlayerStatistics.csv"

paquetes = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
)

spark = SparkSession.builder \
    .appName("Ingesta_Escalabilidad") \
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

# ─── Load full CSV ─────────────────────────────────────────────────────────────

print("📁 Loading full PlayerStatistics.csv...")
df_raw = spark.read.csv(CSV_PATH, header=True, inferSchema=False)

# Cast columns explicitly — historical data has mixed types so we cast
# everything and let nulls fall where they may (documented in thesis)
df = df_raw.select(
    col("firstName").cast(StringType()),
    col("lastName").cast(StringType()),
    col("personId").cast(StringType()),
    col("gameId").cast(StringType()),
    col("gameDateTimeEst").cast(StringType()),
    col("playerteamName").cast(StringType()),
    col("opponentteamName").cast(StringType()),
    col("gameType").cast(StringType()),
    col("gameLabel").cast(StringType()),
    col("win").cast(IntegerType()),
    col("home").cast(IntegerType()),
    col("numMinutes").cast(IntegerType()),
    col("points").cast(IntegerType()),
    col("assists").cast(IntegerType()),
    col("blocks").cast(IntegerType()),
    col("steals").cast(IntegerType()),
    col("fieldGoalsAttempted").cast(IntegerType()),
    col("fieldGoalsMade").cast(IntegerType()),
    col("threePointersAttempted").cast(IntegerType()),
    col("threePointersMade").cast(IntegerType()),
    col("freeThrowsAttempted").cast(IntegerType()),
    col("freeThrowsMade").cast(IntegerType()),
    col("reboundsDefensive").cast(IntegerType()),
    col("reboundsOffensive").cast(IntegerType()),
    col("foulsPersonal").cast(IntegerType()),
    col("turnovers").cast(IntegerType()),
    col("plusMinus").cast(IntegerType()),
)

total = df.count()
print(f"Total rows loaded: {total:,}")

# Document null rates for thesis data quality section
print("\nNull rate per column (data quality report):")
for c in ["points", "assists", "rebounds_total", "gameType", "win", "numMinutes"]:
    if c == "rebounds_total":
        null_count = df.filter(
            col("reboundsDefensive").isNull() & col("reboundsOffensive").isNull()
        ).count()
    else:
        null_count = df.filter(col(c).isNull()).count()
    pct = round(null_count / total * 100, 2)
    print(f"{c}: {null_count:,} nulls ({pct}%)")

# ─── Tier 2: ~500k rows (2015-16 season onwards) ──────────────────────────────

print("\nCreating players_500k (2015-16 season onwards)...")
df_500k = df.filter(
    col("gameDateTimeEst") >= "2015-10-01"
)
count_500k = df_500k.count()
print(f"Rows: {count_500k:,}")

df_500k.writeTo("iceberg.processed.players_500k") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

print(f"players_500k created: {count_500k:,} rows")

# ─── Tier 3: Full dataset (1947-present) ──────────────────────────────────────

print("\nCreating players_full (full 1947-present dataset)...")
df.writeTo("iceberg.processed.players_full") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

print(f"players_full created: {total:,} rows")

# ─── Summary ──────────────────────────────────────────────────────────────────

print("\n" + "="*50)
print("SCALABILITY TIERS SUMMARY")
print("="*50)
current = spark.sql("SELECT COUNT(*) FROM iceberg.processed.players_eoinamoore").collect()[0][0]
print(f"Tier 1 — players_eoinamoore : {current:,} rows  (current season)")
print(f"Tier 2 — players_500k       : {count_500k:,} rows  (2015-present)")
print(f"Tier 3 — players_full       : {total:,} rows (1947-present)")
print(f"Scale factor T1→T3          : {round(total/current)}×")

spark.stop()
print("\nScalability dataset ingestion complete.")