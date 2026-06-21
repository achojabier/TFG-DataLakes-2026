import os
import csv
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from spark.spark_utils import get_spark_session

spark = get_spark_session("Ingesta_Escalabilidad")
CSV_PATH       = "/home/iceberg/jobs/PlayerStatistics.csv"


print("Loading full PlayerStatistics.csv...")
df_raw = spark.read.csv(CSV_PATH, header=True, inferSchema=False)

df = df_raw.select(
    col("firstName").cast(StringType()),
    col("lastName").cast(StringType()),
    col("personId").cast(IntegerType()),
    col("gameId").cast(IntegerType()),
    col("gameDateTimeEst").cast("timestamp").alias("gamedatetimeest"),
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
    col("plusMinusPoints").cast(IntegerType()).alias("plusMinus"),
)

total = df.count()
print(f"Total rows loaded: {total:,}")


print("\nNull rate per column (data quality report):")
null_report = []
columns_to_check = [
    ("points", "points"),
    ("assists", "assists"),
    ("rebounds_total", "reboundsDefensive / reboundsOffensive (calculated)"),
    ("gameType", "gameType"),
    ("win", "win"),
    ("numMinutes", "numMinutes"),
]

for col_alias, col_desc in columns_to_check:
    if col_alias == "rebounds_total":
        null_count = df.filter(
            col("reboundsDefensive").isNull() & col("reboundsOffensive").isNull()
        ).count()
    else:
        null_count = df.filter(col(col_alias).isNull()).count()
    pct = round(null_count / total * 100, 2)
    print(f"{col_alias}: {null_count:,} nulls ({pct}%)")
    null_report.append({
        "column": col_alias,
        "description": col_desc,
        "null_count": null_count,
        "null_percentage": pct
    })

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
null_report_file = f"/home/iceberg/jobs/null_rates_{timestamp}.csv"
with open(null_report_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["column", "description", "null_count", "null_percentage"])
    writer.writeheader()
    writer.writerows(null_report)
print(f"\nNull rate report saved to: {null_report_file}")


print("\nCreating players_500k (2015-16 season onwards)...")
df_500k = df.filter(
    col("gamedatetimeest") >= "2015-10-01"
)
count_500k = df_500k.count()
print(f"Rows: {count_500k:,}")

df_500k.writeTo("iceberg.processed.players_500k") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .option("write.partitioning", "months(gamedatetimeest)") \
    .option("write.sort.order", "playerteamName ASC, gameDateTimeEst DESC, personId ASC") \
    .createOrReplace()

print(f"players_500k created: {count_500k:,} rows")


print("\nCreating players_full (full 1947-present dataset)...")
df.writeTo("iceberg.processed.players_full") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .option("write.partitioning", "years(gamedatetimeest)") \
    .option("write.sort.order", "playerteamName ASC, gameDateTimeEst DESC, personId ASC") \
    .createOrReplace()

print(f"players_full created: {total:,} rows")


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