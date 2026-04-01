import os
from difflib import SequenceMatcher
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, split, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

MINIO_USER     = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin123")
MINIO_ENDPOINT = "http://minio:9000"
CSV_PATH       = "/home/iceberg/jobs/hoopshype_nba_salaries.csv"

# Minimum similarity score to accept a fuzzy match (0.0 - 1.0)
# 0.80 is conservative — catches "Santi/Santiago", "Jr."/"III" suffixes
# but avoids false positives between similar names
FUZZY_THRESHOLD = 0.80

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
).withColumn(
    "firstname", trim(split(col("fullname"), " ").getItem(0))
).withColumn(
    "lastname", trim(regexp_replace(col("fullname"), r"^\S+\s+", ""))
)

pdf_salaries = df_salaries.toPandas()
print(f"Salary rows loaded: {len(pdf_salaries)}")


print("Loading player registry from players_eoinamoore...")
pdf_registry = spark.sql("""
    SELECT DISTINCT personid, firstname, lastname
    FROM iceberg.processed.players_eoinamoore
""").toPandas()

# Build normalised full name for matching
pdf_registry["fullname_norm"] = (
    pdf_registry["firstname"].str.strip().str.lower() + " " +
    pdf_registry["lastname"].str.strip().str.lower()
)
print(f"Registry players: {len(pdf_registry)}")

print(f"Fuzzy matching names (threshold={FUZZY_THRESHOLD})...")

def best_match(fullname, registry_df):
    """Find the best matching personId for a player name."""
    name_norm = fullname.strip().lower()
    best_score = 0
    best_id = None
    best_name = None
    for _, row in registry_df.iterrows():
        score = SequenceMatcher(None, name_norm, row["fullname_norm"]).ratio()
        if score > best_score:
            best_score = score
            best_id = row["personid"]
            best_name = row["fullname_norm"]
    if best_score >= FUZZY_THRESHOLD:
        return best_id, best_name, best_score
    return None, None, best_score

matched    = 0
unmatched  = 0
results    = []
low_confidence = []

for _, row in pdf_salaries.iterrows():
    person_id, matched_name, score = best_match(row["fullname"], pdf_registry)
    results.append({
        "personid":   person_id,
        "firstname":  row["firstname"],
        "lastname":   row["lastname"],
        "salary_usd": row["salary_usd"],
    })
    if person_id:
        matched += 1
        # Log cases where name differed but still matched
        if row["fullname"].strip().lower() != (row["firstname"] + " " + row["lastname"]).strip().lower() or score < 0.95:
            if score < 0.95:
                low_confidence.append((row["fullname"], matched_name, round(score, 3)))
    else:
        unmatched += 1

coverage_pct = round(matched / len(pdf_salaries) * 100, 1)
print(f"\nFuzzy match results:")
print(f"Matched   : {matched} / {len(pdf_salaries)} ({coverage_pct}%)")
print(f"Unmatched : {unmatched} (personId will be NULL — G-League/two-way players)")

if low_confidence:
    print(f"\nLow-confidence matches (useful for thesis data quality section):")
    for salary_name, registry_name, score in low_confidence[:10]:
        print(f"'{salary_name}' → '{registry_name}' (score={score})")

print("\nWriting to iceberg.processed.dim_salaries...")

schema = StructType([
    StructField("personid",   StringType(), True),
    StructField("firstname",  StringType(), True),
    StructField("lastname",   StringType(), True),
    StructField("salary_usd", DoubleType(), True),
])

pdf_results = pd.DataFrame(results)
df_final = spark.createDataFrame(pdf_results, schema=schema)

df_final.writeTo("iceberg.processed.dim_salaries") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()

count = spark.sql("SELECT COUNT(*) FROM iceberg.processed.dim_salaries").collect()[0][0]
print(f"dim_salaries now has {count} rows")


print("\n📊 Final join coverage (personId-based):")
coverage = spark.sql("""
    SELECT
        COUNT(DISTINCT p.personid)                                                      AS players_in_boxscores,
        COUNT(DISTINCT CASE WHEN s.personid IS NOT NULL THEN p.personid END)            AS players_with_salary,
        ROUND(
            COUNT(DISTINCT CASE WHEN s.personid IS NOT NULL THEN p.personid END) * 100.0 /
            COUNT(DISTINCT p.personid), 1
        )                                                                               AS coverage_pct
    FROM iceberg.processed.players_eoinamoore p
    LEFT JOIN iceberg.processed.dim_salaries s
      ON p.personid = s.personid
""").collect()[0]

print(f"Players in box scores : {coverage[0]}")
print(f"Players with salary   : {coverage[1]}")
print(f"Coverage              : {coverage[2]}%")

print("\nSample unmatched players (G-League/two-way — no salary data available):")
spark.sql("""
    SELECT DISTINCT p.firstname, p.lastname
    FROM iceberg.processed.players_eoinamoore p
    LEFT JOIN iceberg.processed.dim_salaries s
      ON p.personid = s.personid
    WHERE s.personid IS NULL
    ORDER BY p.lastname
    LIMIT 10
""").show(truncate=False)

spark.stop()
print("\nSalary ingestion complete.")