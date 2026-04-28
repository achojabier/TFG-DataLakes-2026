import os
from difflib import SequenceMatcher
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, split, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

MINIO_USER     = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin123")
MINIO_ENDPOINT = "http://minio:9000"

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
    .appName("Procesar_Salaries") \
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
print("Leyendo salarios crudos desde landing y transformando años a filas...")
# Usamos stack para coger las columnas de los años y convertirlas en filas (season, salary_usd)
df_salaries_unpivoted = spark.sql("""
    SELECT 
        fullname,
        playerteamName,
        season,
        CAST(salary_usd AS DOUBLE) as salary_usd
    FROM (
        SELECT 
            fullname,
            playerteamName,
            stack(6, 
                '2025-26', `2025-26`, 
                '2026-27', `2026-27`, 
                '2027-28', `2027-28`, 
                '2028-29', `2028-29`, 
                '2029-30', `2029-30`, 
                '2030-31', `2030-31`
            ) as (season, salary_usd)
        FROM iceberg.landing.dim_salaries_raw
    )
    WHERE salary_usd IS NOT NULL AND salary_usd > 0
""")

pdf_salaries = df_salaries_unpivoted.toPandas()

pdf_salaries["firstname"] = pdf_salaries["fullname"].str.split(" ").str[0].str.strip()
pdf_salaries["lastname"]  = pdf_salaries["fullname"].str.replace(r"^\S+\s+", "", regex=True).str.strip()

print("Leyendo registro de jugadores desde landing...")
pdf_registry = spark.sql("""
    SELECT DISTINCT personid, firstname, lastname
    FROM iceberg.landing.players_eoinamoore
""").toPandas()

pdf_registry["fullname_norm"] = (
    pdf_registry["firstname"].str.strip().str.lower() + " " +
    pdf_registry["lastname"].str.strip().str.lower()
)
print(f"Jugadores en registro: {len(pdf_registry)}")

def best_match(fullname, registry_df):
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

print(f"Fuzzy matching (threshold={FUZZY_THRESHOLD})...")
matched, unmatched, results, low_confidence = 0, 0, [], []

for _, row in pdf_salaries.iterrows():
    person_id, matched_name, score = best_match(row["fullname"], pdf_registry)
    if person_id:
        results.append({
            "personid":   int(person_id),
            "player_name": row["fullname"],
            "season":     row["season"],      
            "salary_usd": float(row["salary_usd"]),
            "playerteamName": row["playerteamName"]
        })
        matched += 1
        if score < 0.95:
            low_confidence.append((row["fullname"], matched_name, round(score, 3)))
    else:
        unmatched += 1

coverage_pct = round(matched / len(pdf_salaries) * 100, 1)
print(f"Matched   : {matched} / {len(pdf_salaries)} ({coverage_pct}%)")
print(f"Unmatched : {unmatched} (G-League/two-way players)")

if low_confidence:
    print("Low-confidence matches:")
    for salary_name, registry_name, score in low_confidence[:10]:
        print(f"  '{salary_name}' → '{registry_name}' (score={score})")

# EL ESQUEMA AHORA COINCIDE CON LA TABLA ICEBERG
schema = StructType([
    StructField("personid",   LongType(), True),
    StructField("player_name", StringType(), True),
    StructField("season",     StringType(), True),
    StructField("salary_usd", DoubleType(), True),
    StructField("playerteamName", StringType(), True),
])

df_final = spark.createDataFrame(results, schema=schema)

print("Escribiendo a iceberg.processed.dim_salaries (haciendo APPEND)...")
# Usamos append() porque la tabla ya está creada y gestionada por el DAG de Init
df_final.writeTo("iceberg.processed.dim_salaries") \
    .using("iceberg") \
    .append()

count = spark.sql("SELECT COUNT(*) FROM iceberg.processed.dim_salaries").collect()[0][0]
print(f"dim_salaries tiene {count} filas")

spark.stop()
print("Procesamiento de salarios completado.")