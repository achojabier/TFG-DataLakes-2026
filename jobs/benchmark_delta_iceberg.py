import os
import time
import csv
import statistics
from datetime import datetime
from pyspark.sql import SparkSession
from trino.dbapi import connect
import pandas as pd

MINIO_USER     = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin123")
MINIO_ENDPOINT = "http://minio:9000"

N_RUNS = 3

PAQUETES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
    "io.delta:delta-spark_2.12:3.2.0"
)

DELTA_BASE = "s3a://warehouse/delta"
DELTA_TIERS = {
    "players_30k":  f"{DELTA_BASE}/players_30k",
    "players_500k": f"{DELTA_BASE}/players_500k",
    "players_full": f"{DELTA_BASE}/players_full",
}

ICEBERG_TIERS = {
    "players_30k":  "iceberg.processed.players_eoinamoore",
    "players_500k": "iceberg.processed.players_500k",
    "players_full": "iceberg.processed.players_full",
}

QUERIES = [
    {
        "id":          "Q1_count",
        "description": "Full table scan — COUNT(*)",
        "category":    "Scan",
        "sql":         "SELECT COUNT(*) FROM {TABLE}",
    },
    {
        "id":          "Q2_filter",
        "description": "Selective filter — players with 30+ points",
        "category":    "Filter",
        "sql":         """
            SELECT firstname, lastname, playerteamname, points
            FROM {TABLE}
            WHERE points >= 30
            ORDER BY points DESC
        """,
    },
    {
        "id":          "Q3_aggregation",
        "description": "GROUP BY — avg points per team",
        "category":    "Aggregation",
        "sql":         """
            SELECT playerteamname,
                   COUNT(*)               AS games,
                   ROUND(AVG(points), 2)  AS avg_points,
                   ROUND(AVG(assists), 2) AS avg_assists
            FROM {TABLE}
            GROUP BY playerteamname
            ORDER BY avg_points DESC
        """,
    },
    {
        "id":          "Q4_window",
        "description": "Window function — rolling avg points per player",
        "category":    "Window",
        "sql":         """
            SELECT firstname, lastname, points,
                   ROUND(AVG(points) OVER (
                       PARTITION BY personid
                       ORDER BY gamedatetimeest
                       ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                   ), 2) AS rolling_avg
            FROM {TABLE}
            LIMIT 1000
        """,
    },
    {
        "id":          "Q5_multi_agg",
        "description": "Multi-column aggregation — shooting efficiency",
        "category":    "Aggregation",
        "sql":         """
            SELECT playerteamname,
                   ROUND(SUM(CAST(fieldgoalsmade AS DOUBLE)) /
                         NULLIF(SUM(fieldgoalsattempted), 0) * 100, 2) AS fg_pct,
                   ROUND(SUM(CAST(threepointersmade AS DOUBLE)) /
                         NULLIF(SUM(threepointersattempted), 0) * 100, 2) AS three_pct,
                   COUNT(DISTINCT personid) AS players_used
            FROM {TABLE}
            GROUP BY playerteamname
            ORDER BY fg_pct DESC
        """,
    },
]


def crear_spark():
    
    spark = SparkSession.builder \
        .appName("Delta_vs_Iceberg") \
        .config("spark.jars.packages", PAQUETES) \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.iceberg",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT) \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ingestar_delta(spark):
    print("\n" + "="*60)
    print("INGESTA DE DATOS EN DELTA LAKE")
    print("="*60)

    for tier_name, iceberg_table in ICEBERG_TIERS.items():
        delta_path = DELTA_TIERS[tier_name]
        print(f"\nCargando {tier_name}...")
        print(f"Origen  : {iceberg_table}")
        print(f"Destino : {delta_path}")

        t0 = time.perf_counter()
        df = spark.read.format("iceberg").load(iceberg_table)
        count = df.count()

        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(delta_path)

        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        print(f"Filas   : {count:,}")
        print(f"Tiempo  : {elapsed} ms")

    print("\nIngesta Delta Lake completada")


def benchmark_delta(spark):
    print("\n" + "="*60)
    print("BENCHMARK SPARK SQL + DELTA LAKE")
    print("="*60)

    results = []

    for tier_name, delta_path in DELTA_TIERS.items():
        tier_rows = {
            "players_30k":  30590,
            "players_500k": 389139,
            "players_full": 1662746,
        }[tier_name]

        print(f"\n[{tier_name}] {tier_rows:,} filas")

        df = spark.read.format("delta").load(delta_path)
        df.createOrReplaceTempView(f"delta_{tier_name}")

        for q in QUERIES:
            sql = q["sql"].replace("{TABLE}", f"delta_{tier_name}")
            times = []
            row_count = 0
            error = None

            for run in range(N_RUNS):
                try:
                    t0 = time.perf_counter()
                    result_df = spark.sql(sql)
                    row_count = result_df.count()
                    elapsed = round((time.perf_counter() - t0) * 1000, 2)
                    times.append(elapsed)
                    print(f"[{q['id']}] Run {run+1}: {elapsed} ms")
                except Exception as e:
                    error = str(e)
                    print(f"[{q['id']}] Run {run+1}: ERROR — {e}")
                    times.append(None)

            valid = [t for t in times if t is not None]
            results.append({
                "engine":      "Spark SQL + Delta",
                "format":      "Delta Lake",
                "tier":        tier_name,
                "rows":        tier_rows,
                "query_id":    q["id"],
                "category":    q["category"],
                "description": q["description"],
                "avg_ms":      round(statistics.mean(valid), 2) if valid else None,
                "min_ms":      round(min(valid), 2) if valid else None,
                "max_ms":      round(max(valid), 2) if valid else None,
                "stddev_ms":   round(statistics.stdev(valid), 2) if len(valid) > 1 else 0,
                "row_count":   row_count,
                "error":       error,
            })

    return results


def benchmark_iceberg(spark):
    print("\n" + "="*60)
    print("BENCHMARK SPARK SQL + ICEBERG (control)")
    print("="*60)

    results = []

    for tier_name, iceberg_table in ICEBERG_TIERS.items():
        tier_rows = {
            "players_30k":  30590,
            "players_500k": 389139,
            "players_full": 1662746,
        }[tier_name]

        print(f"\n[{tier_name}] {tier_rows:,} filas")

        for q in QUERIES:
            sql = q["sql"].replace("{TABLE}", iceberg_table)
            times = []
            row_count = 0
            error = None

            for run in range(N_RUNS):
                try:
                    t0 = time.perf_counter()
                    result_df = spark.sql(sql)
                    row_count = result_df.count()
                    elapsed = round((time.perf_counter() - t0) * 1000, 2)
                    times.append(elapsed)
                    print(f"[{q['id']}] Run {run+1}: {elapsed} ms")
                except Exception as e:
                    error = str(e)
                    print(f"[{q['id']}] Run {run+1}: ERROR — {e}")
                    times.append(None)

            valid = [t for t in times if t is not None]
            results.append({
                "engine":      "Spark SQL + Iceberg",
                "format":      "Iceberg",
                "tier":        tier_name,
                "rows":        tier_rows,
                "query_id":    q["id"],
                "category":    q["category"],
                "description": q["description"],
                "avg_ms":      round(statistics.mean(valid), 2) if valid else None,
                "min_ms":      round(min(valid), 2) if valid else None,
                "max_ms":      round(max(valid), 2) if valid else None,
                "stddev_ms":   round(statistics.stdev(valid), 2) if len(valid) > 1 else 0,
                "row_count":   row_count,
                "error":       error,
            })

    return results


def comparar_storage(spark):
    print("\n" + "="*60)
    print("COMPARACIÓN DE TAMAÑO EN STORAGE")
    print("="*60)

    sizes = []

    
    conn = connect(host="trino", port=8080, user="admin")
    iceberg_tables = [
        ("players_30k",  "iceberg.processed", "players_eoinamoore"),
        ("players_500k", "iceberg.processed", "players_500k"),
        ("players_full", "iceberg.processed", "players_full"),
    ]
    for tier_name, schema, table in iceberg_tables:
        try:
            cur = conn.cursor()
            cur.execute(
                f'SELECT sum(file_size_in_bytes) FROM {schema}."{table}$files"'
            )
            result = cur.fetchone()
            size_mb = round(result[0] / (1024 * 1024), 2) if result and result[0] else 0
            print(f"Iceberg {tier_name:<15}: {size_mb} MB")
            sizes.append({"format": "Iceberg", "tier": tier_name, "size_mb": size_mb})
        except Exception as e:
            print(f"Iceberg {tier_name}: ERROR — {e}")

    for tier_name, delta_path in DELTA_TIERS.items():
        try:
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs_uri = spark._jvm.org.apache.hadoop.fs.Path(delta_path).toUri()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(fs_uri, hadoop_conf)
            status = fs.listStatus(
                spark._jvm.org.apache.hadoop.fs.Path(delta_path)
            )
            total_bytes = sum(
                s.getLen() for s in status
                if str(s.getPath().getName()).endswith(".parquet")
            )
            size_mb = round(total_bytes / (1024 * 1024), 2)
            print(f"Delta:{tier_name:<15}: {size_mb} MB")
            sizes.append({"format": "Delta Lake", "tier": tier_name, "size_mb": size_mb})
        except Exception as e:
            print(f"Delta: {tier_name}: ERROR — {e}")

    return sizes



def guardar_resultados(all_results, storage_sizes):
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results_file = f"delta_vs_iceberg_{timestamp}.csv"
    fieldnames = ["engine", "format", "tier", "rows", "query_id", "category",
                  "description", "avg_ms", "min_ms", "max_ms", "stddev_ms",
                  "row_count", "error"]
    with open(results_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)
    print(f"\nResultados guardados en: {results_file}")

    storage_file = f"delta_storage_{timestamp}.csv"
    with open(storage_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["format", "tier", "size_mb"])
        writer.writeheader()
        writer.writerows(storage_sizes)
    print(f"Storage sizes guardados en: {storage_file}")

    print("\n" + "="*60)
    print("RESUMEN — Latencia media por formato y tier (ms)")
    print("="*60)
    df = pd.DataFrame(all_results)
    if not df.empty:
        pivot = df.groupby(["format", "tier"])["avg_ms"].mean().round(1).unstack("tier")
        print(pivot.to_string())

    print("\n" + "="*60)
    print("RESUMEN — Tamaño en storage (MB)")
    print("="*60)
    df_s = pd.DataFrame(storage_sizes)
    if not df_s.empty:
        pivot_s = df_s.pivot(index="format", columns="tier", values="size_mb")
        print(pivot_s.to_string())


if __name__ == "__main__":
    print("Benchmark — Delta Lake vs Iceberg")
    print(f"Timestamp : {datetime.now().isoformat()}")
    print(f"Tiers     : {len(DELTA_TIERS)}")
    print(f"Queries   : {len(QUERIES)}")
    print(f"Runs each : {N_RUNS}")

    spark = crear_spark()

    ingestar_delta(spark)

    results_delta = benchmark_delta(spark)

    results_iceberg = benchmark_iceberg(spark)

    storage_sizes = comparar_storage(spark)

    spark.stop()

    guardar_resultados(results_delta + results_iceberg, storage_sizes)

    print("\nComparación Delta Lake vs Iceberg completada.")