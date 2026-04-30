import sys
import time
import os
import csv
import statistics
from datetime import datetime
from trino.dbapi import connect
import pandas as pd

TRINO_HOST     = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT     = int(os.environ.get("TRINO_PORT", 8080))
TRINO_USER     = "admin"
MINIO_USER     = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin123")
MINIO_ENDPOINT = "http://minio:9000"
CSV_PATH = os.environ.get("CSV_PATH", "/home/iceberg/jobs/PlayerStatistics.csv")

N_RUNS = 3

TIERS = [
    {
        "name":       "Tier 1 — 30k",
        "table":      "iceberg.processed.players_eoinamoore",
        "csv_filter": ("2025-05-24", "2026-03-21"),
        "rows":       None,# will be filled in later by querying Iceberg metadata
    },
    {
        "name":       "Tier 2 — 389k",
        "table":      "iceberg.processed.players_500k",
        "csv_filter": ("2015-10-01", "2026-03-21"),
        "rows":       None,
    },
    {
        "name":       "Tier 3 — 1.66M",
        "table":      "iceberg.processed.players_full",
        "csv_filter": None,  # use full CSV
        "rows":       None,
    },
]



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
            LIMIT 100
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
            {
        "id":          "Q_skip_team",
        "description": "Data skipping — filter on one team (non‑partition column)",
        "category":    "Scan (filtered)",
        "sql":         """
            SELECT COUNT(*)
            FROM {TABLE}
            WHERE playerteamname = 'Boston Celtics'
        """,
    },
    {
        "id":          "Q_skip_player",
        "description": "Data skipping — filter on one player (non‑partition column)",
        "category":    "Scan (filtered)",
        "sql":         """
            SELECT COUNT(*)
            FROM {TABLE}
            WHERE personid = 2544
        """,
    },
]



def run_trino_tier(tier):
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    results = []

    for q in QUERIES:
        sql = q["sql"].replace("{TABLE}", tier["table"])
        times = []
        row_count = 0
        error = None

        for run in range(N_RUNS):
            try:
                cur = conn.cursor()
                t0 = time.perf_counter()
                cur.execute(sql)
                rows = cur.fetchall()
                bytes_scanned = None
                if q["id"] in ("Q_skip_team", "Q_skip_player"):
                    try:
                        query_id = cur.query_id
                        cur_stats = conn.cursor()
                        cur_stats.execute(
                            "SELECT physical_input_bytes FROM system.runtime.query_stats WHERE query_id = %s",
                            (query_id,),
                        )
                        row_stats = cur_stats.fetchone()
                        if row_stats and row_stats[0] is not None:
                            bytes_scanned = int(row_stats[0])
                    except Exception:
                        bytes_scanned = None
                elapsed = round((time.perf_counter() - t0) * 1000, 2)
                times.append(elapsed)
                row_count = len(rows)
                print(f"Run {run+1}: {elapsed} ms")
            except Exception as e:
                error = str(e)
                print(f"Run {run+1}: ERROR — {e}")
                times.append(None)

        valid = [t for t in times if t is not None]
        results.append({
            "engine":      "Trino",
            "tier":        tier["name"],
            "rows":        tier["rows"],
            "query_id":    q["id"],
            "category":    q["category"],
            "description": q["description"],
            "avg_ms":      round(statistics.mean(valid), 2) if valid else None,
            "min_ms":      round(min(valid), 2) if valid else None,
            "max_ms":      round(max(valid), 2) if valid else None,
            "stddev_ms":   round(statistics.stdev(valid), 2) if len(valid) > 1 else 0,
            "row_count":   row_count,
            "bytes_scanned": bytes_scanned,
            "error":       error,
        })

    return results


def run_spark_tier(spark, tier):
    results = []

    for q in QUERIES:
        sql = q["sql"].replace("{TABLE}", tier["table"])
        times = []
        row_count = 0
        error = None

        for run in range(N_RUNS):
            try:
                t0 = time.perf_counter()
                df = spark.sql(sql)
                rows = df.collect() 
                row_count = len(rows)  # force full execution
                bytes_scanned = None
                elapsed = round((time.perf_counter() - t0) * 1000, 2)
                times.append(elapsed)
                print(f"Run {run+1}: {elapsed} ms")
            except Exception as e:
                error = str(e)
                print(f"Run {run+1}: ERROR — {e}")
                times.append(None)

        valid = [t for t in times if t is not None]
        results.append({
            "engine":      "Spark SQL",
            "tier":        tier["name"],
            "rows":        tier["rows"],
            "query_id":    q["id"],
            "category":    q["category"],
            "description": q["description"],
            "avg_ms":      round(statistics.mean(valid), 2) if valid else None,
            "min_ms":      round(min(valid), 2) if valid else None,
            "max_ms":      round(max(valid), 2) if valid else None,
            "stddev_ms":   round(statistics.stdev(valid), 2) if len(valid) > 1 else 0,
            "row_count":   row_count,
            "bytes_scanned": bytes_scanned,
            "error":       error,
        })

    return results


def run_pandas_tier(tier, df):
    results = []

    print(f"Pandas slice rows: {len(df):,}")

    pandas_queries = [
        {
            "id": "Q1_count",
            "fn": lambda d: len(d),
        },
        {
            "id": "Q2_filter",
            "fn": lambda d: d[d["points"] >= 30].sort_values("points", ascending=False).head(100),
        },
        {
            "id": "Q3_aggregation",
            "fn": lambda d: d.groupby("playerteamName").agg(
                games=("points", "count"),
                avg_points=("points", "mean"),
                avg_assists=("assists", "mean"),
            ).round(2).sort_values("avg_points", ascending=False),
        },
        {
            "id": "Q4_window",
            "fn": lambda d: d.sort_values("gameDateTimeEst").assign(
                rolling_avg=d.groupby("personId")["points"]
                    .transform(lambda x: x.rolling(5, min_periods=1).mean().round(2))
            ).head(1000),
        },
        {
            "id": "Q5_multi_agg",
            "fn": lambda d: (
                d.groupby("playerteamName")
                .agg(
                    fg_made_sum      = ("fieldGoalsMade", "sum"),
                    fg_attempted_sum = ("fieldGoalsAttempted", "sum"),
                    three_made_sum   = ("threePointersMade", "sum"),
                    three_attempted_sum = ("threePointersAttempted", "sum"),
                    players_used     = ("personId", pd.Series.nunique)
                )
                .assign(
                    fg_pct    = lambda x: (x.fg_made_sum / x.fg_attempted_sum * 100).round(2).fillna(0),
                    three_pct = lambda x: (x.three_made_sum / x.three_attempted_sum * 100).round(2).fillna(0),
                )
                .drop(columns=["fg_made_sum", "fg_attempted_sum", "three_made_sum", "three_attempted_sum"])
                .sort_values("fg_pct", ascending=False)
            ),
        },
                {
            "id": "Q_skip_team",
            "fn": lambda d: len(d[d["playerteamName"] == "Boston Celtics"]),
        },
        {
            "id": "Q_skip_player",
            "fn": lambda d: len(d[d["personId"] == 2544]),
        },
    ]

    for q_sql, q_pd in zip(QUERIES, pandas_queries):
        times = []
        row_count = 0
        error = None

        for run in range(N_RUNS):
            try:
                t0 = time.perf_counter()
                result = q_pd["fn"](df)
                elapsed = round((time.perf_counter() - t0) * 1000, 2)
                times.append(elapsed)
                row_count = len(result) if hasattr(result, "__len__") else 1
                print(f"Run {run+1}: {elapsed} ms")
            except Exception as e:
                error = str(e)
                print(f"Run {run+1}: ERROR — {e}")
                times.append(None)

        valid = [t for t in times if t is not None]
        results.append({
            "engine":      "Pandas",
            "tier":        tier["name"],
            "rows":        tier["rows"],
            "query_id":    q_sql["id"],
            "category":    q_sql["category"],
            "description": q_sql["description"],
            "avg_ms":      round(statistics.mean(valid), 2) if valid else None,
            "min_ms":      round(min(valid), 2) if valid else None,
            "max_ms":      round(max(valid), 2) if valid else None,
            "stddev_ms":   round(statistics.stdev(valid), 2) if len(valid) > 1 else 0,
            "row_count":   row_count,
            "bytes_scanned": None,
            "error":       error,
        })

    return results


if __name__ == "__main__":
    print("TFG Data Lake — Scalability Benchmark")
    print(f"Timestamp : {datetime.now().isoformat()}")
    print(f"Tiers     : {len(TIERS)}")
    print(f"Queries   : {len(QUERIES)}")
    print(f"Runs each : {N_RUNS}")

    all_results = []

    print("\nWarming up Trino...")
    try:
        
        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
        for _ in range(3):
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM iceberg.processed.players_eoinamoore")
            cur.fetchall()
        print("Trino warm ✓")

        cur = conn.cursor()
        for tier in TIERS:
            cur.execute(f"SELECT COUNT(*) FROM {tier['table']}")
            tier["rows"] = cur.fetchone()[0]
            print(f"  {tier['name']}: {tier['rows']:,} rows")
        cur.close()
        print("Row counts resolved ✓")
    except Exception as e:
        print(f"Trino setup failed: {e}")
        print("Aborting benchmark – Trino is required.")
        sys.exit(1)



    # Load once
    if not os.path.isfile(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
    df_full = pd.read_csv(CSV_PATH, low_memory=False)
    df_full["gameDateTimeEst"] = pd.to_datetime(df_full["gameDateTimeEst"], errors="coerce")

    print("\n" + "="*60)
    print("TRINO BENCHMARK")
    print("="*60)
    for tier in TIERS:
        print(f"\n[{tier['name']}] {tier['rows']:,} rows")
        try:
            all_results += run_trino_tier(tier)
        except Exception as e:
            print(f"  ERROR: {e}")

    print("\n" + "="*60)
    print("SPARK SQL BENCHMARK")
    print("="*60)
    try:
        from pyspark.sql import SparkSession
        paquetes = (
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
        )
        spark = SparkSession.builder \
            .appName("Escalabilidad_Spark") \
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

        print("\nWarming up Spark...")
        try:
            warmup_table = TIERS[0]["table"] 
            for _ in range(3):
                spark.sql(f"SELECT COUNT(*) FROM {warmup_table}").collect()
            print("Spark warmed up")
        except Exception as e:
            print(f"Spark warmup failed: {e}")

        for tier in TIERS:
            print(f"\n[{tier['name']}] {tier['rows']:,} rows")
            try:
                all_results += run_spark_tier(spark, tier)
            except Exception as e:
                print(f"ERROR: {e}")

        spark.stop()
    except Exception as e:
        print(f"Spark benchmark failed: {e}")

    print("\n" + "="*60)
    print("PANDAS BENCHMARK")
    print("="*60)
    for tier in TIERS:
        print(f"\n  [{tier['name']}] {tier['rows']:,} rows")
        if tier["csv_filter"]:
            start, end = tier["csv_filter"]
            df_slice = df_full[(df_full["gameDateTimeEst"] >= start) & (df_full["gameDateTimeEst"] <= end)].copy()
        else:
            df_slice = df_full.copy()
        print(f"  Pandas slice rows: {len(df_slice):,}")
        try:
            all_results += run_pandas_tier(tier, df_slice)
        except Exception as e:
            print(f"ERROR: {e}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"scalability_results_{timestamp}.csv"

    fieldnames = ["engine", "tier", "rows", "query_id", "category",
                  "description", "avg_ms", "min_ms", "max_ms", "stddev_ms",
                  "row_count","bytes_scanned", "error"]
    with open(results_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)
    print(f"\n✅ Results saved to: {results_file}")

    print("\n" + "="*60)
    print("SUMMARY — avg latency (ms) per engine per tier")
    print("="*60)
    df_r = pd.DataFrame(all_results)
    if not df_r.empty:
        pivot = df_r.groupby(["engine", "tier"])["avg_ms"].mean().round(1).unstack("tier")
        print(pivot.to_string())

    print("\nScalability benchmark complete.")