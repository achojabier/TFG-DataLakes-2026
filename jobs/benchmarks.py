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

N_RUNS = 3

QUERIES_TRINO = [
    {
        "id": "Q1_count",
        "description": "Full table scan — COUNT(*) on box scores",
        "category": "Scan",
        "sql": "SELECT COUNT(*) FROM iceberg.processed.players_eoinamoore",
    },
    {
        "id": "Q2_filter",
        "description": "Selective filter — players with 30+ points",
        "category": "Filter",
        "sql": """
            SELECT firstname, lastname, playerteamname, points, gameid
            FROM iceberg.processed.players_eoinamoore
            WHERE points >= 30
            ORDER BY points DESC
        """,
    },
    {
        "id": "Q3_aggregation",
        "description": "GROUP BY aggregation — avg points per team",
        "category": "Aggregation",
        "sql": """
            SELECT playerteamname,
                   COUNT(*)                      AS games_played,
                   ROUND(AVG(points), 2)         AS avg_points,
                   ROUND(AVG(assists), 2)        AS avg_assists,
                   ROUND(AVG(rebounds_total), 2) AS avg_rebounds
            FROM (
                SELECT playerteamname, points, assists,
                       reboundsdefensive + reboundsoffensive AS rebounds_total
                FROM iceberg.processed.players_eoinamoore
            )
            GROUP BY playerteamname
            ORDER BY avg_points DESC
        """,
    },
    {
        "id": "Q4_window",
        "description": "Window function — running avg points per player",
        "category": "Window",
        "sql": """
            SELECT firstname, lastname, gameid, points,
                   ROUND(AVG(points) OVER (
                       PARTITION BY personid
                       ORDER BY gamedatetimeest
                       ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                   ), 2) AS rolling_avg_5games
            FROM iceberg.processed.players_eoinamoore
            ORDER BY personid, gamedatetimeest
            LIMIT 1000
        """,
    },
    {
        "id": "Q5_join_schedule",
        "description": "JOIN — box scores joined with schedule",
        "category": "Join",
        "sql": """
            SELECT p.firstname, p.lastname, p.points, p.assists,
                   s.arenaname, s.arenacity, s.gamelabel
            FROM iceberg.processed.players_eoinamoore p
            JOIN iceberg.processed.dim_schedule s
              ON CAST(p.gameid AS INTEGER) = s.gameid
            WHERE p.points > 20
            ORDER BY p.points DESC
            LIMIT 500
        """,
    },
    {
        "id": "Q6_join_advanced",
        "description": "JOIN — box scores joined with advanced stats",
        "category": "Join",
        "sql": """
            SELECT p.firstname, p.lastname, p.points,
                   a.tspct, a.pie, a.netrating, a.usgpct
            FROM iceberg.processed.players_eoinamoore p
            JOIN iceberg.processed.dim_advanced_stats a
              ON CAST(p.personid AS INTEGER) = a.personid
             AND p.gameid = CAST(a.gameid AS VARCHAR)
            LIMIT 1000
        """,
    },
    {
        "id": "Q7_multi_agg",
        "description": "Multi-column aggregation — shooting efficiency per team",
        "category": "Aggregation",
        "sql": """
            SELECT playerteamname,
                   ROUND(SUM(CAST(fieldgoalsmade AS DOUBLE)) /
                         NULLIF(SUM(fieldgoalsattempted), 0) * 100, 2) AS fg_pct,
                   ROUND(SUM(CAST(threepointersmade AS DOUBLE)) /
                         NULLIF(SUM(threepointersattempted), 0) * 100, 2) AS three_pct,
                   ROUND(SUM(CAST(freethrowsmade AS DOUBLE)) /
                         NULLIF(SUM(freethrowsattempted), 0) * 100, 2) AS ft_pct,
                   COUNT(DISTINCT personid) AS players_used
            FROM iceberg.processed.players_eoinamoore
            GROUP BY playerteamname
            ORDER BY fg_pct DESC
        """,
    },
    {
        "id": "Q8_rank",
        "description": "RANK window — top scorer per team",
        "category": "Window",
        "sql": """
            SELECT playerteamname, firstname, lastname, total_points, rnk
            FROM (
                SELECT playerteamname, firstname, lastname,
                       SUM(points) AS total_points,
                       RANK() OVER (
                           PARTITION BY playerteamname
                           ORDER BY SUM(points) DESC
                       ) AS rnk
                FROM iceberg.processed.players_eoinamoore
                GROUP BY playerteamname, firstname, lastname
            )
            WHERE rnk = 1
            ORDER BY total_points DESC
        """,
    },
    {
        "id": "Q9_advanced_filter",
        "description": "Advanced stats filter — high efficiency players",
        "category": "Filter",
        "sql": """
            SELECT playername, teamname, tspct, pie, netrating,
                   offrating, defrating, usgpct
            FROM iceberg.processed.dim_advanced_stats
            WHERE tspct > 0.6
              AND usgpct > 0.25
              AND pie > 0.15
            ORDER BY pie DESC
            LIMIT 100
        """,
    },
    {
        "id": "Q10_three_way_join",
        "description": "Three-table join — box scores + advanced + schedule",
        "category": "Join",
        "sql": """
            SELECT p.firstname, p.lastname,
                   p.points, p.assists,
                   a.tspct, a.pie,
                   s.arenaname, s.arenacity
            FROM iceberg.processed.players_eoinamoore p
            JOIN iceberg.processed.dim_advanced_stats a
              ON CAST(p.personid AS INTEGER) = a.personid
             AND p.gameid = CAST(a.gameid AS VARCHAR)
            JOIN iceberg.processed.dim_schedule s
              ON CAST(p.gameid AS INTEGER) = s.gameid
            WHERE p.points >= 25
            ORDER BY p.points DESC
            LIMIT 200
        """,
    },
]

QUERIES_SPARK = []
for q in QUERIES_TRINO:
    spark_q = dict(q)
    if q["id"] in ("Q6_join_advanced", "Q10_three_way_join"):
        spark_q["sql"] = q["sql"].replace(
            "CAST(a.gameid AS VARCHAR)",
            "CAST(a.gameid AS STRING)"
        )
    QUERIES_SPARK.append(spark_q)



def run_trino_benchmarks():
    from trino.dbapi import connect

    print("\n" + "="*60)
    print("TRINO BENCHMARK")
    print("="*60)

    results = []
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)

    for q in QUERIES_TRINO:
        times = []
        row_count = 0
        error = None

        print(f"\n[{q['id']}] {q['description']}")

        for run in range(N_RUNS):
            try:
                cur = conn.cursor()
                t0 = time.perf_counter()
                cur.execute(q["sql"])
                rows = cur.fetchall()
                t1 = time.perf_counter()
                elapsed = round((t1 - t0) * 1000, 2)
                times.append(elapsed)
                row_count = len(rows)
                print(f"Run {run+1}: {elapsed} ms — {row_count} rows")
            except Exception as e:
                error = str(e)
                print(f"Run {run+1}: ERROR — {e}")
                times.append(None)

        valid_times = [t for t in times if t is not None]
        results.append({
            "engine":      "Trino",
            "format":      "Iceberg",
            "query_id":    q["id"],
            "category":    q["category"],
            "description": q["description"],
            "runs":        N_RUNS,
            "avg_ms":      round(statistics.mean(valid_times), 2) if valid_times else None,
            "min_ms":      round(min(valid_times), 2) if valid_times else None,
            "max_ms":      round(max(valid_times), 2) if valid_times else None,
            "stddev_ms":   round(statistics.stdev(valid_times), 2) if len(valid_times) > 1 else 0,
            "row_count":   row_count,
            "error":       error,
        })

    return results



def run_spark_benchmarks():
    from pyspark.sql import SparkSession

    print("\n" + "="*60)
    print("SPARK SQL BENCHMARK")
    print("="*60)

    paquetes = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    )

    spark = SparkSession.builder \
        .appName("TFG_Benchmark") \
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

    results = []

    for q in QUERIES_SPARK:
        times = []
        row_count = 0
        error = None

        print(f"\n[{q['id']}] {q['description']}")

        for run in range(N_RUNS):
            try:
                t0 = time.perf_counter()
                df = spark.sql(q["sql"])
                row_count = df.count()  # force full execution
                t1 = time.perf_counter()
                elapsed = round((t1 - t0) * 1000, 2)
                times.append(elapsed)
                print(f"Run {run+1}: {elapsed} ms — {row_count} rows")
            except Exception as e:
                error = str(e)
                print(f"Run {run+1}: ERROR — {e}")
                times.append(None)

        valid_times = [t for t in times if t is not None]
        results.append({
            "engine":      "Spark SQL",
            "format":      "Iceberg",
            "query_id":    q["id"],
            "category":    q["category"],
            "description": q["description"],
            "runs":        N_RUNS,
            "avg_ms":      round(statistics.mean(valid_times), 2) if valid_times else None,
            "min_ms":      round(min(valid_times), 2) if valid_times else None,
            "max_ms":      round(max(valid_times), 2) if valid_times else None,
            "stddev_ms":   round(statistics.stdev(valid_times), 2) if len(valid_times) > 1 else 0,
            "row_count":   row_count,
            "error":       error,
        })

    spark.stop()
    return results

def run_pandas_benchmarks():
    print("\n" + "="*60)
    print("PANDAS / CSV BASELINE BENCHMARK")
    print("="*60)

    CSV_PLAYERS  = "/home/iceberg/jobs/PlayerStatistics.csv"
    CSV_ADVANCED = "/home/iceberg/jobs/PlayerStatisticsAdvanced.csv"
    CSV_SCHEDULE = "/home/iceberg/jobs/LeagueSchedule25_26.csv"

    results = []

    print("\nLoading CSVs into memory...")
    try:
        t0 = time.perf_counter()
        df_players  = pd.read_csv(CSV_PLAYERS,  low_memory=False)
        df_advanced = pd.read_csv(CSV_ADVANCED, low_memory=False)
        df_schedule = pd.read_csv(CSV_SCHEDULE, low_memory=False)
        load_time = round((time.perf_counter() - t0) * 1000, 2)

        df_players['gameDateTimeEst'] = pd.to_datetime(df_players['gameDateTimeEst'])
        df_players = df_players[
            (df_players['gameDateTimeEst'] >= '2025-05-24') &
            (df_players['gameDateTimeEst'] <= '2026-03-20')
        ].copy()

        print(f"players (filtered to Iceberg range): {len(df_players)} rows")
        print(f"CSV load time: {load_time} ms")
        print(f"players: {len(df_players)} rows | advanced: {len(df_advanced)} rows | schedule: {len(df_schedule)} rows")
    except Exception as e:
        print(f"ERROR loading CSVs: {e}")
        return results

    pandas_queries = [
        {
            "id": "Q1_count",
            "description": "Full table scan — COUNT(*)",
            "category": "Scan",
            "fn": lambda: len(df_players),
        },
        {
            "id": "Q2_filter",
            "description": "Selective filter — players with 30+ points",
            "category": "Filter",
            "fn": lambda: df_players[df_players["points"] >= 30].sort_values("points", ascending=False),
        },
        {
            "id": "Q3_aggregation",
            "description": "GROUP BY aggregation — avg points per team",
            "category": "Aggregation",
            "fn": lambda: df_players.assign(
                rebounds_total=df_players["reboundsDefensive"] + df_players["reboundsOffensive"]
            ).groupby("playerteamName").agg(
                games_played=("points", "count"),
                avg_points=("points", "mean"),
                avg_assists=("assists", "mean"),
                avg_rebounds=("rebounds_total", "mean"),
            ).round(2).sort_values("avg_points", ascending=False),
        },
        {
            "id": "Q4_window",
            "description": "Window function — rolling avg points per player",
            "category": "Window",
            "fn": lambda: df_players.sort_values("gameDateTimeEst").assign(
                rolling_avg_5games=df_players.groupby("personId")["points"]
                    .transform(lambda x: x.rolling(5, min_periods=1).mean().round(2))
            ).head(1000),
        },
        {
            "id": "Q5_join_schedule",
            "description": "JOIN — box scores joined with schedule",
            "category": "Join",
            "fn": lambda: df_players[df_players["points"] > 20].merge(
                df_schedule, left_on="gameId", right_on="gameId", how="inner"
            ).sort_values("points", ascending=False).head(500),
        },
        {
            "id": "Q6_join_advanced",
            "description": "JOIN — box scores joined with advanced stats",
            "category": "Join",
            "fn": lambda: df_players.merge(
                df_advanced, on=["personId", "gameId"], how="inner"
            ).head(1000),
        },
        {
            "id": "Q7_multi_agg",
            "description": "Multi-column aggregation — shooting efficiency",
            "category": "Aggregation",
            "fn": lambda: df_players.groupby("playerteamName").apply(
                lambda g: pd.Series({
                    "fg_pct":    round(g["fieldGoalsMade"].sum() / g["fieldGoalsAttempted"].sum() * 100, 2)
                                 if g["fieldGoalsAttempted"].sum() > 0 else None,
                    "three_pct": round(g["threePointersMade"].sum() / g["threePointersAttempted"].sum() * 100, 2)
                                 if g["threePointersAttempted"].sum() > 0 else None,
                    "ft_pct":    round(g["freeThrowsMade"].sum() / g["freeThrowsAttempted"].sum() * 100, 2)
                                 if g["freeThrowsAttempted"].sum() > 0 else None,
                    "players_used": g["personId"].nunique(),
                })
            ).sort_values("fg_pct", ascending=False),
        },
        {
            "id": "Q8_rank",
            "description": "RANK window — top scorer per team",
            "category": "Window",
            "fn": lambda: df_players.groupby(
                ["playerteamName", "firstName", "lastName"]
            )["points"].sum().reset_index(name="total_points").assign(
                rnk=lambda x: x.groupby("playerteamName")["total_points"]
                    .rank(method="min", ascending=False).astype(int)
            ).query("rnk == 1").sort_values("total_points", ascending=False),
        },
        {
            "id": "Q9_advanced_filter",
            "description": "Advanced stats filter — high efficiency players",
            "category": "Filter",
            "fn": lambda: df_advanced[
                (df_advanced["tsPct"] > 0.6) &
                (df_advanced["usgPct"] > 0.25) &
                (df_advanced["pie"] > 0.15)
            ].sort_values("pie", ascending=False).head(100),
        },
        {
            "id": "Q10_three_way_join",
            "description": "Three-table join — players + advanced + schedule",
            "category": "Join",
            "fn": lambda: df_players[df_players["points"] >= 25].merge(
                df_advanced, on=["personId", "gameId"], how="inner"
            ).merge(
                df_schedule, on="gameId", how="inner"
            ).sort_values("points", ascending=False).head(200),
        },
    ]

    for q in pandas_queries:
        times = []
        row_count = 0
        error = None

        print(f"\n[{q['id']}] {q['description']}")

        for run in range(N_RUNS):
            try:
                t0 = time.perf_counter()
                result = q["fn"]()
                t1 = time.perf_counter()
                elapsed = round((t1 - t0) * 1000, 2)
                times.append(elapsed)
                row_count = len(result) if hasattr(result, "__len__") else 1
                print(f"Run {run+1}: {elapsed} ms — {row_count} rows")
            except Exception as e:
                error = str(e)
                print(f"Run {run+1}: ERROR — {e}")
                times.append(None)

        valid_times = [t for t in times if t is not None]
        results.append({
            "engine":      "Pandas",
            "format":      "CSV",
            "query_id":    q["id"],
            "category":    q["category"],
            "description": q["description"],
            "runs":        N_RUNS,
            "avg_ms":      round(statistics.mean(valid_times), 2) if valid_times else None,
            "min_ms":      round(min(valid_times), 2) if valid_times else None,
            "max_ms":      round(max(valid_times), 2) if valid_times else None,
            "stddev_ms":   round(statistics.stdev(valid_times), 2) if len(valid_times) > 1 else 0,
            "row_count":   row_count,
            "error":       error,
        })

    return results


def measure_storage_sizes():
    print("\n" + "="*60)
    print("STORAGE SIZE COMPARISON")
    print("="*60)

    sizes = []

    csv_files = [
        "/home/iceberg/jobs/PlayerStatistics.csv",
        "/home/iceberg/jobs/PlayerStatisticsAdvanced.csv",
        "/home/iceberg/jobs/LeagueSchedule25_26.csv",
        "/home/iceberg/jobs/hoopshype_nba_salaries.csv",
    ]
    for f in csv_files:
        try:
            size_mb = round(os.path.getsize(f) / (1024 * 1024), 3)
            print(f"CSV  {os.path.basename(f)}: {size_mb} MB")
            sizes.append({"format": "CSV", "file": os.path.basename(f), "size_mb": size_mb})
        except FileNotFoundError:
            print(f"NOT FOUND: {f}")
    
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    iceberg_tables = [
        ("iceberg.processed", "players_eoinamoore"),
        ("iceberg.processed", "dim_advanced_stats"),
        ("iceberg.processed", "dim_schedule"),
    ]
    for schema, table in iceberg_tables:
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT sum(file_size_in_bytes) FROM {schema}."{table}$files"')
            result = cur.fetchone()
            size_mb = round(result[0] / (1024 * 1024), 3) if result and result[0] else 0
            print(f"Iceberg {table}: {size_mb} MB")
            sizes.append({"format": "Iceberg (Parquet+Snappy)", "file": table, "size_mb": size_mb})
        except Exception as e:
            print(f"Iceberg {table}: ERROR — {e}")

    return sizes

def save_results(all_results, storage_sizes):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"benchmark_results_{timestamp}.csv"
    storage_file = f"storage_sizes_{timestamp}.csv"

    fieldnames = ["engine", "format", "query_id", "category", "description",
                  "runs", "avg_ms", "min_ms", "max_ms", "stddev_ms", "row_count", "error"]
    with open(results_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)
    print(f"\nQuery results saved to: {results_file}")

    if storage_sizes:
        with open(storage_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["format", "file", "size_mb"])
            writer.writeheader()
            writer.writerows(storage_sizes)
        print(f"Storage sizes saved to: {storage_file}")

    print("\n" + "="*60)
    print("SUMMARY — Average latency per query (ms)")
    print("="*60)
    df = pd.DataFrame(all_results)
    if not df.empty:
        pivot = df.pivot_table(
            index=["query_id", "category"],
            columns="engine",
            values="avg_ms",
            aggfunc="first"
        ).round(2)
        print(pivot.to_string())


if __name__ == "__main__":
    print("TFG Data Lakes — Benchmark Suite")
    print(f"Timestamp : {datetime.now().isoformat()}")
    print(f"Runs/query: {N_RUNS}")
    print(f"Queries   : {len(QUERIES_TRINO)}")

    all_results = []

    try:
        all_results += run_trino_benchmarks()
    except Exception as e:
        print(f"\nTrino benchmark failed: {e}")

    try:
        all_results += run_spark_benchmarks()
    except Exception as e:
        print(f"\nSpark benchmark failed: {e}")

    try:
        all_results += run_pandas_benchmarks()
    except Exception as e:
        print(f"\nPandas benchmark failed: {e}")

    storage_sizes = measure_storage_sizes()

    save_results(all_results, storage_sizes)

    print("\nBenchmark complete.")