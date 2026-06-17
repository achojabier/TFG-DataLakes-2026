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
        "id": "Q5_multi_agg",
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
        "id": "Q6_rank",
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
]

# Ya no necesitamos adaptación para Q6/Q10, pero mantenemos el bucle por si acaso
QUERIES_SPARK = []
for q in QUERIES_TRINO:
    QUERIES_SPARK.append(dict(q))


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

        # ── Warmup (no incluido en la media) ──────────────────────────
        try:
            cur = conn.cursor()
            t0 = time.perf_counter()
            cur.execute(q["sql"])
            rows = cur.fetchall()
            t1 = time.perf_counter()
            warmup_ms = round((t1 - t0) * 1000, 2)
            print(f"Warmup: {warmup_ms} ms — {len(rows)} rows (not counted)")
        except Exception as e:
            print(f"Warmup: ERROR — {e}")

        # ── Mediciones reales ─────────────────────────────────────────
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
    from spark.spark_utils import get_spark_session
    print("\n" + "="*60)
    print("SPARK SQL BENCHMARK")
    print("="*60)

    spark = get_spark_session("Benchmark_Spark_SQL")

    results = []

    for q in QUERIES_SPARK:
        times = []
        row_count = 0
        error = None

        print(f"\n[{q['id']}] {q['description']}")

        # ── Warmup (no incluido en la media) ──────────────────────────
        try:
            t0 = time.perf_counter()
            df_warmup = spark.sql(q["sql"])
            rows_warmup = df_warmup.collect()
            t1 = time.perf_counter()
            warmup_ms = round((t1 - t0) * 1000, 2)
            print(f"Warmup: {warmup_ms} ms — {len(rows_warmup)} rows (not counted)")
        except Exception as e:
            print(f"Warmup: ERROR — {e}")

        # ── Mediciones reales ─────────────────────────────────────────
        for run in range(N_RUNS):
            try:
                t0 = time.perf_counter()
                df = spark.sql(q["sql"])
                rows = df.collect()
                row_count = len(rows)
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
    CSV_SCHEDULE = "/home/iceberg/jobs/LeagueSchedule25_26.csv"

    results = []

    print("\nLoading CSVs into memory...")
    try:
        t0 = time.perf_counter()
        df_players  = pd.read_csv(CSV_PLAYERS,  low_memory=False)
        df_schedule = pd.read_csv(CSV_SCHEDULE, low_memory=False)
        load_time = round((time.perf_counter() - t0) * 1000, 2)

        df_players['gameDateTimeEst'] = pd.to_datetime(df_players['gameDateTimeEst'])
        df_players = df_players[
            (df_players['gameDateTimeEst'] >= '2025-05-24') &
            (df_players['gameDateTimeEst'] <= '2026-03-20')
        ].copy()

        print(f"players (filtered to Iceberg range): {len(df_players)} rows")
        print(f"CSV load time: {load_time} ms")
        print(f"players: {len(df_players)} rows | schedule: {len(df_schedule)} rows")
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
            "id": "Q5_multi_agg",
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
            "id": "Q6_rank",
            "description": "RANK window — top scorer per team",
            "category": "Window",
            "fn": lambda: df_players.groupby(
                ["playerteamName", "firstName", "lastName"]
            )["points"].sum().reset_index(name="total_points").assign(
                rnk=lambda x: x.groupby("playerteamName")["total_points"]
                    .rank(method="min", ascending=False).astype(int)
            ).query("rnk == 1").sort_values("total_points", ascending=False),
        },
    ]

    for q in pandas_queries:
        times = []
        row_count = 0
        error = None

        print(f"\n[{q['id']}] {q['description']}")

        # ── Warmup (no incluido en la media) ──────────────────────────
        try:
            t0 = time.perf_counter()
            result_warmup = q["fn"]()
            t1 = time.perf_counter()
            warmup_ms = round((t1 - t0) * 1000, 2)
            rows_warmup = len(result_warmup) if hasattr(result_warmup, "__len__") else 1
            print(f"Warmup: {warmup_ms} ms — {rows_warmup} rows (not counted)")
        except Exception as e:
            print(f"Warmup: ERROR — {e}")

        # ── Mediciones reales ─────────────────────────────────────────
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

    # Solo los CSV que todavía usas
    csv_files = [
        "/home/iceberg/jobs/PlayerStatistics.csv",
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
    # Ya no medimos dim_advanced_stats
    iceberg_tables = [
        ("iceberg.processed", "players_eoinamoore"),
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
    results_file = f"/home/iceberg/jobs/benchmark_results_{timestamp}.csv"
    storage_file = f"/home/iceberg/jobs/storage_sizes_{timestamp}.csv"

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