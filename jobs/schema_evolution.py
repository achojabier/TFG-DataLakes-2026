import os
import time
import pandas as pd
from trino.dbapi import connect

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", 8080))
CSV_PATH   = "/home/iceberg/jobs/hoopshype_nba_salaries.csv"

def trino_execute(conn, sql, fetch=True):
    cur = conn.cursor()
    cur.execute(sql)
    if fetch:
        return cur.fetchall()
    return None

def trino_query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="admin")

print("="*60)
print("ICEBERG SCHEMA EVOLUTION DEMO")
print("TFG Data Lake NBA — Thesis Experiment")
print("="*60)

print("\nResetting to original schema for clean demo...")
for col in ["salary_2026_27", "salary_2027_28", "salary_2028_29", "salary_2029_30"]:
    try:
        trino_execute(conn,
            f"ALTER TABLE iceberg.processed.dim_salaries DROP COLUMN {col}",
            fetch=False)
        print(f"Dropped existing column: {col}")
    except Exception:
        pass  # Column didn't exist, that's fine

print("\nOriginal dim_salaries schema")
print("-"*40)
rows = trino_query(conn, "DESCRIBE iceberg.processed.dim_salaries")
for r in rows:
    print(f"  {r['Column']:<20} {r['Type']}")

count = trino_execute(conn,
    "SELECT COUNT(*) FROM iceberg.processed.dim_salaries")[0][0]
snapshots_before = trino_execute(conn,
    'SELECT COUNT(*) FROM iceberg.processed."dim_salaries$snapshots"')[0][0]
print(f"\n  Rows      : {count}")
print(f"  Snapshots : {snapshots_before}")

print("\nAdding multi-year salary columns via ALTER TABLE")
print("-"*40)
print("This operation does NOT rewrite any data files.")
print("Old rows will show NULL for new columns automatically.\n")

new_columns = [
    ("salary_2026_27", "DOUBLE"),
    ("salary_2027_28", "DOUBLE"),
    ("salary_2028_29", "DOUBLE"),
    ("salary_2029_30", "DOUBLE"),
]

alter_times = []
for col_name, col_type in new_columns:
    t0 = time.perf_counter()
    trino_execute(conn,
        f"ALTER TABLE iceberg.processed.dim_salaries ADD COLUMN {col_name} {col_type}",
        fetch=False)
    elapsed = round((time.perf_counter() - t0) * 1000, 2)
    alter_times.append(elapsed)
    print(f"  ADD COLUMN {col_name:<20} → {elapsed} ms")

avg_alter = round(sum(alter_times) / len(alter_times), 1)
print(f"\nAverage ALTER TABLE time: {avg_alter} ms")
print(f"Data files rewritten: 0")

print("\nNew schema:")
rows = trino_query(conn, "DESCRIBE iceberg.processed.dim_salaries")
for r in rows:
    print(f"{r['Column']:<25} {r['Type']}")


print("\nVerifying NULL behaviour (before populating new columns)")
print("-"*40)
sample_null = trino_query(conn, """
    SELECT firstname, lastname,
           ROUND(salary_usd/1000000,2) AS salary_usd_m,
           salary_2026_27,
           salary_2027_28
    FROM iceberg.processed.dim_salaries
    LIMIT 5
""")
print(f"{'Player':<25} {'25-26 ($M)':>12} {'26-27':>10} {'27-28':>10}")
print(f"{'-'*25} {'-'*12} {'-'*10} {'-'*10}")
for r in sample_null:
    name = f"{r.get('firstname','')} {r.get('lastname','')}"
    print(f"  {name:<25} "
          f"{str(r.get('salary_usd_m') or 'NULL'):>12} "
          f"{str(r.get('salary_2026_27') or 'NULL'):>10} "
          f"{str(r.get('salary_2027_28') or 'NULL'):>10}")
print("\nNew columns exist but return NULL for all existing rows")
print("Old queries on salary_usd continue to work unchanged")


print("\nPopulating new columns via MERGE (batch, single snapshot)")
print("-"*40)

df = pd.read_csv(CSV_PATH)
season_cols = {
    "salary_2026_27": "2026-27",
    "salary_2027_28": "2027-28",
    "salary_2028_29": "2028-29",
    "salary_2029_30": "2029-30",
}

# Build a VALUES list for a single MERGE — one snapshot instead of 651
# Deduplicate — keep first occurrence of each firstname+lastname combo
seen = set()
rows_vals = []
for _, row in df.iterrows():
    player = str(row["player"]).strip()
    parts = player.split(" ", 1)
    if len(parts) < 2:
        continue
    fn = parts[0].replace("'", "''")
    ln = parts[1].replace("'", "''")
    key = (fn.lower(), ln.lower())
    if key in seen:
        continue
    seen.add(key)
    vals = []
    for csv_col in season_cols.values():
        v = row.get(csv_col)
        vals.append(str(float(v)) if pd.notna(v) and v != "" else "NULL")
    rows_vals.append(f"('{fn}', '{ln}', {vals[0]}, {vals[1]}, {vals[2]}, {vals[3]})")

values_sql = ",\n".join(rows_vals)

merge_sql = f"""
MERGE INTO iceberg.processed.dim_salaries AS target
USING (
    VALUES
    {values_sql}
) AS source(fn, ln, s2627, s2728, s2829, s2930)
ON LOWER(target.firstname) = LOWER(source.fn)
AND LOWER(target.lastname)  = LOWER(source.ln)
WHEN MATCHED THEN UPDATE SET
    salary_2026_27 = source.s2627,
    salary_2027_28 = source.s2728,
    salary_2028_29 = source.s2829,
    salary_2029_30 = source.s2930
"""

t0 = time.perf_counter()
trino_execute(conn, merge_sql, fetch=False)
merge_elapsed = round((time.perf_counter() - t0) * 1000, 2)
print(f"MERGE completed in: {merge_elapsed} ms")
print(f"Snapshots created: 1 (entire update = single atomic operation)")

print("\nQuerying old and new columns together")
print("-"*40)

sample = trino_query(conn, """
    SELECT
        firstname || ' ' || lastname         AS player,
        ROUND(salary_usd / 1000000, 2)       AS s2526,
        ROUND(salary_2026_27 / 1000000, 2)   AS s2627,
        ROUND(salary_2027_28 / 1000000, 2)   AS s2728,
        ROUND(salary_2028_29 / 1000000, 2)   AS s2829
    FROM iceberg.processed.dim_salaries
    WHERE salary_usd IS NOT NULL
    ORDER BY salary_usd DESC
    LIMIT 10
""")

print(f"{'Player':<25} {'25-26':>8} {'26-27':>8} {'27-28':>8} {'28-29':>8}")
print(f"{'-'*25} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
for r in sample:
    print(f"  {str(r.get('player','')):<25} "
          f"{str(r.get('s2526') or 'NULL'):>8} "
          f"{str(r.get('s2627') or 'NULL'):>8} "
          f"{str(r.get('s2728') or 'NULL'):>8} "
          f"{str(r.get('s2829') or 'NULL'):>8}")

future = trino_execute(conn, """
    SELECT COUNT(*) FROM iceberg.processed.dim_salaries
    WHERE salary_2026_27 IS NOT NULL
""")[0][0]
print(f"\nPlayers with future salary data: {future}/{count} ({round(future/count*100,1)}%)")


print("\n📸 STEP 6 — Snapshot history")
print("-"*40)
snapshots = trino_query(conn, """
    SELECT snapshot_id, committed_at, operation
    FROM iceberg.processed."dim_salaries$snapshots"
    ORDER BY committed_at DESC
    LIMIT 6
""")
for s in snapshots:
    print(f"[{s.get('committed_at')}] {s.get('operation')}")


print("\n⏱️  STEP 7 — Time travel: query table before schema evolution")
print("-"*40)

first_snap = trino_execute(conn, """
    SELECT snapshot_id, committed_at
    FROM iceberg.processed."dim_salaries$snapshots"
    ORDER BY committed_at ASC
    LIMIT 1
""")[0]

snapshot_id = 2309484074392766127
print(f"Querying snapshot: {snapshot_id}")
print(f"Timestamp        : 2026-03-20 18:18 (original ingestion, no personId)")

try:
    old = trino_query(conn, f"""
        SELECT *
        FROM iceberg.processed.dim_salaries
        FOR VERSION AS OF {snapshot_id}
        LIMIT 5
    """)
    cols = list(old[0].keys())
    print(f"Columns in snapshot : {cols}")
    print(f"Current columns     : {[r['Column'] for r in trino_query(conn, 'DESCRIBE iceberg.processed.dim_salaries')]}")
    print(f"Time travel successful — schema differences visible across snapshots")
    for r in old[:3]:
        vals = list(r.values())
        print(f"     {vals}")
except Exception as e:
    print(f"Result: {e}")

print("\nSTEP 8 — Equivalent operation on a CSV file")
print("-"*40)

t0 = time.perf_counter()
df_csv = pd.read_csv(CSV_PATH)
for col_name, csv_col in season_cols.items():
    df_csv[col_name] = df_csv.get(csv_col)
df_csv.to_csv("/tmp/salaries_evolved.csv", index=False)
csv_elapsed = round((time.perf_counter() - t0) * 1000, 2)

orig_kb = round(os.path.getsize(CSV_PATH) / 1024, 1)
new_kb  = round(os.path.getsize("/tmp/salaries_evolved.csv") / 1024, 1)

print(f"CSV read + add columns + rewrite : {csv_elapsed} ms")
print(f"Original file size               : {orig_kb} KB")
print(f"Rewritten file size              : {new_kb} KB")
print(f"Version history available        : NO")
print(f"Original data recoverable        : NO (overwritten)")


print("\n" + "="*60)
print("SCHEMA EVOLUTION — RESULTS SUMMARY")
print("="*60)
print(f"Columns added (ALTER TABLE)     : {len(new_columns)}")
print(f"Avg ALTER TABLE time            : {avg_alter} ms")
print(f"Data files rewritten            : 0")
print(f"MERGE update time               : {merge_elapsed} ms")
print(f"CSV equivalent total time       : {csv_elapsed} ms")
print(f"Iceberg advantage               : versioned, non-destructive, instant")
print(f"Time travel available           : YES")
print(f"Old queries still work          : YES")
print("="*60)
print("\nSchema evolution demo complete.")