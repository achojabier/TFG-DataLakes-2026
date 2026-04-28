from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 20),
}

sql_delete_game_logs = "DELETE FROM iceberg.warehouse.game_logs"

sql_game_logs = """
INSERT INTO iceberg.warehouse.game_logs
WITH player_games AS (
    SELECT 
        personid,
        firstName || ' ' || lastName AS player_name,
        gameid,
        playerteamName,
        points,
        assists,
        reboundsOffensive,
        reboundsDefensive,
        steals,
        blocks,
        turnovers,
        numMinutes,
        CAST(gamedatetimeest AS DATE) as game_date
    FROM iceberg.processed.players_eoinamoore
    WHERE numMinutes > 0
),
games_with_lag AS (
    SELECT 
        *,
        LAG(game_date) OVER (PARTITION BY personid ORDER BY game_date) AS prev_game_date
    FROM player_games
)
SELECT 
    personid,
    player_name,
    gameid,
    playerteamName,
    points,
    assists,
    reboundsOffensive,
    reboundsDefensive,
    steals,
    blocks,
    turnovers,
    CAST(numMinutes AS DOUBLE) as numMinutes,
    game_date,
    prev_game_date,
    CASE WHEN date_diff('day', prev_game_date, game_date) = 1 THEN true ELSE false END AS is_back_to_back
FROM games_with_lag
"""

sql_delete_season_stats = "DELETE FROM iceberg.warehouse.player_season_stats"

# --- 2. Usamos DELETE + INSERT para Season Stats ---
sql_season_stats = """
INSERT INTO iceberg.warehouse.player_season_stats
WITH base_stats AS (
    SELECT 
        p.personid,
        p.firstName || ' ' || p.lastName AS player_name,
        p.playerteamName,
        CASE 
            WHEN EXTRACT(MONTH FROM CAST(p.gamedatetimeest AS TIMESTAMP)) >= 10 
            THEN EXTRACT(YEAR FROM CAST(p.gamedatetimeest AS TIMESTAMP))
            ELSE EXTRACT(YEAR FROM CAST(p.gamedatetimeest AS TIMESTAMP)) - 1
        END AS season_start_year,
        p.gameid,
        p.points,
        p.assists,
        (p.reboundsOffensive + p.reboundsDefensive) AS total_rebounds,
        p.steals,
        p.blocks,
        p.turnovers
    FROM iceberg.processed.players_eoinamoore p
    WHERE p.numMinutes > 0
)
SELECT 
    b.personid,
    b.player_name,
    b.playerteamName,
    b.season_start_year,
    COUNT(b.gameid) AS total_games_played,
    SUM(b.points) AS total_points,
    ROUND(AVG(CAST(b.points AS DOUBLE)), 1) AS avg_points,
    SUM(b.assists) AS total_assists,
    ROUND(AVG(CAST(b.assists AS DOUBLE)), 1) AS avg_assists,
    SUM(b.total_rebounds) AS total_rebounds_sum,
    ROUND(AVG(CAST(b.total_rebounds AS DOUBLE)), 1) AS avg_rebounds,
    SUM(b.steals) AS total_steals,
    SUM(b.blocks) AS total_blocks,
    SUM(b.turnovers) AS total_turnovers,
    COALESCE(MAX(s_limpio.salary_usd), 0) AS salary_usd
FROM base_stats b
LEFT JOIN (
    SELECT personid, MAX(salary_usd) AS salary_usd 
    FROM iceberg.processed.dim_salaries 
    GROUP BY personid
) s_limpio 
    ON CAST(b.personid AS VARCHAR) = CAST(s_limpio.personid AS VARCHAR)
GROUP BY 
    b.personid, 
    b.player_name, 
    b.playerteamName,
    b.season_start_year
"""
with DAG(
    'dag_nba_oro',
    default_args=default_args,
    description='Data Marts de la Capa Oro (Idempotente + Full Refresh)',
    schedule_interval=None, 
    catchup=False,
    tags=['nba', 'iceberg', 'gold'],
) as dag:

    delete_game_logs = TrinoOperator(
        task_id='delete_game_logs',
        trino_conn_id='trino_default',
        sql=sql_delete_game_logs
    )

    sql_delete_season_stats = TrinoOperator(
        task_id='delete_season_stats',
        trino_conn_id='trino_default',
        sql=sql_delete_season_stats
    )

    crear_game_logs = TrinoOperator(
        task_id='create_gold_game_logs',
        trino_conn_id='trino_default',
        sql=sql_game_logs
    )

    crear_season_stats = TrinoOperator(
        task_id='create_gold_season_stats',
        trino_conn_id='trino_default',
        sql=sql_season_stats
    )

    delete_game_logs >> crear_game_logs
    sql_delete_season_stats >> crear_season_stats