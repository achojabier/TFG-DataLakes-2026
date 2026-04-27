from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 20),
}

sql_game_logs = """
CREATE OR REPLACE TABLE iceberg.warehouse.game_logs WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['playerteamName', 'personId'],
    identifier_fields = ARRAY['gameId', 'personId']
) AS
WITH player_games AS (
    SELECT 
        personId,
        firstName || ' ' || lastName AS player_name,
        gameId,
        playerteamName,
        points,
        assists,
        reboundsOffensive,
        reboundsDefensive,
        steals,
        blocks,
        turnovers,
        numMinutes,
        CAST(gameDateTimeEst AS DATE) as game_date
    FROM iceberg.processed.players_eoinamoore
    WHERE numMinutes > 0
),
games_with_lag AS (
    SELECT 
        *,
        LAG(game_date) OVER (PARTITION BY personId ORDER BY game_date) AS prev_game_date
    FROM player_games
)
SELECT 
    *,
    CASE 
        WHEN date_diff('day', prev_game_date, game_date) = 1 THEN true 
        ELSE false 
    END AS is_back_to_back
FROM games_with_lag
"""

sql_season_stats = """
CREATE OR REPLACE TABLE iceberg.warehouse.player_season_stats WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['playerteamName', 'personId', 'season_start_year'],
    identifier_fields = ARRAY['personId', 'season_start_year']
) AS
WITH base_stats AS (
    SELECT 
        p.personId,
        p.firstName || ' ' || p.lastName AS player_name,
        p.playerteamName,
        CASE 
            WHEN EXTRACT(MONTH FROM CAST(p.gameDateTimeEst AS TIMESTAMP)) >= 10 
            THEN EXTRACT(YEAR FROM CAST(p.gameDateTimeEst AS TIMESTAMP))
            ELSE EXTRACT(YEAR FROM CAST(p.gameDateTimeEst AS TIMESTAMP)) - 1
        END AS season_start_year,
        
        p.gameId,
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
    b.personId,
    b.player_name,
    b.playerteamName,
    b.season_start_year,
    
    COUNT(b.gameId) AS total_games_played,
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
    SELECT personId, MAX(salary_usd) AS salary_usd 
    FROM iceberg.processed.dim_salaries 
    GROUP BY personId
) s_limpio 
    ON CAST(b.personId AS VARCHAR) = CAST(s_limpio.personId AS VARCHAR)

GROUP BY 
    b.personId, 
    b.player_name, 
    b.playerteamName,
    b.season_start_year
"""

sql_pk_game_logs = "ALTER TABLE iceberg.warehouse.game_logs SET PROPERTIES ('identifier-fields' = 'gameId, personId')"
sql_pk_season_stats = "ALTER TABLE iceberg.warehouse.player_season_stats SET PROPERTIES ('identifier-fields' = 'personId, season_start_year')"

with DAG(
    'dag_nba_oro',
    default_args=default_args,
    description='Data Marts de la Capa Oro (Idempotente + Full Refresh)',
    schedule_interval=None, 
    catchup=False,
    tags=['nba', 'iceberg', 'gold'],
) as dag:

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

    set_pk_game_logs = TrinoOperator(
        task_id='set_pk_game_logs',
        trino_conn_id='trino_default',
        sql=sql_pk_game_logs
    )

    set_pk_season_stats = TrinoOperator(
        task_id='set_pk_season_stats',
        trino_conn_id='trino_default',
        sql=sql_pk_season_stats
    )

    crear_game_logs >> set_pk_game_logs
    crear_season_stats >> set_pk_season_stats