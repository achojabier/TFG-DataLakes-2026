from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 20),
}

sql_game_logs = """
CREATE OR REPLACE TABLE iceberg.gold.game_logs WITH (format = 'PARQUET') AS
WITH player_games AS (
    SELECT 
        personId,
        firstName || ' ' || lastName AS player_name,
        gameId,
        playerteamName,
        points,
        assists,
        numMinutes,
        CAST(SUBSTR(gameDateTimeEst, 1, 10) AS DATE) as game_date
    FROM iceberg.processed.players_eoinamoore
    -- AÑADIMOS EL FILTRO AQUÍ TAMBIÉN:
    WHERE CAST(numMinutes AS INTEGER) > 0
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
CREATE OR REPLACE TABLE iceberg.gold.player_season_stats WITH (format = 'PARQUET') AS
SELECT 
    p.personId,
    p.firstName || ' ' || p.lastName AS player_name,
    p.playerteamName,
    
    -- Ahora las métricas serán reales y aditivas
    COUNT(p.gameId) AS total_games_played,
    SUM(p.points) AS total_points,
    SUM(p.assists) AS total_assists,
    
    -- Traemos el salario único
    COALESCE(MAX(s_limpio.salary_usd), 0) AS salary_usd

FROM iceberg.processed.players_eoinamoore p

LEFT JOIN (
    -- Subconsulta para tener salarios únicos
    SELECT 
        personId, 
        MAX(salary_usd) AS salary_usd 
    FROM iceberg.processed.dim_salaries 
    GROUP BY personId
) s_limpio 
    ON CAST(p.personId AS VARCHAR) = CAST(s_limpio.personId AS VARCHAR)

WHERE CAST(p.numMinutes AS INTEGER) > 0

GROUP BY 
    p.personId, 
    p.firstName, 
    p.lastName, 
    p.playerteamName
"""

with DAG(
    'dag_nba_oro',
    default_args=default_args,
    description='Data Marts de la Capa Oro',
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

    [crear_game_logs, crear_season_stats]