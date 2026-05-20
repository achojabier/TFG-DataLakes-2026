import os
from pyspark.sql import SparkSession
from spark_utils import get_spark_session

spark = get_spark_session("Init_Gold_Tables")

print("Limpiando esquemas antiguos de Oro...")
spark.sql("DROP TABLE IF EXISTS iceberg.warehouse.game_logs")
spark.sql("DROP TABLE IF EXISTS iceberg.warehouse.player_season_stats")

# --- game_logs (Molde de 15 columnas exactas) ---
spark.sql("""
    CREATE TABLE iceberg.warehouse.game_logs (
        personid BIGINT,
        player_name STRING,
        gameid BIGINT,
        playerteamname STRING,
        points INT,
        assists INT,
        reboundsoffensive INT,
        reboundsdefensive INT,
        steals INT,
        blocks INT,
        turnovers INT,
        numminutes DOUBLE,
        game_date DATE,
        prev_game_date DATE,
        is_back_to_back BOOLEAN
    )
    USING iceberg
    TBLPROPERTIES (
        'write.metadata.compression-codec' = 'gzip',
        'identifier-fields'  = 'gameid,personid',
        'format-version' = '2'
    )
    PARTITIONED BY (playerteamname)
""")

# --- player_season_stats (Molde de 15 columnas exactas) ---
spark.sql("""
    CREATE TABLE iceberg.warehouse.player_season_stats (
        personid BIGINT,
        player_name STRING,
        playerteamname STRING,
        season_start_year BIGINT,
        total_games_played BIGINT,
        total_points BIGINT,
        avg_points DOUBLE,
        total_assists BIGINT,
        avg_assists DOUBLE,
        total_rebounds_sum BIGINT,
        avg_rebounds DOUBLE,
        total_steals BIGINT,
        total_blocks BIGINT,
        total_turnovers BIGINT,
        salary_usd BIGINT
    )
    USING iceberg
    TBLPROPERTIES (
        'write.metadata.compression-codec' = 'gzip',
        'identifier-fields' = 'personid,season_start_year',
        'format-version' = '2'
    )
    PARTITIONED BY (season_start_year)
""")

spark.sql("ALTER TABLE iceberg.warehouse.game_logs WRITE ORDERED BY playerteamname ASC, game_date DESC, personid ASC")
spark.sql("ALTER TABLE iceberg.warehouse.player_season_stats WRITE ORDERED BY season_start_year DESC, playerteamname ASC, total_points DESC")

print("Tablas gold creadas correctamente con identifier fields y esquemas alineados.")
spark.stop()