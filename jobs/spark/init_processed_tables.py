import os

from pyspark.sql import SparkSession

from spark_utils import get_spark_session

spark = get_spark_session("Init_Processed_Tables")

print("Limpiando tablas antiguas para aplicar el nuevo esquema...")
spark.sql("DROP TABLE IF EXISTS iceberg.processed.players_eoinamoore")
spark.sql("DROP TABLE IF EXISTS iceberg.processed.dim_salaries")

# --- players_eoinamoore ---
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.processed.players_eoinamoore (
        firstName               STRING,
        lastName                STRING,
        personid                BIGINT,
        gameid                  BIGINT,
        playerteamName          STRING,
        opponentteamName        STRING,
        gameType                STRING,
        gameLabel               STRING,
        win                     BOOLEAN,
        home                    BOOLEAN,
        numMinutes              INT,
        points                  INT,
        assists                 INT,
        blocks                  INT,
        steals                  INT,
        fieldGoalsAttempted     INT,
        fieldGoalsMade          INT,
        threePointersAttempted  INT,
        threePointersMade       INT,
        freeThrowsAttempted     INT,
        freeThrowsMade          INT,
        reboundsDefensive       INT,
        reboundsOffensive       INT,
        foulsPersonal           INT,
        turnovers               INT,
        plusMinus               INT,
        gamedatetimeest         TIMESTAMP,
        rating                  DOUBLE
    )
    USING iceberg
    TBLPROPERTIES (
        'write.metadata.compression-codec' = 'gzip',
        'identifier-fields' = 'gameid,personid'
    )
    PARTITIONED BY (months(gamedatetimeest))
""")

# --- dim_salaries ---
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.processed.dim_salaries (
        personid        BIGINT,
        player_name     STRING,
        playerteamName  STRING,
        season          STRING,
        salary_usd      BIGINT
    )
    USING iceberg
    TBLPROPERTIES (
        'write.metadata.compression-codec' = 'gzip',
        'identifier-fields' = 'personid,season'
    )
    PARTITIONED BY (season)
""")

spark.sql("ALTER TABLE iceberg.processed.players_eoinamoore WRITE ORDERED BY playerteamName ASC, gameid DESC")
spark.sql("ALTER TABLE iceberg.processed.dim_salaries WRITE ORDERED BY personid ASC, season DESC")

print("Tablas processed creadas correctamente con identifier fields.")
spark.stop()