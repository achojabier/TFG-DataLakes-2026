import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from spark.spark_utils import get_spark_session

def iniciar_consumidor_player():
    print("Arrancando Consumidor Spark para los datos de jugadores...")
    
    spark = get_spark_session("Consumidor_Players")

    esquema = StructType([
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("personId", StringType(), False),
        StructField("gameId", StringType(), False),
        StructField("playerteamName", StringType(), True),
        StructField("opponentteamName", StringType(), True),
        StructField("gameType", StringType(), True),
        StructField("gameLabel", StringType(), True),
        StructField("win", IntegerType(), True),
        StructField("home", IntegerType(), True),
        StructField("numMinutes", IntegerType(), True),
        StructField("points", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("blocks", IntegerType(), True),
        StructField("steals", IntegerType(), True),
        StructField("fieldGoalsAttempted", IntegerType(), True),
        StructField("fieldGoalsMade", IntegerType(), True),
        StructField("threePointersAttempted", IntegerType(), True),
        StructField("threePointersMade", IntegerType(), True),
        StructField("freeThrowsAttempted", IntegerType(), True),
        StructField("freeThrowsMade", IntegerType(), True),
        StructField("reboundsDefensive", IntegerType(), True),
        StructField("reboundsOffensive", IntegerType(), True),
        StructField("foulsPersonal", IntegerType(), True),
        StructField("turnovers", IntegerType(), True),
        StructField("plusMinus", IntegerType(), True),
        StructField("gameDateTimeEst", TimestampType(), False)
    ])

    print("Inicializando tabla Iceberg 'nba.players_eoinamoore'...")

    spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.landing.players_eoinamoore (
        firstName STRING,
        lastName STRING,
        personId STRING,
        gameId STRING,
        playerteamName STRING,
        opponentteamName STRING,
        gameType STRING,
        gameLabel STRING,
        win INT,
        home INT,
        numMinutes INT,
        points INT,
        assists INT,
        blocks INT,
        steals INT,
        fieldGoalsAttempted INT,
        fieldGoalsMade INT,
        threePointersAttempted INT,
        threePointersMade INT,
        freeThrowsAttempted INT,
        freeThrowsMade INT,
        reboundsDefensive INT,
        reboundsOffensive INT,
        foulsPersonal INT,
        turnovers INT,
        plusMinus INT,
        gameDateTimeEst TIMESTAMP
    ) USING iceberg
    """)


    print("Escuchando canal 'nba_players_eoinamoore' en Kafka...")
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "nba_players_eoinamoore") \
        .option("startingOffsets", "earliest") \
        .load()

    df_tiros = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), esquema, {"mode":"PERMISSIVE"}).alias("data")) \
        .select("data.*") \
        .dropna(subset=["personId", "gameId", "gameDateTimeEst"])
    
    
    print("Guardando micro-lotes en MinIO/Iceberg cada 5 segundos...")
    query = df_tiros.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("path", "iceberg.landing.players_eoinamoore") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/players_eoinamoore") \
        .start()
    
    query_cold_storage = df_kafka.selectExpr("CAST(value AS STRING) as raw_json") \
        .writeStream \
        .format("text") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("path", "s3a://raw-archive/players_eoinamoore/") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/players_archive_raw") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    iniciar_consumidor_player()