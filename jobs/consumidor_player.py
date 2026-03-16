import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def iniciar_consumidor_player():
    print("⏳ Arrancando Consumidor Spark para Basketball-Reference (Player)...")
    
    paquetes = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    )

    spark = SparkSession.builder \
        .appName("Consumidor_player") \
        .config("spark.jars.packages", paquetes) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_USER")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_PASSWORD")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    esquema = StructType([
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("personId", StringType(), True),
        StructField("gameId", StringType(), True),
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
        StructField("foulsPersonal", DoubleType(), True),
        StructField("turnovers", IntegerType(), True),
        StructField("plusMinus", IntegerType(), True),
        StructField("gameDateTimeEst", StringType(), True)
    ])

    print("🧊 Inicializando tabla Iceberg 'nba.players_eoinamoore'...")
    spark.createDataFrame([], esquema).writeTo("iceberg.nba.players_eoinamoore").append()

    print("🎧 Escuchando canal 'nba_players_eoinamoore' en Kafka...")
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "nba_players_eoinamoore") \
        .option("startingOffsets", "latest") \
        .load()

    df_tiros = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), esquema, {"node":"PERMISSIVE"}).alias("data")) \
        .select("data.*")
    
    print("💾 Guardando micro-lotes en MinIO/Iceberg cada 5 segundos...")
    query = df_tiros.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("path", "iceberg.nba.players_eoinamoore") \
        .option("checkpointLocation", "s3a://warehouse/checkpoints/players_eoinamoore") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    iniciar_consumidor_player()