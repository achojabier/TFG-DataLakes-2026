import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_USER", "admin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_PASSWORD", "admin123")

def iniciar_consumidor_full():
    print("⏳ Arrancando Consumidor Spark para Basketball-Reference (Full)...")
    
    paquetes = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    )

    spark = SparkSession.builder \
        .appName("Consumidor_Scraper_Full") \
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
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ESQUEMA PIXEL-PERFECT (Basado en el JSON exacto de la librería)
    esquema_completo = StructType([
        StructField("fecha_partido", StringType(), True),
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("team", StringType(), True),
        StructField("location", StringType(), True),
        StructField("opponent", StringType(), True),
        StructField("outcome", StringType(), True),
        StructField("seconds_played", IntegerType(), True),
        StructField("made_field_goals", IntegerType(), True),
        StructField("attempted_field_goals", IntegerType(), True),
        StructField("made_three_point_field_goals", IntegerType(), True),
        StructField("attempted_three_point_field_goals", IntegerType(), True),
        StructField("made_free_throws", IntegerType(), True),
        StructField("attempted_free_throws", IntegerType(), True),
        StructField("offensive_rebounds", IntegerType(), True),
        StructField("defensive_rebounds", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("steals", IntegerType(), True),
        StructField("blocks", IntegerType(), True),
        StructField("turnovers", IntegerType(), True),
        StructField("personal_fouls", IntegerType(), True),
        StructField("game_score", DoubleType(), True)
    ])

    print("🧊 Inicializando tabla Iceberg 'nba.basketball_reference'...")
    spark.createDataFrame([], esquema_completo).writeTo("iceberg.nba.basketball_reference").createOrReplace()

    # LEER DE KAFKA
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "basketball_reference") \
        .option("startingOffsets", "latest") \
        .load()

    # PARSEAR JSON
    df_datos = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), esquema_completo).alias("data")) \
        .select("data.*")

    # ESCRIBIR EN ICEBERG - LANDING
    print("💾 Guardando micro-lotes en la nueva tabla...")
    query = df_datos.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("path", "iceberg.nba.basketball_reference") \
        .option("checkpointLocation", "s3a://warehouse/checkpoints/basketball_reference_full") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    iniciar_consumidor_full()