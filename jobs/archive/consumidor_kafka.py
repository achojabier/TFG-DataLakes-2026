import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Variables para MinIO/Iceberg
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

def iniciar_streaming():
    print("⏳ Arrancando motor de Spark Streaming...")
    
    paquetes = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    )

    spark = SparkSession.builder \
        .appName("Consumidor_Kafka_Iceberg") \
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

    esquema_tiro = StructType([
        StructField("GAME_ID", StringType(), True),
        StructField("player_name", StringType(), True),
        StructField("SHOT_NUMBER", IntegerType(), True),
        StructField("PERIOD", IntegerType(), True),
        StructField("SHOT_DIST", DoubleType(), True),
        StructField("PTS", IntegerType(), True),
        StructField("SHOT_RESULT", StringType(), True)
    ])

    print("🧊 Inicializando tabla Iceberg 'nba.tiros_vivo'...")
    spark.createDataFrame([], esquema_tiro).writeTo("iceberg.nba.tiros_vivo").createOrReplace()

    print("🎧 Escuchando canal 'nba_tiros_vivo' en Kafka...")
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "nba_tiros_vivo") \
        .option("startingOffsets", "latest") \
        .load()

    df_tiros = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), esquema_tiro).alias("data")) \
        .select("data.*")

    print("💾 Guardando micro-lotes en MinIO/Iceberg cada 5 segundos...")
    query = df_tiros.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("path", "iceberg.nba.tiros_vivo") \
        .option("checkpointLocation", "s3a://warehouse/checkpoints/tiros_vivo") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    iniciar_streaming()