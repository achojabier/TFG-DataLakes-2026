import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def iniciar_consumidor_api():
    print("⏳ Arrancando Consumidor Spark (Datos Nativos NBA API)...")
    
    paquetes = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    )

    spark = SparkSession.builder \
        .appName("Consumidor_NBA_API") \
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

    
    esquema_tiros = StructType([
        StructField("id_partido", StringType(), True),
        StructField("fecha_partido", StringType(), True),
        StructField("cuarto", IntegerType(), True),
        StructField("minutos_restantes", IntegerType(), True),
        StructField("segundos_restantes", IntegerType(), True),
        StructField("equipo_local", StringType(), True),
        StructField("equipo_visitante", StringType(), True),
        StructField("equipo_tira", StringType(), True),
        StructField("jugador", StringType(), True),
        StructField("distancia_pies", IntegerType(), True),
        StructField("valor_tiro", IntegerType(), True),
        StructField("anotado", IntegerType(), True),
        StructField("loc_x", IntegerType(), True),
        StructField("loc_y", IntegerType(), True),
        StructField("marcador_previo", StringType(), True)
    ])

    print("🧊 Inicializando tabla Iceberg 'nba.tiros_api'...")
    spark.createDataFrame([], esquema_tiros).writeTo("iceberg.nba.tiros_api").createOrReplace()

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "topic_nba_api_tiros") \
        .option("startingOffsets", "latest") \
        .load()

    df_datos = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), esquema_tiros).alias("data")) \
        .select("data.*")

    print("💾 Guardando coordenadas y tiros en Iceberg...")
    query = df_datos.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("path", "iceberg.nba.tiros_api") \
        .option("checkpointLocation", "s3a://warehouse/checkpoints/tiros_api_nba") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    iniciar_consumidor_api()