import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .appName("Docker_Bootstrap_Auto") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("[BOOTSTRAP] Iniciando recuperación del catálogo...")

try:

    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.nba")
    

    csv_path = "file:///home/iceberg/notebooks/shot_logs.csv"
    
    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

    # 3. Mapeo Completo (Sin recortes)
    df_completo = df_raw.select(
        col("GAME_ID").alias("partido_id"),
        col("MATCHUP").alias("emparejamiento"),
        col("LOCATION").alias("local_visitante"),
        col("W").alias("victoria"),
        col("FINAL_MARGIN").alias("margen_final"),
        col("SHOT_NUMBER").alias("numero_tiro"),
        col("PERIOD").alias("cuarto"),
        col("GAME_CLOCK").alias("tiempo_reloj"),
        col("SHOT_CLOCK").alias("tiempo_posesion"),
        col("DRIBBLES").alias("botes"),
        col("TOUCH_TIME").alias("tiempo_con_balon"),
        col("SHOT_DIST").alias("distancia_tiro"),
        col("PTS_TYPE").alias("valor_tiro"),
        col("SHOT_RESULT").alias("resultado"),
        col("CLOSEST_DEFENDER").alias("defensor"),
        col("CLOSE_DEF_DIST").alias("distancia_defensor"),
        col("FGM").alias("tiros_metidos"),
        col("PTS").alias("puntos"),
        col("player_name").alias("jugador"),
        col("player_id").alias("jugador_id")
    )


    df_completo.writeTo("iceberg.nba.tiros_test").createOrReplace()
    
    print("✅ [BOOTSTRAP] Tabla 'tiros_test' recuperada exitosamente.")

except Exception as e:
    print(f"❌ [BOOTSTRAP] Error crítico: {e}")
    sys.exit(1)

finally:
    spark.stop()