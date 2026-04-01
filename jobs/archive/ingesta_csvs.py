import os
from pyspark.sql import SparkSession

def cargar_csvs_a_iceberg():
    print("🚀 Arrancando Spark en modo Batch para cargar CSVs...")
    
    paquetes = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    )

    spark = SparkSession.builder \
        .appName("Carga_Dimensiones_CSV") \
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
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_USER", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_PASSWORD", "password")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Cargar el Calendario (Spark infiere las 17 columnas automáticamente)
    print("📅 Leyendo CSV del Calendario...")
    df_calendario = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/home/iceberg/jobs/LeagueSchedule25_26.csv") # Asegúrate de que el nombre coincida

    print("🧊 Creando tabla dim_calendar en Iceberg...")
    # El comando .create() le dice a Iceberg que genere la tabla desde cero con el esquema del DataFrame
    df_calendario.writeTo("iceberg.processed.dim_schedule").create()


    # 2. Cargar las Estadísticas Avanzadas (Spark infiere las 60 columnas automáticamente)
    print("📊 Leyendo CSV de Estadísticas Avanzadas...")
    df_stats = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/home/iceberg/jobs/PlayerStatisticsAdvanced.csv") # Asegúrate de que el nombre coincida

    print("🧊 Creando tabla dim_advanced_stats en Iceberg...")
    df_stats.writeTo("iceberg.processed.dim_advanced_stats").create()

    print("✅ ¡Proceso terminado con éxito!")

if __name__ == "__main__":
    cargar_csvs_a_iceberg()