import os
from pyspark.sql import SparkSession

MINIO_USER     = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin123")
MINIO_ENDPOINT = "http://minio:9000"

paquetes = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
)

spark = SparkSession.builder \
    .appName("init_gold_tables") \
    .config("spark.jars.packages", paquetes) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT) \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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