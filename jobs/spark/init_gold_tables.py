import os
from pyspark.sql import SparkSession


MINIO_USER     = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin123")
MINIO_ENDPOINT = "http://minio:9000"

spark = SparkSession.builder \
    .appName("init_processed_tables") \
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

# --- game_logs ---
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.warehouse.game_logs (
        personId        BIGINT,
        player_name     STRING,
        gameId          BIGINT,
        playerteamName  STRING,
        points          INT,
        assists         INT,
        reboundsOffensive INT,
        reboundsDefensive INT,
        steals          INT,
        blocks          INT,
        turnovers       INT,
        numMinutes      DOUBLE,
        game_date       DATE,
        prev_game_date  DATE,
        is_back_to_back BOOLEAN
    )
    USING iceberg
    TBLPROPERTIES (
        'write.metadata.compression-codec' = 'gzip',
        'identifier-fields'  = 'gameId,personId'
    )
    PARTITIONED BY (playerteamName)
    -- sorted_by no es propiedad de tabla, se controla en escritura
""")

# --- player_season_stats ---
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.warehouse.player_season_stats (
        personId            BIGINT,
        player_name         STRING,
        playerteamName      STRING,
        season_start_year   INT,
        total_games_played  BIGINT,
        total_points        BIGINT,
        avg_points          DOUBLE,
        total_assists       BIGINT,
        avg_assists         DOUBLE,
        total_rebounds_sum  BIGINT,
        avg_rebounds        DOUBLE,
        total_steals        BIGINT,
        total_blocks        BIGINT,
        total_turnovers     BIGINT,
        salary_usd          BIGINT
    )
    USING iceberg
    TBLPROPERTIES (
        'write.metadata.compression-codec' = 'gzip',
        'identifier-fields' = 'personId,season_start_year'
    )
    PARTITIONED BY (season_start_year)
""")

print("Tablas gold creadas correctamente con identifier fields.")
spark.stop()