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
        gamedatetimeest         TIMESTAMP
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