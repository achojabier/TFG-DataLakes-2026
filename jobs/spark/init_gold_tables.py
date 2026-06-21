import os
from pyspark.sql import SparkSession

MINIO_USER = os.environ.get("MINIO_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "admin123")

spark = SparkSession.builder \
    .appName("Init_Processed_Tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", MINIO_USER) \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", MINIO_PASSWORD) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("Limpiando esquemas antiguos de Oro...")
spark.sql("DROP TABLE IF EXISTS iceberg.warehouse.game_logs")
spark.sql("DROP TABLE IF EXISTS iceberg.warehouse.player_season_stats")

spark.sql("""
    CREATE TABLE iceberg.warehouse.game_logs (
        personid BIGINT,
        player_name STRING,
        gameid BIGINT,
        playerteamname STRING,
        opponentteamname STRING,    
        home BOOLEAN,               
        win BOOLEAN,                
        gametype STRING,            
        points INT,
        assists INT,
        reboundsoffensive INT,
        reboundsdefensive INT,
        totalrebounds INT,
        steals INT,
        blocks INT,
        turnovers INT,
        fieldgoalsmade INT,         
        fieldgoalsattempted INT,    
        threepointersmade INT,      
        threepointersattempted INT, 
        freethrowsmade INT,         
        freethrowsattempted INT,    
        foulspersonal INT,          
        plusminus DOUBLE,           
        numminutes DOUBLE,
        game_date DATE,
        prev_game_date DATE,
        is_back_to_back BOOLEAN,
        rating DOUBLE
    )
    USING iceberg
    TBLPROPERTIES (
        'write.metadata.compression-codec' = 'gzip',
        'identifier-fields'  = 'gameid,personid',
        'format-version' = '2'
    )
    PARTITIONED BY (playerteamname)
""")

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
        salary_usd BIGINT,
        avg_rating DOUBLE
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

print("Tablas gold creadas correctamente.")
spark.stop()