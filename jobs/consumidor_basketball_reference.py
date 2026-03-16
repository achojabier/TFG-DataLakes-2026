from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)
import os

class NBATirosConsumer:
    TOPIC = "topic_nba_boxscore"
    TABLE = "iceberg.nba.boxscores_api"
    CHECKPOINT = "s3a://warehouse/checkpoints/boxscores_api"

    PACKAGES = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    )

    SCHEMA = StructType([
        StructField("slug", StringType(), True),
        StructField("name", StringType(), True),
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
        StructField("total_rebounds", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("steals", IntegerType(), True),
        StructField("blocks", IntegerType(), True),
        StructField("turnovers", IntegerType(), True),
        StructField("personal_fouls", IntegerType(), True),
        StructField("game_score", FloatType(), True),
        StructField("points", IntegerType(), True),
        StructField("fecha_partido", StringType(), True),
    ])

    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Consumer_NBA_Boxscores") \
            .config("spark.jars.packages", self.PACKAGES) \
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
        self.spark.sparkContext.setLogLevel("WARN")

    def _ensure_table(self):
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE} (
                slug STRING, name STRING, team STRING, location STRING,
                opponent STRING, outcome STRING,
                seconds_played INT, made_field_goals INT, attempted_field_goals INT,
                made_three_point_field_goals INT, attempted_three_point_field_goals INT,
                made_free_throws INT, attempted_free_throws INT,
                offensive_rebounds INT, defensive_rebounds INT, total_rebounds INT,
                assists INT, steals INT, blocks INT, turnovers INT,
                personal_fouls INT, game_score FLOAT, points INT,
                fecha_partido STRING
            ) USING iceberg PARTITIONED BY (fecha_partido)
        """)

    def _read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", self.TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()

    def _parse(self, df):
        return df.select(
            from_json(col("value").cast("string"), self.SCHEMA).alias("data")
        ).select("data.*")

    def run(self):
        print(f"🚀 [CONSUMER] Esperando datos en {self.TOPIC}...")
        self._ensure_table()

        query = self._parse(self._read_stream()) \
            .writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", self.CHECKPOINT) \
            .option("mergeSchema", "true") \
            .toTable(self.TABLE)

        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("🛑 [CONSUMER] Deteniendo stream...")
            query.stop()


if __name__ == "__main__":
    NBATirosConsumer().run()