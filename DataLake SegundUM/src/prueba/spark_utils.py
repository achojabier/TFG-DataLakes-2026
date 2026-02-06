from pyspark.sql import SparkSession
import config
import boto3
from botocore.exceptions import ClientError

def get_spark_session(app_name: str = "SegundUM_App") -> SparkSession:
    _ensure_minio_bucket_exists("warehouse")
    
    # 2. Construir la sesión
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[4]") \
        .config("spark.jars.packages", ",".join(config.PACKAGES)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.default.catalog", config.CATALOG_NAME)
    
    # 3. Inyectar configuración del Catálogo REST (Iceberg)
    builder = _configure_iceberg_catalog(builder)
    
    # 4. Inyectar configuración de S3 (MinIO)
    builder = _configure_s3_storage(builder)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark

def _ensure_minio_bucket_exists(bucket_name):
    """Verifica si el bucket existe en MinIO y si no, lo crea usando Boto3"""
    print(f">>> Verificando bucket '{bucket_name}' en MinIO...")
    s3_client = boto3.client(
        "s3",
        endpoint_url=config.MINIO_URL,
        aws_access_key_id=config.AWS_ACCESS_KEY,
        aws_secret_access_key=config.AWS_SECRET_KEY,
        region_name="us-east-1"
    )
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f">>> Bucket '{bucket_name}' ya existe.")
    except ClientError:
        print(f">>> El bucket no existe. Creando '{bucket_name}'...")
        s3_client.create_bucket(Bucket=bucket_name)
        print(f">>> Bucket '{bucket_name}' creado correctamente.")

def _configure_iceberg_catalog(builder):
    """Configura el catálogo REST de Iceberg con credenciales EXPLÍCITAS"""
    cat_prefix = f"spark.sql.catalog.{config.CATALOG_NAME}"
    
    return builder \
        .config(f"{cat_prefix}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"{cat_prefix}.type", "rest") \
        .config(f"{cat_prefix}.uri", config.CATALOG_URL) \
        .config(f"{cat_prefix}.warehouse", config.WAREHOUSE_PATH) \
        .config(f"{cat_prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config(f"{cat_prefix}.s3.endpoint", config.MINIO_URL) \
        .config(f"{cat_prefix}.s3.path-style-access", "true") \
        .config(f"{cat_prefix}.s3.access-key-id", config.AWS_ACCESS_KEY) \
        .config(f"{cat_prefix}.s3.secret-access-key", config.AWS_SECRET_KEY) \
        .config(f"{cat_prefix}.client.region", "us-east-1")

def _configure_s3_storage(builder):
    """Configura el sistema de archivos Hadoop para hablar con MinIO"""
    return builder \
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")