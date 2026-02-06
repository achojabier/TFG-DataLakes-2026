import os
import sys

# --- Entorno Local (Windows) ---
# Detectamos automáticamente el python actual
PYTHON_EXEC = sys.executable

# --- Versiones de Dependencias ---
# Definimos las versiones aquí para no tener "números mágicos" en el código
ICEBERG_VERSION = "1.5.0"
SPARK_SCALE = "3.5"
SCALA_VERSION = "2.12"

# Paquetes Maven necesarios
PACKAGES = [
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_SCALE}_{SCALA_VERSION}:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
]

# --- Configuración de Infraestructura (Docker) ---
MINIO_URL = "http://localhost:9000"
CATALOG_URL = "http://localhost:8181"
WAREHOUSE_PATH = "s3://warehouse/"

# Credenciales (En un proyecto real, esto vendría de os.getenv por seguridad)
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "admin123"

# Nombres de Catálogo y Namespace
CATALOG_NAME = "mi_catalogo"
NAMESPACE = "default"