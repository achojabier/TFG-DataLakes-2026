from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType

schema_users = StructType([
    StructField("id",StringType(),False),
    StructField("email",StringType(),True),
    StructField("nombre",StringType(),True),
    StructField("apellidos",StringType(),True),
    StructField("clave",StringType(),True),
    StructField("fecha_nacimiento",DateType(),True),
    StructField("telefono",StringType(),True),
    StructField("administrador",BooleanType(),True)
])

schema_categories = StructType([
    StructField("id",StringType(),False),
    StructField("nombre",StringType(),True),
    StructField("descripcion",StringType(),True),
    StructField("ruta",StringType(),True),
    StructField("parent_id",StringType(),True),
])

schema_products = StructType([
    StructField("id",StringType(),False),
    StructField("titulo",StringType(),True),
    StructField("descripcion",StringType(),True),
    StructField("precio",DoubleType(),True),
    StructField("estado",StringType(),True),
    StructField("fecha_publicacion",TimestampType(),True),
    StructField("visualizaciones",IntegerType(),True),
    StructField("envio_disponible",BooleanType(),True),
    StructField("vendedor_id",StringType(),True),
    StructField("categoria_id",StringType(),True),
    StructField("latitud",DoubleType(),False),
    StructField("longitud",DoubleType(),False),
    StructField("descripcion_lugar",StringType(),False)
])

