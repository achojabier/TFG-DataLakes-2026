from pyspark.sql import SparkSession
from datetime import date, datetime
from schemas import schema_users, schema_products, schema_categories

def generar_datos_prueba(spark: SparkSession):
    data_users = [
    ("U1", "juan@alum.um.es", "Juan", "Pérez", "pass123", date(1990, 5, 15), "600111222", False),
    ("U2", "maria@alum.um.es", "María", "López", "secure456", date(1985, 10, 20), "600333444", False),
    ("U3", "admin@segundum.es", "Admin", "System", "adminpass", date(1980, 1, 1), None, True)
    ]

    data_categories = [
        ("CAT1", "Electrónica", "Gadgets", "/img/elec.png", None),
        ("CAT2", "Hogar", "Cosas casa", "/img/hogar.png", None),
        ("CAT1_1", "Móviles", "Smartphones", "/img/movil.png", "CAT1"),
        ("CAT1_2", "Portátiles", "Laptops", "/img/laptop.png", "CAT1")
    ]

    data_products = [
        ("P100", "iPhone 12", "Perfecto estado", 450.50, "como nuevo", datetime(2023, 11, 1, 10, 0), 120, True, "U1", "CAT1_1", 37.99, -1.13, "Plaza Circular"),
        ("P101", "Sofá cama", "Pequeña mancha", 80.00, "buen estado", datetime(2023, 11, 2, 16, 30), 45, False, "U1", "CAT2", 37.98, -1.12, "Mi casa"),
        ("P102", "MacBook Pro", "Pantalla rota", 200.00, "para piezas o reparar", datetime(2023, 10, 28, 9, 0), 300, True, "U2", "CAT1_2", 40.41, -3.70, "Madrid Centro")
    ]

    df_users = spark.createDataFrame(data_users, schema_users)
    df_categories = spark.createDataFrame(data_categories, schema_categories)
    df_products = spark.createDataFrame(data_products, schema_products)
    return df_users, df_categories, df_products
