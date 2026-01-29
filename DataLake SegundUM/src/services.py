from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, year, month, desc

def calcular_jerarquia_categorias(df_categories: DataFrame)->DataFrame:
    df_raiz = df_categories.filter(col("parent_id").isNull()) \
        .withColumn("jerarquia", concat(lit("/"),col("id"),lit("/")))
    
    df_hijos = df_categories.filter(col("parent_id").isNotNull())

    df_jerarquia = df_hijos.alias("hijo").join(
        df_raiz.alias("padre"),
        col("hijo.parent_id") == col("padre.id"),
        "inner"
    ).select(
        col("hijo.id"),
        col("hijo.nombre"),
        col("hijo.descripcion"),
        col("hijo.ruta"),
        col("hijo.parent_id"),
        concat("padre.jerarquia", col("hijo.id"), lit("/")).alias("jerarquia")
    )

    return df_raiz.union(df_jerarquia)

def historial_mensual(df_products: DataFrame, df_categories: DataFrame, target_year: int, target_month:int)->DataFrame:
    df_filtrado = df_products.filter(
        ((year(col("fecha_publicacion"))==target_year) & (month(col("fecha_publicacion"))==target_month))
    )

    df_joined = df_filtrado.alias("filtrado").join(
        df_categories.alias("categorias"),
        col("filtrado.categoria_id") == col("categorias.id"),
        "inner"
    )

    return df_joined.select(
        col("filtrado.id"),
        col("filtrado.titulo"),
        col("filtrado.precio"),
        col("filtrado.fecha_publicacion"),
        col("categorias.nombre").alias("nombre_categoria"),
        col("filtrado.visualizaciones")
    ).orderBy(desc("visualizaciones"))