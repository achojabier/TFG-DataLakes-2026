from pyspark.sql import SparkSession
from data_loader import generar_datos_prueba
from services import calcular_jerarquia_categorias, historial_mensual
import config
from spark_utils import get_spark_session

def main():
    spark = get_spark_session("segundUM_ETL")
    
    df_users, df_categories, df_products = generar_datos_prueba(spark)

    df_cat_final = calcular_jerarquia_categorias(df_categories)
    df_cat_final.show(truncate=False)

    df_reporte = historial_mensual(df_products,df_cat_final, 2023,11)
    df_reporte.show()

    nombre_tabla = f"{config.CATALOG_NAME}.{config.NAMESPACE}.reporte_ventas"
    print(f"--- 3. Guardando en Iceberg: {nombre_tabla} ---")
    
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.CATALOG_NAME}.{config.NAMESPACE}")
    
    df_reporte.writeTo(nombre_tabla) \
        .createOrReplace()

    df_reporte.write \
        .mode("overwrite") \
        .option("header","true") \
        .csv("output/reporte_mensual_csv")
    
    df_reporte.write \
        .mode("overwrite") \
        .option("header","true") \
        .parquet("output/reporte_mensual_parquet")


    spark.stop()

if __name__ == "__main__":
    main()