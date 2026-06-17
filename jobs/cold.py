from spark.spark_utils import get_spark_session

def main():
    # 1. Arrancamos tu motor super-tuneado
    spark = get_spark_session("Cold_Save_Emergency")
    
    print("Extrayendo datos de la capa Bronce (Landing)...")
    df_bronze = spark.sql("SELECT * FROM iceberg.landing.players_eoinamoore")
    
    # 2. Define la ruta de tu bucket de Cold Storage en MinIO
    # (Asegúrate de que el bucket 'raw-archive' existe en tu MinIO)
    ruta_cold_storage = "s3a://raw-archive/boxscores/backup_seguridad_2026/"
    
    print(f"Escribiendo datos inmutables en {ruta_cold_storage} ...")
    
    # 3. Guardamos en formato Parquet (comprimido y eficiente para archivo)
    df_bronze.write \
        .mode("append") \
        .parquet(ruta_cold_storage)
        
    print("¡Misión cumplida! Datos congelados en el Cold Storage para siempre. 🧊")
    
    spark.stop()

if __name__ == "__main__":
    main()