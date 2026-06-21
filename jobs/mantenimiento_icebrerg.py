from spark_utils import get_spark_session

def main():
    print("Iniciando rutina de mantenimiento del Data Lakehouse (Iceberg)...")
    
    spark = get_spark_session("Mantenimiento_Iceberg")

    tablas = [
        "iceberg.processed.players_eoinamoore",
        "iceberg.processed.dim_salaries"
    ]

    for tabla in tablas:
        print(f"\n" + "="*50)
        print(f"Aplicando mantenimiento a: {tabla}")
        print("="*50)
        
        try:
            print("1/3 - Ejecutando OPTIMIZE (Compactación de binarios)...")
            spark.sql(f"ALTER TABLE {tabla} EXECUTE OPTIMIZE")

            print("2/3 - Ejecutando EXPIRE_SNAPSHOTS (Liberando espacio)...")
            spark.sql(f"ALTER TABLE {tabla} EXECUTE EXPIRE_SNAPSHOTS(older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY)")

            print("3/3 - Ejecutando REMOVE_ORPHAN_FILES (Limpieza profunda)...")
            spark.sql(f"ALTER TABLE {tabla} EXECUTE REMOVE_ORPHAN_FILES(older_than => CURRENT_TIMESTAMP - INTERVAL '3' DAY)")
            
            print(f"Mantenimiento de {tabla} completado.")
        
        except Exception as e:
            print(f"ERROR durante el mantenimiento de {tabla}: {e}")
    
    print("\nRutina de DataOps finalizada con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()