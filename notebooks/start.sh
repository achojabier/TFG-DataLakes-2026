#!/bin/bash

# 1. Espera de seguridad (Wait strategy)
echo "⏳ [INIT] Esperando 15 segundos a que MinIO y el Catálogo respiren..."
sleep 15

# 2. Comprobación de seguridad (¿Existe el script de Python?)
SCRIPT_PATH="/home/iceberg/notebooks/bootstrap.py"

if [ -f "$SCRIPT_PATH" ]; then
    echo "🚀 [INIT] Ejecutando recuperación de datos..."
    
    # Ejecutamos Spark apuntando al master local del contenedor
    /opt/spark/bin/spark-submit --master local[*] $SCRIPT_PATH
    
    if [ $? -eq 0 ]; then
        echo "✅ [INIT] ¡ÉXITO! El catálogo se ha recuperado."
    else
        echo "❌ [INIT] El script de Python falló."
        exit 1
    fi
else
    echo "❌ [INIT] CRÍTICO: No encuentro $SCRIPT_PATH. Revisa que el archivo exista."
    exit 1
fi