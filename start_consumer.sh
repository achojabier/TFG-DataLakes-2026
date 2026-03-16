#!/bin/sh
echo "⏳ Esperando 15 segundos a que Kafka y Trino arranquen..."
sleep 15
echo "🚀 Lanzando Consumidor Automático de Spark..."

python /home/iceberg/jobs/consumidor_player.py