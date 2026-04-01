import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from basketball_reference_web_scraper import client
from basketball_reference_web_scraper.data import OutputType

def ingesta_diaria_nba():
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    ayer= datetime.now() - timedelta(days=1)
    print(f'📅 Iniciando ingesta para la fecha: {ayer.strftime("%Y-%m-%d")}')

    try:
        estadisticas = client.player_box_scores(day=ayer.day, month=ayer.month, year=ayer.year, output_type=OutputType.JSON)

        datos_enviados = json.loads(estadisticas)

        if not datos_enviados:
            print(f'⚠️ No se encontraron datos para la fecha {ayer.strftime("%Y-%m-%d")}.')
            return
        print(f'✅ Datos obtenidos para {len(datos_enviados)} jugadores. Enviando a Kafka...')

        for stat in datos_enviados:
            stat["fecha_partido"] = ayer.strftime("%Y-%m-%d")
            producer.send('basketball_reference', stat)
            time.sleep(0.05)
        
        producer.flush()
        print(f'🚀 Ingesta completada para la fecha {ayer.strftime("%Y-%m-%d")}')
    except Exception as e:
        print(f'❌ Error durante la ingesta: {e}')  

if __name__ == "__main__":
    ingesta_diaria_nba()