import pandas as pd
import json
import time
from kafka import KafkaProducer

def simulador_partido_vivo():
    print("🏀 Iniciando simulador de tiros NBA en tiempo real...")


    productor = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("📁 Cargando datos para la simulación...")
    df = pd.read_csv('/home/iceberg/notebooks/shot_logs.csv')
    
    df = df.fillna(0)

    print("🚀 ¡PITIDO INICIAL! Retransmitiendo tiros en vivo...\n")
    
    for indice, fila in df.iterrows():
        tiro = fila.to_dict()
        productor.send('nba_tiros_vivo', tiro)
        productor.flush() 
        
        print(f"[{indice}] {tiro['player_name']} lanza de {tiro['SHOT_DIST']}ft... ¡{tiro['SHOT_RESULT']}! ({tiro['PTS']} pts)")
        
        time.sleep(1)

if __name__ == "__main__":
    simulador_partido_vivo()