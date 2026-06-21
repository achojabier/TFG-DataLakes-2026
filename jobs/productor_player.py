import pandas as pd
import json
import time
import os
from kafka import KafkaProducer

ARCHIVO_ESTADO = '/opt/airflow/jobs/watermark.json'
FECHA_DEFAULT = '2026-05-01'

def obtener_ultimo_estado():
    print("Comprobando archivo de estado (Watermark)...")
    if os.path.exists(ARCHIVO_ESTADO):
        try:
            with open(ARCHIVO_ESTADO, 'r') as f:
                datos = json.load(f)
                ultima_fecha = datos.get('ultima_fecha', FECHA_DEFAULT)
                print(f"Último partido procesado fue el: {ultima_fecha}")
                return ultima_fecha
        except Exception as e:
            print(f"Error leyendo el estado: {e}. Se usará la fecha por defecto.")
    else:
        print("No hay estado previo. Iniciando carga histórica desde cero.")
    
    return FECHA_DEFAULT

def guardar_nuevo_estado(nueva_fecha):
    with open(ARCHIVO_ESTADO, 'w') as f:
        json.dump({'ultima_fecha': nueva_fecha}, f)
    print(f"Estado actualizado con éxito. Nuevo Watermark: {nueva_fecha}")

def simulador_partido_vivo():
    ultima_fecha_procesada = obtener_ultimo_estado()
    
    productor = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("Cargando datos del CSV...")
    df = pd.read_csv('/opt/airflow/jobs/PlayerStatistics.csv')
    df['fecha_real'] = pd.to_datetime(df['gameDateTimeEst'], utc=True)
    
    df_filtrado = df[df['fecha_real'].dt.strftime('%Y-%m-%d') > ultima_fecha_procesada].copy()

    if df_filtrado.empty:
        print(f"El sistema está al día. No hay partidos nuevos después del {ultima_fecha_procesada}.")
        return

    df_filtrado = df_filtrado.sort_values('fecha_real', ascending=True)
    df_filtrado = df_filtrado.fillna(0)

    print(f"Se han encontrado {len(df_filtrado)} eventos nuevos. Iniciando transmisión...\n")

    nueva_fecha_maxima = df_filtrado['fecha_real'].max().strftime('%Y-%m-%d')

    for indice, fila in df_filtrado.iterrows():
        box_score = fila.to_dict()

        int_fields = ['win', 'home', 'numMinutes', 'points', 'assists', 'blocks', 'steals', 'fieldGoalsAttempted', 'fieldGoalsMade', 'threePointersAttempted', 'threePointersMade', 'freeThrowsAttempted', 'freeThrowsMade', 'reboundsDefensive', 'reboundsOffensive', 'turnovers', 'plusMinus', 'foulsPersonal']
        str_fields = ['personId', 'gameId', 'firstName', 'lastName', 'playerteamName', 'opponentteamName', 'gameType', 'gameLabel', 'gameDateTimeEst']

        for field in int_fields:
            val = box_score.get(field)
            box_score[field] = int(val) if val is not None and not (isinstance(val, float) and pd.isna(val)) else 0

        for field in str_fields:
            val = box_score.get(field)
            box_score[field] = str(val) if val is not None and not (isinstance(val, float) and pd.isna(val)) else ""

        for field in ['personId', 'gameId']:
            val = box_score.get(field)
            if val is not None:
                try: box_score[field] = str(int(float(val)))
                except (ValueError, TypeError): box_score[field] = str(val) if val else ""

        if 'fecha_real' in box_score: del box_score['fecha_real']

        productor.send('nba_players_eoinamoore', box_score)
        print(f"Enviando partido del {box_score['gameDateTimeEst']} -> {box_score['firstName']} {box_score['lastName']}")
        
        time.sleep(0.01) 

    productor.flush()
    
    guardar_nuevo_estado(nueva_fecha_maxima)
    print("Ingesta finalizada con éxito.")

if __name__ == "__main__":
    simulador_partido_vivo()