from trino.dbapi import connect
import pandas as pd
import json
import time
from datetime import datetime
from kafka import KafkaProducer

def obtener_ultima_fecha_iceberg():
    print("🔍 Consultando a Trino/Iceberg el último partido guardado...")
    try:
        
        conn = connect(host="trino", port=8080, user="admin")
        cur = conn.cursor()
        
        # Buscamos la fecha máxima
        cur.execute("SELECT MAX(gameDateTimeEst) FROM iceberg.nba.players_eoinamoore")
        res = cur.fetchone()
        
        if res and res[0]:
            ultima_fecha = res[0]
            print(f"✅ Último partido en Iceberg es del: {ultima_fecha}")
            return ultima_fecha
            
    except Exception as e:
        print(f"⚠️ Aviso: No se pudo consultar la tabla (quizás aún no existe o está vacía).")
        print(f"Detalle del error: {e}")
        fecha_default = '2025-10-20 00:00:00'
        print(f"➡️ Usando fecha por defecto: {fecha_default} (se cargarán todos los partidos)")
    return fecha_default

def simulador_partido_vivo():
    print("🏀 Iniciando ingesta de box scores NBA")

    productor = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    # Obtener la última fecha de partido almacenada en Iceberg para no reingestar datos antiguos
    fecha = obtener_ultima_fecha_iceberg()
    print("📁 Cargando datos...")
    df = pd.read_csv('/opt/airflow/jobs/PlayerStatistics.csv')

    df['fecha_real'] = pd.to_datetime(df['gameDateTimeEst'])
    df_filtrado = df[df['fecha_real'] > pd.to_datetime(fecha)].copy()

    df_filtrado = df_filtrado.sort_values('fecha_real', ascending=True)
    df_filtrado = df_filtrado.fillna(0)

    print("🚀 Iniciando transmisión de box scores...\n")

    for indice, fila in df_filtrado.iterrows():
        box_score = fila.to_dict()

        # Explicitly coerce nulls to correct default types
        int_fields = [
            'win', 'home', 'numMinutes', 'points', 'assists', 'blocks', 'steals',
            'fieldGoalsAttempted', 'fieldGoalsMade', 'threePointersAttempted',
            'threePointersMade', 'freeThrowsAttempted', 'freeThrowsMade',
            'reboundsDefensive', 'reboundsOffensive', 'turnovers', 'plusMinus'
        ]
        float_fields = ['foulsPersonal']
        str_fields = ['personId', 'gameId', 'firstName', 'lastName', 
                    'playerteamName', 'opponentteamName', 'gameType', 
                    'gameLabel', 'gameDateTimeEst']

        for field in int_fields:
            val = box_score.get(field)
            box_score[field] = int(val) if val is not None and not (isinstance(val, float) and pd.isna(val)) else 0

        for field in float_fields:
            val = box_score.get(field)
            box_score[field] = float(val) if val is not None and not (isinstance(val, float) and pd.isna(val)) else 0.0

        for field in str_fields:
            val = box_score.get(field)
            box_score[field] = str(val) if val is not None and not (isinstance(val, float) and pd.isna(val)) else ""

        # Clean float-formatted IDs like "204001.0" → "204001"
        for field in ['personId', 'gameId']:
            val = box_score.get(field)
            if val is not None:
                try:
                    box_score[field] = str(int(float(val)))
                except (ValueError, TypeError):
                    box_score[field] = str(val) if val else ""

        if 'fecha_real' in box_score: del box_score['fecha_real']

        productor.send('nba_players_eoinamoore', box_score)
        productor.flush()

        print(f"[{indice}] Enviando box score de {box_score['firstName']} {box_score['lastName']} del equipo {box_score['playerteamName']} contra {box_score['opponentteamName']} el día {box_score['gameDateTimeEst']}, partido de {box_score['gameType']}, {box_score['gameLabel']}...")

        time.sleep(0.05)

if __name__ == "__main__":
    simulador_partido_vivo()