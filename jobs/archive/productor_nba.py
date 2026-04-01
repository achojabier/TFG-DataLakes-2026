import json
import time
import random
from requests.exceptions import ReadTimeout, ConnectionError
from kafka import KafkaProducer
from trino.dbapi import connect
from nba_api.stats.endpoints import shotchartdetail, playbyplayv3
from nba_api.stats.static import teams


HEADERS_CUSTOM = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nba.com/'
}

def obtener_ultima_fecha():
    try:
        conn = connect(host="trino", port=8080, user="admin", catalog="iceberg", schema="nba")
        cur = conn.cursor()
        cur.execute("SELECT MAX(fecha_partido) FROM tiros_api")
        res = cur.fetchone()
        if res and res[0]:
            print(f"✅ Última fecha en Iceberg: {res[0]}")
            return res[0]
    except:
        pass
    print("⚠️ Tabla vacía. Iniciando carga masiva de la temporada 2025-26.")
    return "20251001"

def descargar_pbp_con_reintentos(game_id, intentos=3):
    """
    Intenta descargar el PlayByPlay con reintentos si la NBA nos bloquea.
    """
    for i in range(intentos):
        try:
            # Timeout de 60 segundos (el doble de lo normal) y headers
            pbp = playbyplayv3.PlayByPlayV3(game_id=game_id, timeout=60, headers=HEADERS_CUSTOM)
            return pbp.get_dict()
        except (ReadTimeout, ConnectionError) as e:
            tiempo_espera = (i + 1) * 15 + random.randint(1, 5) # Espera 15s, luego 30s, luego 45s... + aleatorio
            print(f"      ⚠️ Timeout con la NBA. Pausando {tiempo_espera} segundos antes de reintentar ({i+1}/{intentos})...")
            time.sleep(tiempo_espera)
        except Exception as e:
            print(f"      ❌ Error desconocido en descarga: {e}")
            return None
    return None

def ingesta_nba_api_avanzada():
    productor = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    ultima_fecha_str = obtener_ultima_fecha()
    equipos_nba = teams.get_teams()
    
    print(f"\n🚀 [FASE 1] Extrayendo mapas espaciales de los 30 equipos...")
    tiros_pendientes = []

    for i, equipo in enumerate(equipos_nba, 1):
        print(f"[{i}/30] Buscando tiros de: {equipo['full_name']}...")
        try:
            shot_chart = shotchartdetail.ShotChartDetail(
                team_id=equipo['id'],
                player_id=0,
                season_nullable='2025-26',
                context_measure_simple='FGA',
                headers=HEADERS_CUSTOM,
                timeout=60
            )
            datos_raw = shot_chart.get_dict()
            headers = datos_raw['resultSets'][0]['headers']
            rows = datos_raw['resultSets'][0]['rowSet']
            datos_tiros = [dict(zip(headers, row)) for row in rows]
            
            nuevos = 0
            for tiro in datos_tiros:
                if tiro['GAME_DATE'] > ultima_fecha_str:
                    tiros_pendientes.append(tiro)
                    nuevos += 1
            print(f"   -> Encontrados {nuevos} tiros nuevos.")
        except Exception as e:
            print(f"   -> ❌ Error con {equipo['full_name']}: {e}")
            
        time.sleep(1) 

    if not tiros_pendientes:
        print("✅ No hay tiros nuevos que procesar.")
        return

    # Agrupar por partido
    tiros_por_partido = {}
    for tiro in tiros_pendientes:
        game_id = tiro['GAME_ID']
        if game_id not in tiros_por_partido:
            tiros_por_partido[game_id] = []
        tiros_por_partido[game_id].append(tiro)

    print(f"\n🔥 [FASE 2] Descargando Play-by-Play (V3) y cruzando marcadores...")

    partidos_procesados = 0
    total_partidos = len(tiros_por_partido)
    
    for game_id, tiros in tiros_por_partido.items():
        partidos_procesados += 1
        print(f"   -> Procesando partido {game_id} ({partidos_procesados}/{total_partidos})...")
        
        
        pbp_json = descargar_pbp_con_reintentos(game_id)
        
        if pbp_json is None:
            print(f"      ❌ SALTANDO PARTIDO {game_id} tras fallar todos los reintentos.")
            continue
            
        try:
            pbp_datos = pbp_json['game']['actions']
            mapa_marcadores = {}
            marcador_actual = "0 - 0"
            
            for evento in pbp_datos:
                score_home = evento.get('scoreHome')
                score_away = evento.get('scoreAway')
                action_number = evento.get('actionNumber')
                
                if score_home is not None and score_away is not None:
                    if str(score_home).isdigit():
                         marcador_actual = f"{score_home} - {score_away}"
                
                mapa_marcadores[action_number] = marcador_actual
            
            enviados = 0
            for tiro in tiros:
                event_id_tiro = tiro['GAME_EVENT_ID']
                marcador_momento_tiro = mapa_marcadores.get(event_id_tiro, "0 - 0")
                
                es_triple = 1 if '3PT' in str(tiro.get('SHOT_TYPE','')) else 0
                valor_tiro = 3 if es_triple else 2
                anotado = 1 if tiro.get('EVENT_TYPE') == 'Made Shot' else 0
                
                registro = {
                    "id_partido": tiro['GAME_ID'],
                    "fecha_partido": tiro['GAME_DATE'],
                    "cuarto": tiro['PERIOD'],
                    "minutos_restantes": tiro['MINUTES_REMAINING'],
                    "segundos_restantes": tiro['SECONDS_REMAINING'],
                    "equipo_local": tiro['HTM'],
                    "equipo_visitante": tiro['VTM'],
                    "equipo_tira": tiro['TEAM_NAME'],
                    "jugador": tiro['PLAYER_NAME'],
                    "distancia_pies": tiro['SHOT_DISTANCE'],
                    "valor_tiro": valor_tiro,
                    "anotado": anotado,
                    "loc_x": tiro['LOC_X'],
                    "loc_y": tiro['LOC_Y'],
                    "marcador_previo": marcador_momento_tiro
                }
                
                productor.send('topic_nba_api_tiros', registro)
                enviados += 1
            
            productor.flush()
            
        except Exception as e:
            print(f"      ❌ Error procesando datos del partido {game_id}: {e}")
            
        time.sleep(random.uniform(1.0, 2.0)) 

    print("\n🚀 ¡Ingesta Masiva Completada con Éxito!")

if __name__ == "__main__":
    ingesta_nba_api_avanzada()