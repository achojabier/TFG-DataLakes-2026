from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime, timedelta

# Configuración básica del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# La super-query de Iceberg
sql_merge = """
MERGE INTO iceberg.processed.players_eoinamoore AS target
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY gameId, personId ORDER BY gameDateTimeEst DESC) as rn
        FROM iceberg.nba.players_eoinamoore
    ) WHERE rn = 1
) AS source
ON target.gameId = source.gameId AND target.personId = source.personId
WHEN MATCHED THEN
    UPDATE SET 
        firstName = source.firstName,
        lastName = source.lastName,
        playerteamName = source.playerteamName,
        opponentteamName = source.opponentteamName,
        gameType = source.gameType,
        gameLabel = source.gameLabel,
        win = source.win,
        home = source.home,
        numMinutes = source.numMinutes,
        points = source.points,
        assists = source.assists,
        blocks = source.blocks,
        steals = source.steals,
        fieldGoalsAttempted = source.fieldGoalsAttempted,
        fieldGoalsMade = source.fieldGoalsMade,
        threePointersAttempted = source.threePointersAttempted,
        threePointersMade = source.threePointersMade,
        freeThrowsAttempted = source.freeThrowsAttempted,
        freeThrowsMade = source.freeThrowsMade,
        reboundsDefensive = source.reboundsDefensive,
        reboundsOffensive = source.reboundsOffensive,
        foulsPersonal = source.foulsPersonal,
        turnovers = source.turnovers,
        plusMinus = source.plusMinus,
        gameDateTimeEst = source.gameDateTimeEst
WHEN NOT MATCHED THEN
    INSERT VALUES (
        source.firstName, source.lastName, source.personId, source.gameId, 
        source.playerteamName, source.opponentteamName, source.gameType, 
        source.gameLabel, source.win, source.home, source.numMinutes, 
        source.points, source.assists, source.blocks, source.steals, 
        source.fieldGoalsAttempted, source.fieldGoalsMade, 
        source.threePointersAttempted, source.threePointersMade, 
        source.freeThrowsAttempted, source.freeThrowsMade, 
        source.reboundsDefensive, source.reboundsOffensive, 
        source.foulsPersonal, source.turnovers, source.plusMinus, 
        source.gameDateTimeEst
    )
"""

with DAG(
    'procesar_capa_plata_nba',
    default_args=default_args,
    description='Ejecuta el MERGE en Trino para limpiar la capa Processed de Iceberg',
    schedule_interval='0 8 * * *',  # Se ejecuta todos los días a las 08:00 AM
    catchup=False,
    tags=['nba', 'iceberg', 'trino', 'processed'],
) as dag:

    # Operador que manda la orden a Trino
    ejecutar_merge_trino = TrinoOperator(
        task_id='merge_landing_to_processed',
        trino_conn_id='trino_default',  # Usaremos esta conexión en Airflow
        sql=sql_merge
    )

    ejecutar_merge_trino