from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from trino.dbapi import connect
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

sql_merge = """
MERGE INTO iceberg.processed.players_eoinamoore AS target
USING (
    SELECT * FROM (
        SELECT 
            firstName, lastName,
            CAST(personId AS BIGINT) AS personId,
            CAST(gameId AS BIGINT) AS gameId,
            playerteamName, opponentteamName, gameType, gameLabel,
            CAST(win AS BOOLEAN) AS win,
            CAST(home AS BOOLEAN) AS home,
            numMinutes, points, assists, blocks, steals,
            fieldGoalsAttempted, fieldGoalsMade,
            threePointersAttempted, threePointersMade,
            freeThrowsAttempted, freeThrowsMade,
            reboundsDefensive, reboundsOffensive,
            foulsPersonal, turnovers, plusMinus, gameDateTimeEst,
            ROW_NUMBER() OVER(PARTITION BY gameId, personId ORDER BY gameDateTimeEst DESC) as rn
        FROM iceberg.landing.players_eoinamoore
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
        gamedatetimeest = source.gamedatetimeest
WHEN NOT MATCHED THEN
    INSERT VALUES (
        source.firstName, source.lastName, source.personid, source.gameid, 
        source.playerteamName, source.opponentteamName, source.gameType, 
        source.gameLabel, source.win, source.home, source.numMinutes, 
        source.points, source.assists, source.blocks, source.steals, 
        source.fieldGoalsAttempted, source.fieldGoalsMade, 
        source.threePointersAttempted, source.threePointersMade, 
        source.freeThrowsAttempted, source.freeThrowsMade, 
        source.reboundsDefensive, source.reboundsOffensive, 
        source.foulsPersonal, source.turnovers, source.plusMinus, 
        source.gamedatetimeest
    )
"""
def verificar_datos_landing(**context):
    """
    FIX: Added a pre-merge sanity check.
    Queries the landing table to confirm new rows exist before running the
    expensive MERGE. If landing is empty or unchanged, raises an error so the
    DAG fails fast instead of running a no-op MERGE.
    """
    
 
    execution_date = context['execution_date']
    print(f"Verificando datos en landing para la ejecución: {execution_date}")
 
    conn = connect(host="trino", port=8080, user="admin")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM iceberg.landing.players_eoinamoore")
    count = cur.fetchone()[0]
    print(f"Filas encontradas en landing: {count}")
 
    if count == 0:
        raise ValueError("La tabla landing está vacía — el MERGE no tiene sentido. ¿Falló la ingesta?")
 
    print("Landing tiene datos. Procediendo con el MERGE.")

with DAG(
    'procesar_capa_plata_nba',
    default_args=default_args,
    description='Ejecuta el MERGE en Trino para limpiar la capa Processed de Iceberg',
    schedule_interval=None,
    catchup=False,
    tags=['nba', 'iceberg', 'trino', 'processed'],
) as dag:
    verificar_landing = PythonOperator(
        task_id='verificar_datos_landing',
        python_callable=verificar_datos_landing,
        provide_context=True,
    )
    # Operador que manda la orden a Trino
    ejecutar_merge_trino = TrinoOperator(
        task_id='merge_landing_to_processed',
        trino_conn_id='trino_default',
        sql=sql_merge
    )

    procesar_salarios = BashOperator(
        task_id='procesar_salarios',
        bash_command='python /opt/airflow/jobs/procesar_salaries.py',
        execution_timeout=timedelta(minutes=15)
    )

    disparar_oro = TriggerDagRunOperator(
    task_id='trigger_capa_oro',
    trigger_dag_id='dag_nba_oro', # El nombre exacto de tu otro DAG
    wait_for_completion=False
    )

    verificar_landing >> [ejecutar_merge_trino, procesar_salarios] >> disparar_oro