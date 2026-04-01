from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'player_box_scores',
    default_args=default_args,
    description='Ingesta Box Scores',
    schedule_interval='0 8 * * *', #Hora de ejecución: 08:00 AM todos los días
    start_date=datetime(2025, 10, 20),
    catchup=False,
    tags=['TFG', 'Producción']
) as dag:
    tarea_productor = BashOperator(
        task_id='etl_box_scores',
        bash_command='python /opt/airflow/jobs/productor_player.py',
        execution_timeout=timedelta(minutes=30)
    )

    tarea_productor