from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'player_box_scores',  # Mismo ID que tienes en el log
    default_args=default_args,
    description='Ingesta Box Score usando BashOperator',
    schedule_interval='0 9 * * *',
    start_date=datetime(2025, 10, 22),
    catchup=False,
    tags=['TFG', 'Producción']
) as dag:

    # USAMOS BASH OPERATOR PARA EJECUTAR EN EL CONTENEDOR CORRECTO
    tarea_productor = BashOperator(
        task_id='etl_box_scores',
        # Este comando entra en el contenedor 'spark-consumer' y ejecuta el script allí
        bash_command='python /opt/airflow/jobs/productor_player.py'
    )

    tarea_productor