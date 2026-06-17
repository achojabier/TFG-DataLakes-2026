from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_mantenimiento_lakehouse',
    default_args=default_args,
    description='Rutina semanal de optimización y limpieza de metadatos en Apache Iceberg',
    schedule_interval='0 3 * * 0',
    catchup=False,
    tags=['dataops', 'iceberg', 'maintenance'],
) as dag:

    ejecutar_mantenimiento = BashOperator(
        task_id='run_iceberg_maintenance',
        bash_command='python /opt/airflow/jobs/spark/mantenimiento_iceberg.py',
        execution_timeout=timedelta(minutes=30)
    )

    ejecutar_mantenimiento