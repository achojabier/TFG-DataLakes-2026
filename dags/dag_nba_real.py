from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

PAQUETES_ICEBERG = 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-aws-bundle:1.5.0'

default_args = {
    'owner': 'tfg_admin',
    'depends_on_past': False,
    'retries': 3,                            
    'retry_delay': timedelta(seconds=30),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'tfg_nba_orquesta_completa',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['TFG', 'Producción', 'Lakehouse']
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    # Spark procesa el CSV y escribe en Iceberg
    ingesta_spark = SparkSubmitOperator(
        task_id='etl_spark_iceberg',
        application='/opt/airflow/jobs/etl_batch.py',
        conn_id='spark_default',
        packages=PAQUETES_ICEBERG
    )

    # Trino hace la analítica directa sobre Iceberg
    analisis_trino = SQLExecuteQueryOperator(
        task_id='analisis_top_anotadores_trino',
        conn_id='trino_default',
        sql="""
            SELECT jugador, SUM(puntos) as puntos_totales
            FROM iceberg.nba.tiros_batch
            GROUP BY jugador
            ORDER BY puntos_totales DESC
            LIMIT 5
        """,
        # Imprime el resultado en el Log de Airflow
        handler=lambda cur: [print(f"🏀 {row[0]}: {row[1]} pts") for row in cur.fetchall()]
    )

    fin = EmptyOperator(task_id='fin')

    inicio >> ingesta_spark >> analisis_trino >> fin