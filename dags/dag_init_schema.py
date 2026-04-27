from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    'dag_init_schema',
    description='Crea tablas Iceberg con identifier fields via Spark (ejecutar manualmente)',
    schedule_interval=None,
    start_date=datetime(2025, 10, 20),
    catchup=False,
    tags=['nba', 'iceberg', 'schema', 'init'],
) as dag:

    crear_tablas_processed = BashOperator(
        task_id='crear_tablas_processed',
        bash_command=(
            'spark-submit '
            '--master local[*] '
            '--name init_processed_schema '
            '--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
            'org.apache.hadoop:hadoop-aws:3.3.4,'
            'org.apache.iceberg:iceberg-aws-bundle:1.5.0 '
            '/opt/airflow/jobs/spark/init_processed_tables.py'
        ),
        execution_timeout=timedelta(minutes=10),
    )

    crear_tablas_gold = BashOperator(
        task_id='crear_tablas_gold',
        bash_command=(
            'spark-submit '
            '--master local[*] '
            '--name init_gold_schema '
            '--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
            'org.apache.hadoop:hadoop-aws:3.3.4,'
            'org.apache.iceberg:iceberg-aws-bundle:1.5.0 '
            '/opt/airflow/jobs/spark/init_gold_tables.py'
        ),
        execution_timeout=timedelta(minutes=10),
    )

    crear_tablas_processed >> crear_tablas_gold