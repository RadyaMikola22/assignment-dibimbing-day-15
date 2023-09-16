"""
DAG ini digunakan untuk mengekstrak dan mengirimkan data ke PostgreSQL menggunakan Spark.
"""

from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_airflow_dag",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Ekstrak dan submit ke Postgres",
    start_date=days_ago(1),
)

extract_and_submit_posgtres = SparkSubmitOperator(
    application="/spark-scripts/spark-fulltest.py",
    conn_id="spark_tgs",
    task_id="spark_submit_task",
    dag=spark_dag,
)

extract_and_submit_posgtres
