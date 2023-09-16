"""
DAG ini digunakan untuk mengekstrak dan mengirimkan data ke PostgreSQL menggunakan Spark.
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

# Tentukan parameter DAG
spark_dag = DAG(
    dag_id="spark_postgres_dag",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Extract dan hasilnya di submit ke Postgres",
    start_date=days_ago(1),
)

# Definisi task untuk menjalankan Spark Job untuk ekstrak
extract_task = SparkSubmitOperator(
    application="/spark-scripts/spark-extract.py",
    conn_id="spark_tgs",
    task_id="extract_task",
    dag=spark_dag,
)

# Definisi task untuk menyimpan hasil ke PostgreSQL
submit_to_postgres_task = SparkSubmitOperator(
    application="/spark-scripts/spark-to-postgres.py",
    conn_id="spark_tgs",
    task_id="submit_postgres_task",
    dag=spark_dag,
)

# Atur ketergantungan antara task
extract_task >> submit_to_postgres_task