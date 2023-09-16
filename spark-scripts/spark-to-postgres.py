"""
Script ini dirancang untuk menyimpan hasil dari proses script spark-extract ke database postgres
"""

# Konfigurasi postgres dan spark
## Import library yang diperlukan
from pyspark.sql import SparkSession

## Membuat sesi Spark
spark = SparkSession.builder.appName("TugasDay15toPostgres").getOrCreate()

## Konfigurasi JDBC
jdbc_url = "jdbc:postgresql://dibimbing-dataeng-postgres:5432/postgres_db"
properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Simpan ke Postgres
## Tabel target di PostgreSQL untuk hasil
target_table_name = "top_country_payments_df"

## Menyimpan hasil analisis dari bentuk dataframe ke tabel dalam database
top_country_payments_df.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, table=target_table_name, mode="overwrite", properties=properties)
