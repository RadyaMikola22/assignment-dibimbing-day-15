"""
Script ini dirancang untuk mengekstrak data dari database dengan mengidentifikasi 5 negara teratas 
berdasarkan total pembayaran yang tercatat dalam dataset. Untuk mencapai hal tersebut, 
langkah-langkah yang dilakukan adalah  menghitung jumlah produk yang terjual dalam setiap pesanan, 
kemudian mengalikannya dengan harga produk untuk menghitung total pembayaran dari setiap pesanan. 
Data kemudian dikelompokkan berdasarkan negara untuk menghitung total pembayaran yang diterima 
dari masing-masing negara.

"""

""" KONFIGURASI SPARK DAN POSTGRES, EKSTRAK DATA DAN ANALISIS"""
# Preparation
# Import library yang diperlukan
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Membuat sesi Spark
spark = SparkSession.builder.appName("TugasDay15Extract").getOrCreate()

# Konfigurasi JDBC
jdbc_url = "jdbc:postgresql://dibimbing-dataeng-postgres:5432/postgres_db"
properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Tabel sumber data di PostgreSQL
table_name = "retail"

# Membaca data dari PostgreSQL
try:
    retail_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
except Exception as e:
    print(f"Error reading data from database: {str(e)}")
    spark.stop()
    sys.exit(1)

# Menghitung total payment per pesanan
retail_df = retail_df.withColumn('PayTotal', F.round(F.col('Quantity') * F.col('UnitPrice'), 2))

# Menghitung total payment per negara
top_country_payments_df = retail_df \
    .groupBy('Country') \
    .agg(F.sum('PayTotal').alias('PayTotal')) \
    .withColumn('PayTotal', F.round(F.col('PayTotal'), 2))

# Mengurutkan hasil berdasarkan total payment secara descending serta diberi limit 5 agar dapat memperlihatkan 5 negara teratas saja
top_country_payments_df = top_country_payments_df \
    .orderBy(F.desc('PayTotal')) \
    .limit(5)

# Menampilkan hasil
top_country_payments_df.show()
