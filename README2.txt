Tugas Day 15
Mochamad Ananda Radya Mikola

Dalam repository ini terdapat tambahan file di folder spark-scripts dan dags sebagai tugasnya. 
Adapun tambahan pada folder spark-scripts yaitu:
1. spark-extract = script untuk mengekstrak dan transform data dari database postgres menggunakan spark
2. spark-to-postgres = script untuk menyimpan dari hasil proses script 'spark-extract' ke dalam database postgres
3. spark-fulltest = script untuk menguji di lokal dari gabungan script spark-extract dan spark-to-postgres menjadi satu dengan make spark-submit-test

Adapun tambahan pada folder dags yaitu:
1. spark-dag-assignment = dag untuk menjalankan script spark-extract dan spark-to-postgres dari folder sparks-script melaui airflow dengan sparksubmitoperator
2. spark-dag-fulltest = dag untuk menjalankan fungsi script spark-fulltest melalui airflow dengan sparksubmitoperator


Catatan tambahan:
- sebenernya saya belum berhasil terus-terusan dalam menjalankan make airflow nya, namun selain itu bisa dijalankan dengan lancar.
untuk problemnya sebagai berikut:

dataeng-airflow-scheduler  | ERROR: You need to initialize the database. Please run `airflow db init`. Make sure the command is run using Airflow version 2.7.1.
dataeng-airflow-webserver  | exec /scripts/entrypoint.sh: no such file or directory
dataeng-airflow-webserver  | exec /scripts/entrypoint.sh: no such file or directory
dataeng-airflow-webserver  | exec /scripts/entrypoint.sh: no such file or directory
dataeng-airflow-webserver  | exec /scripts/entrypoint.sh: no such file or directory
dataeng-airflow-webserver exited with code 1

- saya udah cari cara untuk bisa dibaca file scriptsnya namun tetap saja tidak ada yang berhasil.
- apakah ada eror dari komputer saya atau bagaimana ya, mohon penjelasannya dari mas thosan di feedback assignmentnya 🙏🏻