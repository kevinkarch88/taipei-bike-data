from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'start_date': datetime(2023, 10, 1),
    'retries': 1
}

#  single DAG for downloading files, running pytests, running Spark, and running dbt models
dag = DAG(
    'run_full_bike_pipeline',
    default_args=default_args,
    schedule_interval="*/15 20-23,0-7 * * *",  # Every 15 minutes from 8 PM to 8 AM
    catchup=False
)

# download test_main.py
download_test_main = GCSToLocalFilesystemOperator(
    task_id='download_test_main',
    bucket='taipei-bike-bucket',
    object_name='test_main.py',
    filename='/tmp/test_main.py',
    dag=dag
)

# download main.py
download_main_py = GCSToLocalFilesystemOperator(
    task_id='download_main_py',
    bucket='taipei-bike-bucket',
    object_name='main.py',
    filename='/tmp/main.py',
    dag=dag
)


# run test_main.py
def run_test():
    result = subprocess.run(['python3', '/tmp/test_main.py'], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"test_main.py failed: {result.stderr}")


run_test_task = PythonOperator(
    task_id='run_test_main_py',
    python_callable=run_test,
    dag=dag
)


# Spark job
dataproc_job = {
    'placement': {'cluster_name': 'my-cluster'},
    'pyspark_job': {
        'main_python_file_uri': 'gs://taipei-bike-bucket/main.py',
        'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar'],
        'args': ['--project=taipei-bike-data-project']
    }
}

submit_spark_job = DataprocSubmitJobOperator(
    task_id='submit_spark_job',
    job=dataproc_job,
    region='us-central1',
    project_id='taipei-bike-data-project',
    dag=dag
)

# Task to run dbt models located in the specified local directory
run_dbt_models = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd path/to/home/TaipeiBikeData/taipei_bike_project/models/example && dbt run',
    dag=dag
)

# Task dependencies within the same DAG
# download main and tests, run tests, do spark job every 15 minutes, run dbt models once
[download_test_main, download_main_py] >> run_test_task >> submit_spark_job >> run_dbt_models