from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


default_args = {
    'start_date': datetime(2023, 10, 1),
    'retries': 1
}


# runs dbt models at 8:15 AM daily
dag = DAG(
    'run_dbt_models_at_8_15',
    default_args=default_args,
    schedule_interval='15 8 * * *',  # Run at 8:15 AM every day
    catchup=False
)


# run dbt models
run_dbt_models = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd path/to/TaipeiBikeData/taipei_bike_project/models/example && dbt run',
    dag=dag
)
