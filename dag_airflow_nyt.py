from airflow import DAG
from datetime import time, datetime, timedelta, timezone
from airflow.operators.bash_operator import BashOperator
#from airflow.utils import cron


default_args = {
    'owner': 'ahyar',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 4, tzinfo=timezone(timedelta(hours=7))),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='NYC_News_final_project',
    description='final project',
    schedule_interval='15 8 * * *'
) as dag:
    task1 = BashOperator(
        task_id = 'request_api',
        bash_command = 'python3 /home/ahyar/final_project/fecth_nyt_api.py'
    )

    task2 = BashOperator(
        task_id='staging_in_hive', 
        bash_command='python3 /home/ahyar/final_project/nyt_staging.py'
    )

    task3 = BashOperator(
        task_id='transform_and_data_mart',
        bash_command='python3 /home/ahyar/final_project/transformation.py'
    )

    task4 = BashOperator(
        task_id='create_core_and_load_data_to_solr',
        bash_command='python3 /home/ahyar/final_project/connect_to_solr.py'
    )    

    task1 >> task2 >> task3 >> task4
    #task1 
