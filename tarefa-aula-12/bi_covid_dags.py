import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bi_covid_pg_operator import BIPgOperator

URLS_IMDB = {
   'covid_data': 'owid-covid-data.csv',
   
}


dag1 =  DAG(dag_id=f"bi_covid_data_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = BIPgOperator(
   task_id=f"download_{URLS_IMDB['covid_data']}", url=URLS_IMDB['covid_data'], tablename='covid_data', dag=dag1
)

# TASKS
download_task
