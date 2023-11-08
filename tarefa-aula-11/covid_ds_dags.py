import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from covid_ds_convert_to_parquet_operator import ConvertToParquetOperator

URLS_IMDB = {
   'dataset': 'owid-covid-data.csv'
}


dag1 =  DAG(dag_id=f"covid_ds_dataset_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = ConvertToParquetOperator(
   task_id=f"download_{URLS_IMDB['dataset']}", url=URLS_IMDB['dataset'],dag=dag1
)