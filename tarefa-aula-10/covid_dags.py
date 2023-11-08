import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from covid_download_from_source_operator import CovidDownloadFromSourceOperator

URLS_IMDB = {
   'dataset': 'https://github.com/owid/covid-19-data/tree/master/public/data/owid-covid-data.csv',
   
}


dag1 =  DAG(dag_id=f"ingest_dataset_dag",start_date=datetime(2021,1,1),schedule_interval=None, catchup=False)
      
download_task = CovidDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['dataset'])}", url=URLS_IMDB['dataset'],dag=dag1
)
