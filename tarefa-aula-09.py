from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import pandas as pd
import numpy as np
import os

def buscaEstadosMunicipios():
    df_estados = pd.read_csv('./estados.csv')
    df_municipios = pd.read_csv('./municipios.csv')

    df_estados_municipios = df_estados.merge(df_municipios, on='codigo_uf', how='left')
    df_estados_municipios = df_estados_municipios.rename(columns={
        'nome_x': 'nome_estado',
        'nome_y': 'nome_cidade'
    })
    return df_estados_municipios
def imprimiEstadosMunicipios():
    df_estados_municipios = buscaEstadosMunicipios
    grupos_estado = df_estados_municipios.groupby('uf')
    for estado, grupo in grupos_estado:
        cidades_estado = pd.DataFrame(grupo["nome_cidade"].tolist(), columns=["Cidade"])
        print(f'\n Estado: {estado}')
        print('\n')
        print(f'Cidades: {cidades_estado.head()}')


# Cria uma DAG

with DAG(
    dag_id='estados_municipios',
    start_date=datetime(2021,1,1),
    schedule_interval=None,
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id="busca_estados_municipios",
        python_callable=buscaEstadosMunicipios
    )
    task2 = PythonOperator(
        task_id="imprimi_estados_municipios",
        python_callable=imprimiEstadosMunicipios
    )
   

task1 >> task2

