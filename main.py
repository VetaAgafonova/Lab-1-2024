import os
import datetime
import pandas as pd
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

# Определение директорий
INPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'in')
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'out')

# Функция чтения файлов
def read_files(**kwargs):
    files = [os.path.join(INPUT_PATH, f'chunk{i}.csv') for i in range(26)]
    dataframes = [pd.read_csv(file) for file in files]
    combi_df = pd.concat(dataframes, ignore_index=True)
    combi_df.to_csv('buffer.csv', index=False)

# Функция фильтрации данных
def filter_nan(**kwargs):
    df = pd.read_csv('buffer.csv')
    df = df[df['designation'].notna() & df['region_1'].notna()]
    df.to_csv('buffer.csv', index=False)

# Функция замены null значений
def fill_nan(**kwargs):
    df = pd.read_csv('buffer.csv')
    df.fillna({'price': 0.0}, inplace=True)
    df.to_csv('buffer.csv', index=False)

# Функция сохранения результата в CSV
def save_res(**kwargs):
    final_df = pd.read_csv('buffer.csv')
    final_df.to_csv(os.path.join(OUTPUT_PATH, 'final_output.csv'), index=False)

# Функция сохранения в Elasticsearch
def save_elastic(**kwargs):
    df = pd.read_csv('buffer.csv')
    es_client = Elasticsearch("http://elasticsearch-kibana:9200")
    for _, row in df.iterrows():
        es_client.index(index='out_dag', body=row.to_json())


with DAG(
    dag_id='lr1_dag',
    start_date=pendulum.datetime(2024, 9, 23, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    schedule="0 0 * * *"
) as dag:
    
    read_files_task = PythonOperator(
        task_id='read_files',
        python_callable=read_files
    )
    
    filter_task = PythonOperator(
        task_id='filter_nan',
        python_callable=filter_nan
    )
    
    fill_task = PythonOperator(
        task_id='fill_nan',
        python_callable=fill_nan
    )
    
    save_csv_task = PythonOperator(
        task_id='save_res',
        python_callable=save_res
    )
    
    save_es_task = PythonOperator(
        task_id='save_elastic',
        python_callable=save_elastic
    )
    
    # Задаем порядок выполнения задач
    read_files_task >> filter_task >> fill_task >> [save_csv_task, save_es_task]
