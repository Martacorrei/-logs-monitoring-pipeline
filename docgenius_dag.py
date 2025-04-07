from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import asyncio

from src.docgenius.logs_main import process_logs_with_email
from src.docgenius.kibana_main import fetch_kibana_logs


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')

def run_fetch_kibana_logs():
    logging.info('Starting fetch kibana logs task...')
    return asyncio.run(fetch_kibana_logs())

def run_process_logs(ti):
    log_file_path = ti.xcom_pull(task_ids='fetch_logs_task')
    logging.info(f'Processing logs from {log_file_path}...')
    return asyncio.run(process_logs_with_email(log_file_path))

default_args = {
    'owner': 'marianasilva',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='docgenius_logs', 
    default_args=default_args,
    description='Docgenius logs processing pipeline',
    schedule_interval='0 3 * * *',  # Executa todos os dias às 03:00
    default_view='graph',
    catchup=False  # Para evitar a execução de pendências antigas
    ) as dag:

    fetch_logs_task = PythonOperator(
        task_id='fetch_logs_task',
        python_callable=run_fetch_kibana_logs,
    )

    process_logs_task = PythonOperator(
        task_id='process_logs_task',
        python_callable=run_process_logs
    )

    fetch_logs_task >> process_logs_task