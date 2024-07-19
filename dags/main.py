from utils import extract_data, transform_data, load_data
import os

# For DAGs managment
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Setting DAGs
default_args = {
  'owner': 'Pabloing',
  'start_date': datetime(2024,7,19),
  'retries': 5,
  'retry_delay': timedelta(minutes=5)
}

ETL_dag = DAG(
  dag_id = 'Binance_ETL',
  default_args = default_args,
  description = 'Bitcoin ETL Daily process',
  schedule_interval = '@hourly',
  catchup=False
)

dag_path = os.getcwd()

extract_task = PythonOperator(
  task_id = 'extract_data',
  python_callable = extract_data,
  dag = ETL_dag
)

transform_task = PythonOperator(
  task_id = 'transform_data',
  python_callable = transform_data,
  dag = ETL_dag
)

load_task = PythonOperator(
  task_id = 'load_data',
  python_callable = load_data,
  dag = ETL_dag
)

# extract_task
extract_task >> transform_task >> load_task
    
