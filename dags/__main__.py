
from modules import ETL, API, Database
# from dotenv import load_dotenv
import os

# For DAGs managment
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def main():
  # Setting DAGs
  default_args = {
    'owner': 'Pabloing',
    'start_date': datetime(2024,6,25),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
  }

  ETL_dag = DAG(
    dag_id = 'Binance_ETL',
    default_args = default_args,
    description = 'Bitcoin ETL Daily process',
    schedule_interval = '@daily',
    catchup=False
  )

  dag_path = os.getcwd()

  def test(message:str):
    print(message)

  task_1 = PythonOperator(
    task_id = 'Tast_test_1',
    python_callable = test,
    op_args=['pepe 1'],
    dag=ETL_dag
  )

  task_2 = PythonOperator(
    task_id = 'Tast_test_2',
    python_callable = test,
    op_args=['pepe 2'],
    dag=ETL_dag
  )

  task_1 >> task_2

  # load_dotenv()

  # # Creating an API object
  # binance_api = API(
  #   os.getenv('BINANCE_API_KEY'),
  #   os.getenv('BINANCE_SECRET_KEY')
  # )

  # # Connecting API
  # connect_api = PythonOperator(
  #   task_id = 'connect_API',
  #   python_callable = binance_api.connect,
  #   op_args=['{{ ds }} {{ execution_date.hour }}'],
  #   dag=ETL_dag
  # )

  # #Extracting the rawdata
  # rawdata = binance_api.get_info('BTCUSDT')

  # # Creating new ETL controller 
  # controller = ETL()


  # # Transforming the data to a Dataframe
  # data = controller.transform(rawdata)

  # # Cleaning the data
  # data = controller.clean(data)
 
  # #Creating a Database object
  # database_config = {
  #   'host': os.getenv('REDSHIFT_HOST'),
  #   'port': os.getenv('REDSHIFT_PORT'),
  #   'user': os.getenv('REDSHIFT_USERNAME'),
  #   'pass': os.getenv('REDSHIFT_PASS'),
  #   'dbname': os.getenv('REDSHIFT_DBNAME'),
  #   'schema': os.getenv('REDSHIFT_SCHEMA'),
  # }

  # database = Database(database_config)

  # if (database.connect()):

  #   load_data = PythonOperator(
  #     task_id = 'Load data',
  #     python_callable = database.load(data, 'bitcoin_candles'),
  #     op_args=["{{ ds }} {{ execution_date.hour }}"],
  #     dag=ETL_dag
  #   )

  #   close_connection = PythonOperator(
  #     task_id = 'Close connection',
  #     python_callable = database.close_connection,
  #     op_args=["{{ ds }} {{ execution_date.hour }}"],
  #     dag=ETL_dag
  #   )
    

if __name__ == '__main__':
  main()
