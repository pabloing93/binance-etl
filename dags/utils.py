from modules import ETL, API, Database
from dotenv import load_dotenv
import os
import pandas
# global rawdata
# global clean_data

load_dotenv()
RAW_DATA_PATH = 'data.csv'
CLEAN_DATA_PATH = 'clean_data.csv'

def get_data(path):
    try:
        return pandas.read_csv(path, sep=';')
    except Exception as error:
        print(f'Error in get_data: {error}')

def save_data(path, data):
    try:
        data.to_csv(path, sep=';', index=False)
        print('Data saved')
    except Exception as error:
        print(f'Error in save_data: {error}')

def extract_data():
    print('exctract_data_execution')
    # # Creating an API object
    binance_api = API(
        os.getenv('BINANCE_API_KEY'),
        os.getenv('BINANCE_SECRET_KEY')
    )

    #Connecting to the API
    binance_api.connect()

    #Extracting the rawdata
    rawdata = binance_api.get_info('BTCUSDT')
    # Store rawdata as csv
    print(rawdata)
    controller = ETL()
    data = controller.transform(rawdata)
    print(data)
    save_data(RAW_DATA_PATH, data)
    

def transform_data():
    print('transform_data_execution')

    data = get_data(RAW_DATA_PATH)
    # # Cleaning the data
    print(data)
    controller = ETL()
    clean_data = controller.clean(data)

    save_data(CLEAN_DATA_PATH, clean_data)
    print(clean_data)
    

def load_data():
    print('load_data_execution')
    # #Creating a Database object
    database_config = {
        'host': os.getenv('REDSHIFT_HOST'),
        'port': os.getenv('REDSHIFT_PORT'),
        'user': os.getenv('REDSHIFT_USERNAME'),
        'pass': os.getenv('REDSHIFT_PASS'),
        'dbname': os.getenv('REDSHIFT_DBNAME'),
        'schema': os.getenv('REDSHIFT_SCHEMA')
    }

    database = Database(database_config)
    clean_data = get_data(CLEAN_DATA_PATH)
    database.load(clean_data, 'bitcoin_candles')
    database.close_connection
    print('cambios')