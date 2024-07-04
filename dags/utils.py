from modules import ETL, API, Database
from dotenv import load_dotenv
import os
# global rawdata
# global clean_data

load_dotenv()

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
    global rawdata
    rawdata = binance_api.get_info('BTCUSDT')
    print(rawdata)
    print('cambios')
    

def transform_data():
    print('transform_data_execution')
    # # Creating new ETL controller 
    controller = ETL()

    # Transforming the data to a Dataframe
    data = controller.transform(rawdata)

    # # Cleaning the data
    global clean_data
    clean_data = controller.clean(data)
    print('cambios')

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
    database.load(clean_data, 'bitcoin_candles')
    database.close_connection
    print('cambios')