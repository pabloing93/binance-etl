
from modules import ETL, API, Database
from dotenv import load_dotenv
import os

def main():
  load_dotenv()

  # Creating an API object
  binance_api = API(
    os.getenv('BINANCE_API_KEY'),
    os.getenv('BINANCE_SECRET_KEY')
  )
  binance_api.connect()

  #Extracting the rawdata
  rawdata = binance_api.get_info('BTCUSDT')

  # Creating new ETL controller 
  controller = ETL()

  # Transforming the data to a Dataframe
  data = controller.transform(rawdata)

  # Cleaning the data
  data = controller.clean(data)
 
  #Creating a Database object
  database_config = {
    'host': os.getenv('REDSHIFT_HOST'),
    'port': os.getenv('REDSHIFT_PORT'),
    'user': os.getenv('REDSHIFT_USERNAME'),
    'pass': os.getenv('REDSHIFT_PASS'),
    'dbname': os.getenv('REDSHIFT_DBNAME'),
    'schema': os.getenv('REDSHIFT_SCHEMA'),
  }

  database = Database(database_config)

  if (database.connect()):
    database.load(data, 'bitcoin_candles')
    database.close_connection()
  

if __name__ == '__main__':
  main()
