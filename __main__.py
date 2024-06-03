
from modules import ETL, API
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
 
  #Creating a Database object

  database = Database(
    os.getenv()
  )

  database.connect()
  database.load(data)
  database.close_connection()

  # # Loading the data to the Database
  # controller.load(data)

if __name__ == '__main__':
  main()
