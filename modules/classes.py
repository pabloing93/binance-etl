import pandas
import logging
from binance import Client
from datetime import datetime

logging.basicConfig(
  filename='app.log',
  filemode='a',
  format='%(name)s - %(levelname)s - %(message)s',
  level=logging.INFO
)

class API:
  
  def __init__(self, api_key, secret_key) -> None:
    self.api_key = api_key
    self.secret_key = secret_key
    self.client = None

  def connect(self):
    try:
      self.client = Client(self.api_key, self.secret_key)
    except:
      logging.error('connecting with API')

  def get_info(self, symbol:str):
    try:
      # return self.client.get_ticker(symbol=symbol)
      return self.client.get_historical_klines(symbol=symbol, interval='1d', limit=10)
    except:
      logging.error('getting symbol ticker')

class ETL:

  def transform(self, rawdata:list):
    keys = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    data_transformed = []
    for data in rawdata:
      data_dict = dict(zip(keys, data))
      data_transformed.append(data_dict)
    
    dataframe =  pandas.DataFrame(data_transformed)
    dataframe.drop(
      columns=['quote_asset_volume', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'], 
      inplace=True
    )
    dataframe['open_time'] = pandas.to_datetime(dataframe['open_time'], unit='ms')
    dataframe['close_time'] = pandas.to_datetime(dataframe['close_time'], unit='ms')
    dataframe = dataframe.astype({
      'open': 'float',
      'high': 'float',
      'low': 'float',
      'close': 'float',
      'volume': 'float',
      'number_of_trades': 'int',
    })    
    return dataframe





# class Database:
#   def __init__(self, config: dict, schema: str) -> None:
#     self.config = config
#     self.schema = schema
  
#   def connect(self):
#     return 0
  
#   def load(data):
#     return 0
  
#   def close_connection():
#     return 0
    


  
  # database = Database()
  # database.connect()
  # database.load(data)
  # database.close_connection()