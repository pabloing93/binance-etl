import pandas
import logging
from binance import Client
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
  filename='app.log',
  filemode='a',
  format='%(name)s - %(levelname)s - %(message)s',
  level=logging.INFO
)

class API:
  
  def __init__(self) -> None:
    self.api_key = 'KFaHCpB2tABy1adqTqwMkY8Df4CFHEBJT537z5lXsdcfNUEld6UApytOH7rrGDy9'
    self.secret_key = 'T1fyzT50zyY2tEDhm5KRVMdUzifvNCpGg5uQM81uKxGzpxsxGupGI2NkChTxCMcU'
    self.client = None

  def connect(self):
    try:
      self.client = Client(self.api_key, self.secret_key)
    except:
      logging.error('connecting with API')

  def get_info(self, symbol:str):
    try:
      return self.client.get_ticker(symbol=symbol)
    except:
      logging.error('getting symbol ticker')

class ETL:

  def extract(self):
    try:
      api = API()
      api.connect()
      return api.get_info('BTCUSDT')
    except:
      logging.error('extracting data')

  def transform(self, rawdata:dict):
    pass

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