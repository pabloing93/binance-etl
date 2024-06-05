import pandas
import logging
from binance import Client
from sqlalchemy import create_engine
import sqlalchemy

logging.basicConfig(
  filename='app.log',
  filemode='a',
  format='%(asctime)s ::ClassesModule -> %(name)s - %(levelname)s - %(message)s',
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
      return self.client.get_historical_klines(symbol=symbol, interval='1d')
    except:
      logging.error('getting symbol ticker')

class ETL:

  def transform(self, rawdata:list):
    keys = ['open_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'close_time', 'quote_asset_volume', 'trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
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
      'open_price': 'float',
      'high_price': 'float',
      'low_price': 'float',
      'close_price': 'float',
      'volume': 'float',
      'trades': 'int',
    })    
    return dataframe
  
class Database:
  def __init__(self, config: dict) -> None:
    self.host = config['host']
    self.port = config['port']
    self.user = config['user']
    self.password = config['pass']
    self.dbname = config['dbname']
    self.schema = config['schema']
    self.database = None
  
  def connect(self):
    try:
      url = f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}'
      self.database = create_engine(url)
      logging.info(f'Connected to: {url}')
    except Exception as error:
      logging.error(f'Failed connecting: {error}')
  
  def load(self, data: pandas.DataFrame, table: str):
    try:
      data.to_sql(
        table,
        self.database,
        schema=self.schema,
        if_exists='append',
        index=False
      )
      logging.info(f'{self.schema}.{table} has been uploaded')
    except Exception as error:
      logging.error(f'Cant execute the load: {error}')
  
  def close_connection(self):
    try:
      self.database.dispose()
    except Exception as error:
      logging.error(f'Cant dispose database: {error}')