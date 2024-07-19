import pandas
import logging
from binance import Client
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import execute_values
# from datetime import datetime
# import pytz

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

  def connect(self) -> None:
    try:
      self.client = Client(self.api_key, self.secret_key)
      logging.info('the API connection has been established')
    except:
      logging.error('connecting with API')

  def get_info(self, symbol:str) -> list:
    try:
      info = self.client.get_historical_klines(symbol=symbol, interval='1h', limit=1)
      return info
    except:
      logging.error('getting symbol ticker')

class ETL:

  def transform(self, rawdata:list) -> pandas.DataFrame:
    logging.info('THE RAWDATA:', rawdata)
    keys = ['open_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'close_time', 'quote_asset_volume', 'trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore', 'trend', 'load_time']
    data_transformed = []
    for data in rawdata:
      data_dict = dict(zip(keys, data))
      data_transformed.append(data_dict)
    
    dataframe =  pandas.DataFrame(data_transformed)
    dataframe.drop(
      columns=['quote_asset_volume', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'], 
      inplace=True
    )
    # dataframe['load_time'] = pandas.to_datetime(get_current_time(), unit='ms')
    dataframe['load_time'] = pandas.to_datetime(dataframe['load_time'])
    dataframe['open_time'] = pandas.to_datetime(dataframe['open_time'])
    dataframe['close_time'] = pandas.to_datetime(dataframe['close_time'])
    dataframe = dataframe.astype({
      'open_price': 'float',
      'high_price': 'float',
      'low_price': 'float',
      'close_price': 'float',
      'volume': 'float',
      'trades': 'int',
      'trend': 'object'
    })    
    return dataframe
  
  def clean(self, data:pandas.DataFrame) -> pandas.DataFrame:
    try: 
      clean_data = data.copy()
      # Nan handling
      clean_data.dropna(subset=['open_time', 'volume','close_time' ,'trades'], inplace=True)
      # Duplicates handling
      clean_data.drop_duplicates(inplace=True)
      # Prices imputation

      prices_columns = ['open_price', 'high_price', 'low_price', 'close_price']
      for column in prices_columns:
        mean = clean_data[column].mean()
        clean_data[column].fillna(value=mean, inplace=True)
      logging.info(f'Clean process has done')
      return clean_data
    except Exception as error:
      logging.error(f'Clean process has failed: {error}')
  
class Database:
  def __init__(self, config: dict) -> None:
    self.host = config['host']
    self.port = config['port']
    self.user = config['user']
    self.password = config['pass']
    self.dbname = config['dbname']
    self.schema = config['schema']
    self.database = None
  
  def connect(self) -> bool:
    try:
      #This isnt persist
      url = f'postgresql://{self.user}:{self. password}@{self.host}:{self.port}/{self.dbname}'
      print(url)
      # self.database = create_engine(url)
      self.database = psycopg2.connect(
        host=self.host,
        dbname=self.dbname,
        user=self.user,
        password=self.password,
        port=self.port
      )
      logging.info(f'Connected to: {url}')
      return True
    except Exception as error:
      logging.error(f'Failed connecting: {error}')
      return False
  
  def load(self, data: pandas.DataFrame, table: str) -> None:
    try:
      database = psycopg2.connect(
        host=self.host,
        dbname=self.dbname,
        user=self.user,
        password=self.password,
        port=self.port
      )
      cursor = database.cursor()
      query = f"INSERT INTO pabloing1993_coderhouse.bitcoin_candles (open_time, open_price, high_price, low_price, close_price, volume, close_time, trades, trend, load_time) VALUES %s" 
      values = [tuple(row) for row in data.to_numpy()]
      cursor.execute("BEGIN")
      execute_values(
        cursor,
        query,
        values
      )
      cursor.execute("COMMIT")
      logging.info(f'{self.schema}.{table} has been uploaded')
    except Exception as error:
      logging.error(f'Cant execute the load: {error}')
  
  def close_connection(self) -> None:
    try:
      self.database.dispose()
      logging.info('Database disposed')
    except Exception as error:
      logging.error(f'Cant dispose database: {error}')