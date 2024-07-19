from modules import ETL, API, Database
from dotenv import load_dotenv
import os
import pandas
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pytz
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
        
def get_current_time():
  # Define the UTC-3 timezone
  timezone = pytz.timezone('Etc/GMT+3')

  # Get the current UTC time
  utc3_now = datetime.now(pytz.utc)

  # Convert to the specified timezone
  local_time = utc3_now.astimezone(timezone)

  return local_time.strftime('%Y-%m-%d %H:%M:%S')

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
    trend = extract_trends('BTC')
    current_time = get_current_time()
    print('CURRENT TIMEEEE:', current_time)
    # Store rawdata as csv
    print(trend)
    rawdata[0].append(trend)
    rawdata[0].append(current_time)
    controller = ETL()
    data = controller.transform(rawdata)
    print(data)
    save_data(RAW_DATA_PATH, data)
    
def extract_trends(simbol) -> str:
  # In this function we are going to user webscraping for get the Bitcoin trend to decide whether to buy or not.
  
  #Defining local util functions
  def get_column_position(a_table: BeautifulSoup, column_name: str) -> int:
    for index, columna in enumerate(list(a_table.thead.tr.find_all("th"))):
      if(columna.find('p')):
        texto_p = columna.p.text.strip()
        if column_name == texto_p:
          return index
        
  def get_simbol_row(a_table: BeautifulSoup, a_simbol: str) -> list:
    for tr in a_table.tbody:
      p_tags = tr.find_all("p")
      for p in p_tags:
        if(p.string == a_simbol):
          return list(tr)
        
  def get_trend(row: str) -> str:
    up_icon = "icon-Caret-up"
    down_icon = "icon-Caret-down"
    if(up_icon in row):
      return "up"
    else:
      return "down"
  
  #1) Web scraping
  headers = { "User-Agent": os.getenv('USER_AGENT') }
  url = "https://coinmarketcap.com/"
  request = requests.get(url, headers)
  web_content = BeautifulSoup(request.content, features="lxml")
  html_table = web_content.find("table", class_="cmc-table")
  
  #2) Getting row and column data position
  trend_column_position = get_column_position(html_table, "1h %")
  simbol_row = get_simbol_row(html_table, simbol)
  
  #3) Getting the data
  trend_icon = str(simbol_row[trend_column_position])
  bitcoin_trend = get_trend(trend_icon)
  
  return bitcoin_trend
    
    
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