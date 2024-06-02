from modules import ETL

if __name__ == '__main__':

  # Creating new ETL controller 
  controller = ETL()
  
  # Extracting the data from the API
  rawdata = controller.extract()
  print(rawdata)

  # Transforming the data to a Dataframe
  data = controller.transform(rawdata)

  print(data)
  
  # # Loading the data to the Database
  # controller.load(data)
