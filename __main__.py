# TODO: Encapsular las clases y delegar la responsabildiad al controlador de obtener los datos
# TODO: Utilizar este archivo como controlador y pasar los datos a las clases 
# TODO: La responsabilidad de las clases solamente es la de definir métodos. El controlador es quien las manipula.


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
