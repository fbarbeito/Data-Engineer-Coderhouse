import configparser
import pandas as pd
from functions import etl_weather

# Extraigo API key
config = configparser.ConfigParser()
config.read('config.ini')
APIkey = config['API']['Key']

# Balnearios UY
balneario = pd.read_json('Data/balneariosUy.json')

# current weather 
df_current = etl_weather(APIkey=APIkey,balneario=balneario,current=1)
df_forecast = etl_weather(APIkey=APIkey,balneario=balneario,current=0)




