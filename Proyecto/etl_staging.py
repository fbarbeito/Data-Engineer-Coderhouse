import configparser
import pandas as pd
from functions import etl_current_weather

# Extraigo API key
config = configparser.ConfigParser()
config.read('config.ini')
APIkey = config['API']['Key']

# Balnearios UY
balneario = pd.read_json('Data/balneariosUy.json')

# current weather 
df_current = etl_current_weather(APIkey=APIkey,balneario=balneario)

# url_forecast = 'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={APIkey}&units=metric'.format(lat=lat,lon=lon,APIkey=APIkey)
















