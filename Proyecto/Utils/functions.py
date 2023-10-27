def etl_weather(APIkey,balneario,current=1):
    '''
    input: API-Key de open weather y dataframe de los balnearios con su nombre y ubicación.
    return: Dataframe con caracteristicas del clima actual de los diferentes balnearios consultados en la Api
    '''
    import pandas as pd 
    import datetime as dt 
    import requests
    df_current_or_forecast = pd.DataFrame()
    # funcion lambda para transformar a utc
    fromunix_totimestamp = lambda x: dt.datetime.utcfromtimestamp(x)
    # funcion lambda para sustraer segundos a un datetime
    seconds_dif = lambda x: dt.timedelta(seconds=x)
    for i in range(0,balneario.shape[0]):
        
        

        lat = balneario['latitud'][i]
        lon = balneario['longitud'][i]
        if current == 1:
            df = pd.DataFrame()
            url = 'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={APIkey}&units=metric'.format(lat=lat,lon=lon,APIkey=APIkey)

            resp = requests.get(url)
            j = resp.json()

            df['beach'] = [balneario['balneario'][i]]
            df_coord = pd.DataFrame([j['coord']])[['lon','lat']]
            df_weather = pd.DataFrame(j['weather']).drop({'icon'},axis=1).rename({'id':'weather_id'},axis=1)

            try:
                df_main = pd.DataFrame([j['main']]).drop({'sea_level','grnd_level'},axis=1)
            except:
                df_main = pd.DataFrame([j['main']])

            df_visibility = pd.DataFrame([j['visibility']],columns=['visibility'])
            df_wind = pd.DataFrame([j['wind']])
            df_clouds = pd.DataFrame([j['clouds']]).rename({'all':'clouds'},axis=1)
            df_dt = pd.DataFrame([j['dt']],columns=['dt'])
            df_shifted_dt = pd.DataFrame([j['timezone']],columns=['dt_shift'])
            try:
                df_sys = pd.DataFrame([j['sys']]).drop({'type','id'},axis=1)
            except:
                df_sys = pd.DataFrame([j['sys']])
            
            df_city = pd.DataFrame([j['name']],columns=['city_name'])
            df_city_id = pd.DataFrame([j['id']],columns=['city_id'])
        
            df = pd.concat([df_dt,df,df_sys,df_coord,df_city_id,df_city,df_weather,df_main,df_wind,df_visibility,df_clouds,df_shifted_dt],axis=1)
            # Puede haber más de una descripción del estado del clima 
            df = df.ffill()
            df_current_or_forecast = pd.concat([df_current_or_forecast,df],axis=0)  
            
        else: 
            url_forecast = 'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={APIkey}&units=metric&cnt=24'.format(lat=lat,lon=lon,APIkey=APIkey)
            resp = requests.get(url_forecast)
            j = resp.json()
            for x in range(0,len(j['list'])):
                

                df = pd.DataFrame()
                df['dt'] = [j['list'][x]['dt']]
                df['dt_shift'] = [j['city']['timezone']]
                df['beach'] = [balneario['balneario'][i]]
                df['country'] = j['city']['country']
                df['sunrise'] = j['city']['sunrise']
                df['sunset'] = j['city']['sunset']
                df_coord = pd.DataFrame([j['city']['coord']])[['lon','lat']]
                df = pd.concat([df,df_coord],axis=1)
                df['city_id'] = j['city']['id']
                df['city_name'] = j['city']['name']
                df_weather = pd.DataFrame(j['list'][i]['weather']).rename({'id':'weather_id'},axis=1).drop({'icon'},axis=1)
                df_main = pd.DataFrame([j['list'][x]['main']])[['temp','feels_like','temp_min','temp_max','pressure','humidity']]
                df_wind = pd.DataFrame([j['list'][x]['wind']])[['speed','deg','gust']]
                df = pd.concat([df,df_weather,df_main,df_wind],axis=1)
                df['visibility'] = j['list'][i]['visibility']
                df_cloud = pd.DataFrame([j['list'][x]['clouds']]).rename({'all':'clouds'},axis=1)
                df = pd.concat([df,df_cloud],axis=1)
                # Puede haber más de una descripción del estado del clima 
                df = df.ffill()
                df_current_or_forecast = pd.concat([df_current_or_forecast,df])

    for i in ['dt','sunset','sunrise']:
        df_current_or_forecast[i] = df_current_or_forecast[i].apply(fromunix_totimestamp)

    # Ajusto la hora para que sea acorde a Uy
    df_current_or_forecast['dt'] = df_current_or_forecast['dt'] + df_current_or_forecast['dt_shift'].apply(seconds_dif)
    # Borro el ajuste
    df_current_or_forecast = df_current_or_forecast.drop({'dt_shift'},axis=1)
    return df_current_or_forecast

def conn_string(config_path, config_section):
    '''
    input: path-folder de las claves y la sección
    '''
    from configparser import ConfigParser
    # Lee el archivo de configuración
    parser = ConfigParser()
    parser.read(config_path)
    
    # Lee la sección
    config = parser[config_section]
    host = config['host']
    port = config['puerto']
    dbname = config['db']
    username = config['user']
    pwd = config['pwd']

    # Construye la cadena de conexión
    conn_string = f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

    return conn_string

def conn_apikey(path):
    '''
    input: path-folder de las claves y la sección
    '''
    from configparser import ConfigParser
    # Lee el archivo de configuración
    parser = ConfigParser()
    parser.read(path)
    parser.read('config.ini')
    APIkey = parser['API']['Key']
    return APIkey

def connect_to_db(conn_string):
    """
    Crea una conexión a la base de datos.
    """
    import sqlalchemy as sa
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine