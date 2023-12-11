def etl_weather(APIkey,balneario,current=1):
    '''
    input: API-Key de open weather y dataframe de los balnearios con su nombre y ubicación.
    return: Dataframe con caracteristicas del clima actual de los diferentes balnearios consultados en la Api
    '''
    import pandas as pd 
    import datetime as dt 
    import requests
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # creo un dataframe vacio para ir concatenando los auxiliares
    df_current_or_forecast = pd.DataFrame()

    # funcion lambda para transformar a utc
    fromunix_totimestamp = lambda x: dt.datetime.utcfromtimestamp(x)
    
    # funcion lambda para sustraer segundos a un datetime
    seconds_dif = lambda x: dt.timedelta(seconds=x)
    for i in range(0,balneario.shape[0]):
        
        # extraigo geolocalizacion
        lat = balneario['latitud'][i]
        lon = balneario['longitud'][i]
        
        if current == 1:
            df = pd.DataFrame()
            url = 'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={APIkey}&units=metric&lang=es'.format(lat=lat,lon=lon,APIkey=APIkey)
            # consulto la API
            logging.info(f"Obteniendo datos de openweathermap...")
            resp = requests.get(url)
            resp.raise_for_status()
            logging.info(resp.url)
            logging.info("Datos obtenidos exitosamente... Procesando datos...")
            j = resp.json()

            logging.info("Transformando datos de {balneario}".format(balneario=balneario['balneario'][i]))
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
            # concateno con el df princiapl
            df_current_or_forecast = pd.concat([df_current_or_forecast,df],axis=0)  
            
        else: 
            url_forecast = 'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={APIkey}&units=metric&cnt=24&lang=es'.format(lat=lat,lon=lon,APIkey=APIkey)
            
            logging.info(f"Obteniendo datos de openweathermap...")
            resp = requests.get(url_forecast)
            resp.raise_for_status()
            logging.info(resp.url)
            logging.info("Datos obtenidos exitosamente... Procesando datos...")

            j = resp.json()
            for x in range(0,len(j['list'])):
                logging.info("Transformando datos de {balneario}".format(balneario=balneario['balneario'][i]))
                # extraigo caracteristicas pricipales
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

                # concateno con el df princiapl
                df_current_or_forecast = pd.concat([df_current_or_forecast,df])

    # hago loop sobre las variables temporales para formatearlas
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

def exec_etl_staging(path_cred,path_beach):
    ''''
    Ejecuta el etl stagging. Consulta la API e impacta los datos en Redfshit.
    '''
    import pandas as pd
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Extraigo API key
    APIkey = conn_apikey(path_cred)

    # Balnearios UY
    balneario = pd.read_json(path_beach)

    # current weather 
    logging.info("Extrayendo y transformando datos de la temperatura actual.")
    df_current = etl_weather(APIkey=APIkey,balneario=balneario,current=1)
    logging.info("Transformación exitosa.")
    
    # forecast weather
    logging.info("Extrayendo y transformando datos de la temperatura futura.")
    df_forecast = etl_weather(APIkey=APIkey,balneario=balneario,current=0)
    logging.info("Transformación exitosa.")

    # impacto en Redfshit
    conn, engine = connect_to_db(conn_string(path_cred,'DB_Amazon'))

    df_current.to_sql(name='stg_current_weather_uybeach',schema='barbeito26_coderhouse',con=conn,if_exists='append',index=False)
    df_forecast.to_sql(name='stg_forecast_weather_uybeach',schema='barbeito26_coderhouse',con=conn,if_exists='append',index=False)
    logging.info("Impacto exitoso en Redfshit.")
    conn.invalidate() # invalido la conexión 
    engine.dispose() # limpieza de la engine

def etl_dim(path_cred,schema):
    '''
    Proceso etl para poblar las dim table
    '''
    # Creo en engine para conectar con la base de redfshit
    conn, engine = connect_to_db(conn_string(path_cred,'DB_Amazon'))

    # Queries a ejecutar 
    delete_dim_beach = '''
    delete from {schema}.dim_beach
    '''
    poblar_dim_beach = '''
    insert into {schema}.dim_beach
    select 
        row_number() over (order by beach) as beachid,
        beach,
        lon,
        lat,
        city_name
    from (
    select distinct
        beach,
        lon,
        lat,
        city_name 
    from 
        {schema}.stg_current_weather_uybeach  
    ) as f
    '''
    actualizar_dim_weather = '''
    insert into  {schema}.dim_weather
    select p.*
    from (
    select distinct
        weather_id,
        main,
        description 
    from 
         {schema}.stg_current_weather_uybeach  
    union 
    select distinct
        weather_id,
        main,
        description 
    from 
         {schema}.stg_forecast_weather_uybeach  
    ) p 
        left join  {schema}.dim_weather dw on dw.weather_id = p.weather_id 
    where 
        dw.weather_id is null
    '''
    # Limpio toda la tabla dim_beach
    conn.execute(delete_dim_beach.format(schema=schema))

    # La pueblo dim_beach
    conn.execute(poblar_dim_beach.format(schema=schema))

    # Actualizo la dim_weather
    conn.execute(actualizar_dim_weather.format(schema=schema))


    conn.close() # cierro la sesión 
    engine.dispose() # limpieza de la engine


def etl_facttable(path_cred,schema):
    '''
    Proceso etl para poblar la fact table
    '''

    # Creo en engine para conectar con la base de redfshit
    conn, engine = connect_to_db(conn_string(path_cred,'DB_Amazon'))

    # Queries a ejecutar 
    fact_table = '''
    insert into  {schema}.fact_weather_uybeach
    select 
        sfwu.dt::date as date,
        extract('hour' from sfwu.dt) as hour,
        db.beachid, 
        sfwu.sunrise::time as sunrisehour,
        sfwu.sunset::time as sunsethour,
        sfwu.weather_id,
        round(avg(sfwu.temp),2) as temp,
        round(avg(sfwu.feels_like),2) as feels_like,
        round(min(sfwu.temp_min),2) as temp_min,
        round(max(sfwu.temp_max),2) as temp_max,
        round(avg(a.temp),2) as temp_next24h,
        round(avg(a.feels_like),2) as feels_like_next24h,
        round(min(a.temp_min),2) as temp_min_next24h,
        round(max(a.temp_max),2) as temp_max_next24h,
        round(avg(a2.temp),2) as temp_nextfewh,
        round(avg(a2.feels_like),2) as feels_like_nextfewh,
        round(min(a2.temp_min),2) as temp_min_nextfewh,
        round(max(a2.temp_max),2) as temp_max_nextfewh,
        round(avg(a3.temp),2) as temp_sameh_nextday,
        round(avg(a3.feels_like),2) as feels_like_sameh_nextday,
        round(min(a3.temp_min),2) as temp_min_sameh_nextday,
        round(max(a3.temp_max),2) as temp_max_sameh_nextday
    from 
        {schema}.stg_current_weather_uybeach sfwu
        left join {schema}.dim_beach db on db.lon = sfwu.lon and db.lat = sfwu.lat
        left join {schema}.stg_forecast_weather_uybeach a on a.city_id = sfwu.city_id and a.dt between sfwu.dt + interval'1 hours' and sfwu.dt + interval'27 hours' 
        left join {schema}.stg_forecast_weather_uybeach a2 on a2.city_id = sfwu.city_id and a2.dt between sfwu.dt + interval'1 hours' and sfwu.dt + interval'4 hours' 
        left join {schema}.stg_forecast_weather_uybeach a3 on a3.city_id = sfwu.city_id and a3.dt between sfwu.dt + interval'24 hours' and sfwu.dt + interval'27 hours' 
    where 
        sfwu.dt::date = (select max(dt) from {schema}.stg_current_weather_uybeach)::date 
        and extract('hour' from sfwu.dt) = extract('hour' from  (select max(dt) from {schema}.stg_current_weather_uybeach))
    group by 1,2,3,4,5,6
    '''

    # Ejecuto 
    conn.execute(fact_table.format(schema=schema))

    conn.close() # cierro la sesión 
    engine.dispose() # limpieza de la engine


