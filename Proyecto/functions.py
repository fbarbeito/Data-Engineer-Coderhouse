
def etl_current_weather(APIkey,balneario):
    '''
    input: API-Key de open weather y dataframe de los balnearios con su nombre y ubicación.
    return: Dataframe con caracteristicas del clima actual de los diferentes balnearios consultados en la Api
    '''
    import pandas as pd 
    import datetime as dt 
    import tzlocal
    import requests
    df_current = pd.DataFrame()

    for i in range(0,balneario.shape[0]):
        
        fromunix_totimestamp = lambda x: dt.datetime.fromtimestamp(x,tzlocal.get_localzone())

        lat = balneario['latitud'][i]
        lon = balneario['longitud'][i]
        url = 'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={APIkey}&units=metric'.format(lat=lat,lon=lon,APIkey=APIkey)
        
        df = pd.DataFrame()
        resp = requests.get(url)
        j = resp.json()

        df['beach'] = [balneario['balneario'][i]]
        df_coord = pd.DataFrame([j['coord']])
        df_weather = pd.DataFrame(j['weather']).drop({'icon'},axis=1).rename({'id':'weather_id'},axis=1)

        try:
            df_main = pd.DataFrame([j['main']]).drop({'sea_level','grnd_level'},axis=1)
        except:
            df_main = pd.DataFrame([j['main']])

        df_visibility = pd.DataFrame([j['visibility']],columns=['visibility'])
        df_wind = pd.DataFrame([j['wind']])
        df_clouds = pd.DataFrame([j['clouds']]).rename({'all':'clouds'},axis=1)
        df_dt = pd.DataFrame([j['dt']],columns=['dt'])
        
        try:
            df_sys = pd.DataFrame([j['sys']]).drop({'type','id'},axis=1)
        except:
            df_sys = pd.DataFrame([j['sys']])
        
        df_city = pd.DataFrame([j['name']],columns=['city_name'])
        df_city_id = pd.DataFrame([j['id']],columns=['city_id'])

        df = pd.concat([df_dt,df,df_sys,df_coord,df_city_id,df_city,df_weather,df_main,df_wind,df_visibility,df_clouds],axis=1)
        df_current = pd.concat([df_current,df],axis=0)
    
    for i in ['dt','sunset','sunrise']:
        df_current[i] = df_current[i].apply(fromunix_totimestamp)
    return df_current