def exec_etl_staging():
    import pandas as pd
    from functions import etl_weather,conn_apikey,conn_string,connect_to_db

    # Extraigo API key
    APIkey = conn_apikey('config.ini')

    # Balnearios UY
    balneario = pd.read_json('Data/balneariosUy.json')

    # current weather 
    df_current = etl_weather(APIkey=APIkey,balneario=balneario,current=1)
    df_forecast = etl_weather(APIkey=APIkey,balneario=balneario,current=0)

    # impacto en Redfshit
    conn, engine = connect_to_db(conn_string('config.ini','DB_Amazon'))
    df_current.to_sql(name='stg_current_weather_uybeach',schema='barbeito26_coderhouse',con=conn,if_exists='append',index=False)
    df_forecast.to_sql(name='stg_forecast_weather_uybeach',schema='barbeito26_coderhouse',con=conn,if_exists='append',index=False)

if __name__ == '__main__':
    exec_etl_staging()