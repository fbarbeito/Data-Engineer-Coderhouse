def etl_dim():
    from Utils.functions import conn_string,connect_to_db

    # Creo en engine para conectar con la base de redfshit
    conn, engine = connect_to_db(conn_string('config.ini','DB_Amazon'))

    # Abro y leo las queries para poblar las tablas de dimensiones
    with open('Utils/dim_queries/dim_beach.sql','r') as file:
        queriesbeach = file.read()

    with open('Utils/dim_queries/dim_weather.sql','r') as file:
        queriesweather = file.read()

    # Limpio toda la tabla dim_beach
    conn.execute(queriesbeach.split(';')[0].format(schema='barbeito26_coderhouse'))

    # La pueblo dim_beach
    conn.execute(queriesbeach.split(';')[1].format(schema='barbeito26_coderhouse'))

    # Actualizo la dim_weather
    conn.execute(queriesweather.split(';')[0].format(schema='barbeito26_coderhouse'))


    conn.close() # cierro la sesión 
    engine.dispose() # limpieza de la engine

if __name__ == '__main__':
    etl_dim()