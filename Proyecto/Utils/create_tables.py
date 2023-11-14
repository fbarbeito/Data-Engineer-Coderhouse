def exec_create_tables():
    from Utils.functions import conn_string,connect_to_db
    with open('Utils/create_tables.sql','r') as file:
        queries = file.read()

    conn, engine = connect_to_db(conn_string('config.ini','DB_Amazon')) # obtengo engine y sessión

    conn.execute(queries.split(';')[0].format(schema='barbeito26_coderhouse'))
    conn.execute(queries.split(';')[1].format(schema='barbeito26_coderhouse'))
    conn.execute(queries.split(';')[2].format(schema='barbeito26_coderhouse'))
    conn.execute(queries.split(';')[3].format(schema='barbeito26_coderhouse'))

    conn.invalidate() # invalido la conexión 
    engine.dispose() # limpieza de la engine

if __name__ == '__main__':
    exec_create_tables()