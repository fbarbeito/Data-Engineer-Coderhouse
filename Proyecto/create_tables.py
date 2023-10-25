def exec_create_tables():
    from functions import conn_string,connect_to_db

    with open('create_tables.sql','r') as file:
        queries = file.read()

    conn, engine = connect_to_db(conn_string('config.ini','DB_Amazon'))
    conn.execute(queries.split(';')[0].format(schema='barbeito26_coderhouse'))
    conn.execute(queries.split(';')[1].format(schema='barbeito26_coderhouse'))

if __name__ == '__main__':
    exec_create_tables()