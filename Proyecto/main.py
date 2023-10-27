from Utils.create_tables import exec_create_tables
from Utils.etl_staging import exec_etl_staging 

if __name__ == '__main__':
    exec_create_tables()
    exec_etl_staging()

