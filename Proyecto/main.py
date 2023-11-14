from Utils.create_tables import exec_create_tables
from Utils.etl_staging import exec_etl_staging 
from Utils.etl_dim import etl_dim

if __name__ == '__main__':
    exec_create_tables()
    exec_etl_staging()
    etl_dim()

