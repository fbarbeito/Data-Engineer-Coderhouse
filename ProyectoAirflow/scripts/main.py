from utils import exec_etl_staging 
from utils import etl_dim
from utils import etl_facttable


path_beach = r'config\balneariosUy.json'
path_cred = r'config\config.ini'

if __name__ == '__main__':
    exec_etl_staging(path_beach=path_beach,path_cred=path_cred)
    etl_dim(path_cred='config\config.ini',schema='barbeito26_coderhouse')
    etl_facttable(path_cred='config\config.ini',schema='barbeito26_coderhouse')

