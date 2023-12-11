from utils import exec_etl_staging 
from utils import etl_dim
from utils import etl_facttable
from utils import enviomail_por_listas





path_beach = r'config\balneariosUy.json'
path_cred = r'config\config.ini'
path_correos = r'config\listacorreos.txt'

if __name__ == '__main__':
    exec_etl_staging(path_beach=path_beach,path_cred=path_cred,schema='barbeito26_coderhouse')
    etl_dim(path_cred=path_cred,schema='barbeito26_coderhouse')
    etl_facttable(path_cred=path_cred,schema='barbeito26_coderhouse')
    enviomail_por_listas(pathclaves=path_cred,path_correos=path_correos,schema='barbeito26_coderhouse')

