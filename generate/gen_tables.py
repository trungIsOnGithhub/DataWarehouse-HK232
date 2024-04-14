import os
import platform
# import psycopg2
# import configparser

# # ======================== AUTOMATE CREATING DB ==============================
# config  =   configparser.ConfigParser()
# path    =   os.path.abspath('dwh_pipelines/config.ini')
# config.read(path)
# host                    =   config['data_filepath']['HOST']
# port                    =   config['data_filepath']['PORT']
# database                =   config['data_filepath']['DWH_DB']
# databaseoltp                =   config['data_filepath']['OLTP_DB']
# username                =   config['data_filepath']['USERNAME']
# password                =   config['data_filepath']['PASSWORD']
# postgres_connection     =   None
# cursor                  =   None
# postgres_connection = psycopg2.connect(
# host        =   host,
# port        =   port,
# dbname      =   database,
# user        =   username,
# password    =   password,
# )
# postgres_connection.set_session(autocommit=True)
# cursor = postgres_connection.cursor()
# cursor.execute(f"CREATE DATABASE {databaseoltp};")

# ======================== RUN GENERATION SCRIPT ==============================
if platform.system() == "Windows":
    os.system('genoltp')
else:
    os.system('./genoltp.sh')

level1_dir = 'dwh_pipelines'
level2_dir = 'tables'
common_dir = f'{level1_dir}{os.sep}{level2_dir}'

for filename in [filename for filename in os.listdir(f'{os.getcwd()}{os.sep}{common_dir}') if filename.startswith('tbl_')]:
    if os.sep == '/': # linux, mac
        os.system(f'python3 {common_dir}{os.sep}{filename}')
    else:
        os.system(f'python {common_dir}{os.sep}{filename}')