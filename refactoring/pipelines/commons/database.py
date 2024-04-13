import os
import psycopg2
import configparser

def read_local_config():
    local_config_file_path = os.path.abspath('pipelines/config.ini')

    config = configparser.ConfigParser()
    config.read(local_config_file_path)

    host = config['data_filepath']['HOST']
    port = config['data_filepath']['PORT']
    database = config['data_filepath']['RAW_DB']
    username = config['data_filepath']['USERNAME']
    password = config['data_filepath']['PASSWORD']

    return (host, port, database, username, password)

def get_postgres_db_connection():
    customer_info_path = config['data_filepath']['DATASETS_LOCATION_PATH'] + src_file
    connection_info = read_local_config();

    postgres_connection = psycopg2.connect(
        host = connection_info[0],
        port = connection_info[1],
        dbname = connection_info[2],
        user = connection_info[3],
        password = connection_info[4],
    )

    postgres_connection.set_session(autocommit=True)

    return postgres_connection, postgres_connection.cursor(), connection_info[2]