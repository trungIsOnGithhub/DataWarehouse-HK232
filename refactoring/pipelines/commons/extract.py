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

def load_json_data(source_filename):
    source_filepath = + source_filename

    with open(source_filepath, 'r') as customer_info_file:    
        try:
            customer_info_data = json.load(customer_info_file)
            root_logger.info(f"Successfully located '{src_file}'")
            root_logger.info(f"File type: '{type(customer_info_data)}'")

            return customer_info_data
        except:
            root_logger.error("Unable to locate source file...terminating process...")
            raise Exception("No source file located")