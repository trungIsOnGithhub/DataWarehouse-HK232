import os 
import json
import configparser

def load_json_file(filename):
    config = configparser.ConfigParser()
    path = os.path.abspath('dwh_pipelines/config.ini')
    config.read(path)
    filepath = config['data_filepath']['JSONDATA'] + os.sep + filename

    with open(filepath, 'r') as customer_info_file:    
        try:
            customer_info_data = json.load(customer_info_file)
        except:
            raise Exception("No source file located")