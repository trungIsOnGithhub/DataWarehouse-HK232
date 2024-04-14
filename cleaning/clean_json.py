import os
import configparser

config = configparser.ConfigParser()    
config.read(os.path.abspath('dwh_pipelines/config.ini'))
level1_dir = config['data_filepath']['JSONDATA']
common_dir = f'{os.getcwd()}{os.sep}{level1_dir}'

for filename in os.listdir(common_dir):
    os.remove(f'{common_dir}{os.sep}{filename}')
    print(f'Removed File: {filename}')