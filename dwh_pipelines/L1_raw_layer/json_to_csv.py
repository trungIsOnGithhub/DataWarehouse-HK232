import os
import configparser

import pandas as pd

CSV_FILENAME = ["Discount_Coupon", "Online_Sales"]

def json_to_csv(csvFilePath, jsonFilePath):
    try:
        df = pd.read_json(jsonFilePath)
        df.to_csv( csvFilePath, index=False)
    except (Exception) as err:
        print(err)

config = configparser.ConfigParser()    
config.read(os.path.abspath('dwh_pipelines/local_config.ini'))

for name in CSV_FILENAME:
    json_to_csv(
        f"{os.getcwd()}{os.sep}{config['data_filepath']['CSVDATA']}{os.sep}{name}.csv",
        f"{os.getcwd()}{os.sep}{config['data_filepath']['JSONDATA']}{os.sep}{name}.json"
    )