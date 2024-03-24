import os, json
import pandas as pd
import configparser

def get_full_month(shortened):
    full_month = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

    for month in full_month:
        if month.find(shortened) == 0:
            return month

    return shortened

config = configparser.ConfigParser()

config.read(os.path.abspath('dwh_pipelines/config.ini'))

JSONDATA_DIR = config['data_filepath']['JSONDATA']

JSON_FILENAME = 'Discount_Coupon.csv'

jsonFilePath = f'{os.getcwd()}{os.sep}{JSONDATA_DIR}{os.sep}{JSON_FILENAME}.json'

df = pd.read_json(jsonFilePath)

# given full form of month
df["Month"] = df['Month'].map(lambda ele:get_full_month(ele))

result = df.to_json(orient="records") 

### Rewrite into files
with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
    jsonf.write(json.dumps(json.loads(result), indent=4))