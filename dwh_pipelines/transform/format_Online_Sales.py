import os, json
import pandas as pd
import configparser

def format_date(predate):
    splited_date = predate.split("/")

    if len(splited_date) <= 1:
        return predate

    return splited_date[0] + "-" + splited_date[2]

config = configparser.ConfigParser()

config.read(os.path.abspath('dwh_pipelines/config.ini'))

JSONDATA_DIR = config['data_filepath']['JSONDATA']

JSON_FILENAME = 'Online_Sales.csv'

jsonFilePath = f'{os.getcwd()}{os.sep}{JSONDATA_DIR}{os.sep}{JSON_FILENAME}.json'

df = pd.read_json(jsonFilePath)

# reaformat Date
df["Transaction_Date"] = df['Transaction_Date'].map(lambda ele:format_date(ele))

result = df.to_json(orient="records") 

### Rewrite into files
with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
    jsonf.write(json.dumps(json.loads(result), indent=4))