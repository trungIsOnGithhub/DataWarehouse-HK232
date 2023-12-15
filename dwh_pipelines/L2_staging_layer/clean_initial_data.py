import os, json
import pandas as pd
import configparser

def initial_json_clean(jsonFilePath): 
    try:
        df = pd.read_json(jsonFilePath)

        print("\033[92m {}\033[00m".format(df.info()))
        print("\033[92m {}\033[00m".format(df.describe()))
        
        # drop missing value
        df.dropna(inplace=True)

        # drop duplicate
        duplicate_rows = df.duplicated()
        if duplicate_rows.any():
            df.drop_duplicates(inplace=True)
            print("\033[91m {}\033[00m".format("Number of duplicate rows deleted: ", duplicate_rows.sum()))

        result = df.to_json(orient="records")
        parsed = json.loads(result)
        # json_content = json.loads()

        # print(len(parsed))

        # json_array = []
        # for key in json_content.keys():
        #     json_array.append(json_content[key])

        with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
            jsonf.write(json.dumps(parsed, indent=4))

    except (Exception) as err:
        print("\033[91m {}\033[00m".format("ERROR while Initial Cleaning Data" ))
        print(err)

config = configparser.ConfigParser()    
config.read(os.path.abspath('dwh_pipelines/local_config.ini'))

JSONDATA_DIR = config['data_filepath']['JSONDATA']

JSON_FILENAMES = [filename for filename in os.listdir(f'{os.getcwd()}{os.sep}{JSONDATA_DIR}')]

for name in JSON_FILENAMES:
    initial_json_clean(f"{os.getcwd()}{os.sep}{JSONDATA_DIR}{os.sep}{name}")