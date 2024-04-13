import csv, json, os
import configparser

def csv_to_json(csvFilePath, jsonFilePath):
    jsonArray = []

    try:
        with open(csvFilePath, encoding='utf-8') as csvf: 
            csvReader = csv.DictReader(csvf) 

            for row in csvReader:
                jsonArray.append(row)

        with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
            jsonString = json.dumps(jsonArray, indent=4)
            jsonf.write(jsonString)
    except (Exception) as err:
        print(err)

if __name__ == '__main__':
    config = configparser.ConfigParser()    
    config.read(os.path.abspath('dwh_pipelines/config.ini'))

    CSVDATA_DIR = config['data_filepath']['CSVDATA']

    CSV_FILENAMES = [filename for filename in os.listdir(f'{os.getcwd()}{os.sep}{CSVDATA_DIR}')]

    for name in CSV_FILENAMES:
        csv_to_json(
            f"{os.getcwd()}{os.sep}{CSVDATA_DIR}{os.sep}{name} ",
            f"{os.getcwd()}{os.sep}{config['data_filepath']['JSONDATA']}{os.sep}{name}.json"
        )