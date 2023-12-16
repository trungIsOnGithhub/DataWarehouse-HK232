import pandas

import os
import configparser

XLSX_FILE = [
    { "name": "CustomersData", "sheet": "Customers" },
    { "name": "Tax_amount", "sheet": "GSTDetails" }
]

def xlsx_to_json(xlsxFilePath, csvFilePath, file):
    try:
        pandas.read_excel(xlsxFilePath, sheet_name=file["sheet"]).to_csv(
            csvFilePath,
            index = None, header=True
        )
    except (Exception) as err:
        print( "\033[91m {}\033[00m".format("ERROR while Convert from Excel to CSV success, file: " + file["name"] + " sheet: " + file["sheet"]) )
        print(err)

config = configparser.ConfigParser()    
config.read(os.path.abspath('dwh_pipelines/local_config.ini'))

for file in XLSX_FILE:
    xlsx_to_json(
        f"{os.getcwd()}{os.sep}{config['data_filepath']['XLSXDATA']}{os.sep}{file['name']}.xlsx",
        f"{os.getcwd()}{os.sep}{config['data_filepath']['CSVDATA']}{os.sep}{file['name']}.csv",
        file
    )
    print(  "\033[92m {}\033[00m".format("SUCCESS Convert from Excel to CSV, file: " + file["name"] + " sheet: " + file["sheet"]) )