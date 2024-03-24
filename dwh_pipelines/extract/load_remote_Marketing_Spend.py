import os 
import json
import time 
import random
import psycopg2
import pandas as pd
import configparser
from pathlib import Path
import logging, coloredlogs
from datetime import datetime

FILENAME = "Marketing_Spend.sql.json"
# ================================================ LOGGER ================================================


# Set up root root_logger 
root_logger     =   logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)


# Set up formatter for logs 
file_handler_log_formatter      =   logging.Formatter('%(asctime)s  |  %(levelname)s  |  %(message)s  ')
console_handler_log_formatter   =   coloredlogs.ColoredFormatter(fmt    =   '%(message)s', level_styles=dict(
                                                                                                debug           =   dict    (color  =   'white'),
                                                                                                info            =   dict    (color  =   'green'),
                                                                                                warning         =   dict    (color  =   'cyan'),
                                                                                                error           =   dict    (color  =   'red',      bold    =   True,   bright      =   True),
                                                                                                critical        =   dict    (color  =   'black',    bold    =   True,   background  =   'red')
                                                                                            ),

                                                                                    field_styles=dict(
                                                                                        messages            =   dict    (color  =   'white')
                                                                                    )
                                                                                    )


# Set up file handler object for logging events to file
current_filepath    =   Path(__file__).stem
file_handler        =   logging.FileHandler('logs/' + current_filepath + '.log', mode='w')
file_handler.setFormatter(file_handler_log_formatter)


# Set up console handler object for writing event logs to console in real time (i.e. streams events to stderr)
console_handler     =   logging.StreamHandler()
console_handler.setFormatter(console_handler_log_formatter)


# Add the file handler 
root_logger.addHandler(file_handler)

# Only add the console handler if the script is running directly from this location 
if __name__=="__main__":
    root_logger.addHandler(console_handler)


# Create a config file for storing environment variables
config  =   configparser.ConfigParser()
# Use the local config file from the local machine 
path    =   os.path.abspath('dwh_pipelines/config.ini')
config.read(path)
JSONDATA     =   config['data_filepath']['JSONDATA']

host = config['data_filepath']['OLTP_HOST']
port =   config['data_filepath']['PORT']
database = config['data_filepath']['OLTP_DB']
username = config['data_filepath']['OLTP_USERNAME']
password = config['data_filepath']['PASSWORD']
postgres_connection     =   None
cursor                  =   None



# Begin the data extraction process
root_logger.info("")
root_logger.info("---------------------------------------------")
root_logger.info("Beginning the staging process...")


postgres_connection = psycopg2.connect(
                host        =   host,
                port        =   port,
                dbname      =   database,
                user        =   username,
                password    =   password,
        )
postgres_connection.set_session(autocommit=True)



def load_data_to_flight_schedules_table(postgres_connection):
    try:
        foreign_server                  =   config['data_filepath']['OLTP_HOST']
        active_schema_name              =   'main'
        active_db_name                  =    config['data_filepath']['OLTP_DB']
        src_table_name                  =   'marketing_spend'
        cursor      =   postgres_connection.cursor()



        # Validate the Postgres database connection
        if postgres_connection.closed == 0:
            root_logger.debug(f"")
            root_logger.info("=================================================================================")
            root_logger.info(f"CONNECTION SUCCESS: Managed to connect successfully to the {active_db_name} database!!")
            root_logger.info(f"Connection details: {postgres_connection.dsn} ")
            root_logger.info("=================================================================================")
            root_logger.debug("")
        
        elif postgres_connection.closed != 0:
            raise ConnectionError("CONNECTION ERROR: Unable to connect to the demo_company database...") 


        # ================================================== EXTRACT DATA FROM SOURCE POSTGRES TABLE ==================================================
            
        # Extract non-data lineage columns from raw table 

            # root_logger.info(f'{desired_sql_columns}')
        desired_sql_columns = ['dateh', 'offs', 'ons']


        # Pull flight_schedules_tbl data from staging tables in Postgres database 
        try:
            fetch_flight_schedules_tbl = f'''SELECT { ', '.join(desired_sql_columns) } FROM {active_schema_name}.{src_table_name};  
            '''
            root_logger.debug(fetch_flight_schedules_tbl)
            root_logger.info("")
            root_logger.info(f"Successfully IMPORTED the '{src_table_name}' virtual table from the '{foreign_server}' server into the '{active_schema_name}' schema for '{database}' database. Now advancing to data cleaning stage...")
            root_logger.info("")


            # Execute SQL command to interact with Postgres database
            cursor.execute(fetch_flight_schedules_tbl)

            # Extract header names from cursor's description
            postgres_table_headers = [header[0] for header in cursor.description]


            # Execute script 
            postgres_table_results = cursor.fetchall()
            

            # Use Postgres results to create data frame for flight_schedules_tbl
            flight_schedules_tbl_df = pd.DataFrame(data=postgres_table_results, columns=postgres_table_headers)


            # Create temporary data frame     
            temp_df = flight_schedules_tbl_df

        except Exception as e:
            print(e)

        print(temp_df)
        print(temp_df.columns)
        
        # Write results to temp file for data validation checks 
        with open(f'{JSONDATA}{os.sep}{FILENAME}', 'w') as temp_results_file:
            temp_results_file_df_to_json = temp_df.to_json(orient="records")
            temp_results_file.write(json.dumps(json.loads(temp_results_file_df_to_json), indent=4, sort_keys=True)) 

    except Exception as e:
            root_logger.info(e)
        
    finally:
        
        # Close the cursor if it exists 
        if cursor is not None:
            cursor.close()
            root_logger.debug("")
            root_logger.debug("Cursor closed successfully.")

        # Close the database connection to Postgres if it exists 
        if postgres_connection is not None:
            postgres_connection.close()
            # root_logger.debug("")
            root_logger.debug("Session connected to Postgres database closed.")



load_data_to_flight_schedules_table(postgres_connection)