import os 
import json
import time 
import random
import psycopg2
import configparser
from pathlib import Path
import logging, coloredlogs
from datetime import datetime

with open(f"{os.getcwd()}{os.sep}dwh_pipelines{os.sep}extract{os.sep}load_remote_Marketing_Spend.py") as start_fetch:
    exec(start_fetch.read())

src_file = 'Marketing_Spend.sql.json'

# ================================================ LOGGER ================================================
root_logger     =   logging.getLogger(__name__)
root_logger.setLevel(logging.DEBUG)
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


# Add the file and console handlers 
root_logger.addHandler(file_handler)


# Only add the console handler if the script is running directly from this location 
if __name__=="__main__":
    root_logger.addHandler(console_handler)


# ================================================ CONFIG ================================================


# Create a config file for storing environment variables
config  =   configparser.ConfigParser()

path    =   os.path.abspath('dwh_pipelines/config.ini')
config.read(path)
customer_info_path     =   config['data_filepath']['JSONDATA'] + os.sep + src_file

host = config['data_filepath']['HOST']
port = config['data_filepath']['PORT']
database = config['data_filepath']['DWH_DB']
username = config['data_filepath']['USERNAME']
password = config['data_filepath']['PASSWORD']
postgres_connection = None
cursor = None

root_logger.info("Beginning the source data extraction process...")


# with open(customer_info_path, 'r') as customer_info_file:    
#     try:
#         customer_info_data = json.load(customer_info_file)
#         root_logger.info(f"Successfully located '{src_file}'")
#         root_logger.info(f"File type: '{type(customer_info_data)}'")

#     except:
#         root_logger.error("Unable to locate source file...")
#         raise Exception("No source file located")
    

postgres_connection = psycopg2.connect(
host = host,
port = port,
dbname = database,
user = username,
password = password,
)
postgres_connection.set_session(autocommit=True)

def load_data_to_table(postgres_connection):
    try:
        db_layer_name =   database

        schema_name = 'datamart'
        src_schema_name = 'main'

        dim_table1 = 'customers_data'
        join_col_tbl1 = 'customerid'

        dim_table2 = 'online_sales'
        join_col_tbl2 = 'customerid'

        table_name = 'customers_sales'
        total_null_values_in_table      =   0 
        successful_rows_upload_count    =   0 
        failed_rows_upload_count        =   0

        cursor      =   postgres_connection.cursor()

        list_of_column_selecteds = [f"{dim_table1}.customerid",'gender', 'location', 'product_category', 'transaction_date']


        if postgres_connection.closed == 0:
            root_logger.debug(f"")
            root_logger.info("=================================================================================")
            root_logger.info(f"CONNECTION SUCCESS: Managed to connect successfully to the {db_layer_name} database!!")
            root_logger.info(f"Connection details: {postgres_connection.dsn} ")
            root_logger.info("=================================================================================")
            root_logger.debug("")
        elif postgres_connection.closed != 0:
            raise ConnectionError("CONNECTION ERROR: Unable to connect to the demo_company database...") 




        # ======================================= LOAD SRC TO RAW =======================================
        

        # Set up SQL statements for schema creation and validation check  
        create_schema  = f'''CREATE SCHEMA IF NOT EXISTS {schema_name};'''

        check_if_schema_exists  =   f'''SELECT schema_name from information_schema.schemata WHERE schema_name= '{schema_name}';'''

        delete_tbl_if_exists = f'''DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;'''

        check_if_tbl_is_deleted = f'''SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );'''

        create_tbl = f'''CREATE TABLE {schema_name}.{table_name} AS SELECT {",".join(list_of_column_selecteds)} FROM {src_schema_name}.{dim_table1} JOIN {src_schema_name}.{dim_table2} ON {src_schema_name}.{dim_table1}.{join_col_tbl1} = {src_schema_name}.{dim_table2}.{join_col_tbl2};'''

        # print(create_tbl)

        check_if_tbl_exists = f'''SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );'''

        # insert_data  = f'''INSERT INTO {schema_name}.{table_name} (
        #     Date,
        #     Offline_Spend,
        #     Online_Spend,
        #     created_at,
        #     updated_at,
        #     source
        # )
        # VALUES (to_timestamp(%s/1000)::date, %s, %s, %s, %s, %s);'''

        check_total_row_count_after_insert_statement    =   f'''SELECT COUNT(*) FROM {schema_name}.{table_name}'''
        
        count_total_no_of_columns_in_table  =   f'''            SELECT          COUNT(column_name) 
                                                                FROM            information_schema.columns 
                                                                WHERE           table_name      =   '{table_name}'
                                                                AND             table_schema    =   '{schema_name}'
        '''

        count_total_no_of_unique_records_in_table   =   f'''        SELECT COUNT(*) FROM 
                                                                            (SELECT DISTINCT * FROM {schema_name}.{table_name}) as unique_records   
        '''
        get_list_of_column_names    =   f'''SELECT      column_name
                                                                FROM        information_schema.columns  
                                                                WHERE       table_name   =  '{table_name}'
                                                                ORDER BY    ordinal_position 

        '''

        # Create schema in Postgres
        cursor.execute(create_schema)

        cursor.execute(check_if_schema_exists)

        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=================================================================================================")
            root_logger.info(f"SCHEMA CREATION SUCCESS: Managed to create {schema_name} schema in {db_layer_name} ")
            root_logger.info(f"Schema name in Postgres: {sql_result} ")
            root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
            root_logger.info(f"=================================================================================================")
            root_logger.debug(f"")

        else:
            root_logger.debug(f"")
            root_logger.error(f"=================================================================================================")
            root_logger.error(f"SCHEMA CREATION FAILURE: Unable to create schema for {db_layer_name}...")
            root_logger.info(f"SQL Query for validation check:  {check_if_schema_exists} ")
            root_logger.error(f"=================================================================================================")
            root_logger.debug(f"")

        

        # Delete table if it exists in Postgres
        cursor.execute(delete_tbl_if_exists)
        cursor.execute(check_if_tbl_is_deleted)

        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE DELETION SUCCESS: Managed to drop {table_name} table in {db_layer_name}. Now advancing to recreating table... ")
            root_logger.info(f"SQL Query for validation check:  {check_if_tbl_is_deleted} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE DELETION FAILURE: Unable to delete {table_name}. This table may have objects that depend on it (use DROP TABLE ... CASCADE to resolve) or it doesn't exist. ")
            root_logger.error(f"SQL Query for validation check:  {check_if_tbl_is_deleted} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")

        CREATE_TABLE_START_TIME   =   time.time()
        # Create table if it doesn't exist in Postgres  
        cursor.execute(create_tbl)
        cursor.execute(check_if_tbl_exists)

        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE CREATION SUCCESS: Managed to create {table_name} table in {db_layer_name}.  ")
            root_logger.info(f"SQL Query for validation check:  {check_if_tbl_exists} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE CREATION FAILURE: Unable to create {table_name}... ")
            root_logger.error(f"SQL Query for validation check:  {check_if_tbl_exists} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")
        CREATE_TABLE_END_TIME   =   time.time()

        cursor.execute(check_total_row_count_after_insert_statement)

        total_rows_in_table = cursor.fetchone()[0]
        root_logger.debug(f"")
        root_logger.info(f"Rows after SQL insert in Postgres: {total_rows_in_table} ")
        root_logger.info(f"Total time Table Creation: {CREATE_TABLE_END_TIME - CREATE_TABLE_START_TIME} ")
        root_logger.debug(f"")


        # Prepare data profiling metrics 


        # --------- A. Table statistics 
        cursor.execute(count_total_no_of_columns_in_table)
        total_columns_in_table = cursor.fetchone()[0]

        cursor.execute(count_total_no_of_unique_records_in_table)
        total_unique_records_in_table = cursor.fetchone()[0]
        total_duplicate_records_in_table = total_rows_in_table - total_unique_records_in_table


        cursor.execute(get_list_of_column_names)
        list_of_column_names = cursor.fetchall()
        column_names = [sql_result[0] for sql_result in list_of_column_names]   


        # Display data profiling metrics
        
        # delete some null and validation profiling

        # delete timming on excutuion

        # Add conditional statements for data profile metrics
        root_logger.info('================================================')

        root_logger.info(f"Total columns in table after insertion: {total_columns_in_table}")

        root_logger.info(f"Columns name list in table after insertion: {str(column_names)}")

        if successful_rows_upload_count != total_rows_in_table:
            if successful_rows_upload_count == 0:
                root_logger.error(f"ERROR: No records were upload to '{table_name}' table....")
                raise ImportError("Trace filepath to highlight the root cause of the missing rows...")
            else:
                root_logger.error(f"ERROR: There are only {successful_rows_upload_count} records upload to '{table_name}' table....")
                raise ImportError("Trace filepath to highlight the root cause of the missing rows...")
        

        elif failed_rows_upload_count > 0:
            root_logger.error(f"ERROR: A total of {failed_rows_upload_count} records failed to upload to '{table_name}' table....")
            raise ImportError("Trace filepath to highlight the root cause of the missing rows...")
        

        elif total_unique_records_in_table != total_rows_in_table:
            root_logger.error(f"ERROR: There are {total_duplicate_records_in_table} duplicated records in the uploads for '{table_name}' table....")
            raise ImportError("Trace filepath to highlight the root cause of the duplicated rows...")


        elif total_duplicate_records_in_table > 0:
            root_logger.error(f"ERROR: There are {total_duplicate_records_in_table} duplicated records in the uploads for '{table_name}' table....")
            raise ImportError("Trace filepath to highlight the root cause of the duplicated rows...")
        

        elif total_null_values_in_table > 0:
            root_logger.error(f"ERROR: There are {total_duplicate_records_in_table} NULL values in '{table_name}' table....")
            raise ImportError("Examine table to highlight the columns with the NULL values - justify if these fields should contain NULLs ...")
        else:
            root_logger.debug("")
            root_logger.info("DATA VALIDATION SUCCESS: All general DQ checks passed! ")
            root_logger.debug("")

        
        root_logger.info("End Of Loading Data")
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

load_data_to_table(postgres_connection)