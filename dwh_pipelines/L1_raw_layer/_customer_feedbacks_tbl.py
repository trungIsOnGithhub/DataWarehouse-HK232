
import os 
import json
import time 
import random
import psycopg2
import configparser
from pathlib import Path
import logging, coloredlogs
from datetime import datetime

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
file_handler        =   logging.FileHandler('logs/L1_raw_layer/' + current_filepath + '.log', mode='w')
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

src_file    =   'customer_feedbacks.json'


# Create a config file for storing environment variables
config  =   configparser.ConfigParser()
# Use the local config file from the local machine 
path    =   os.path.abspath('dwh_pipelines/local_config.ini')
config.read(path)
customer_feedbacks_path     =   config['data_filepath']['JSONDATA'] + os.sep + src_file

host                    =   config['data_filepath']['HOST']
port                    =   config['data_filepath']['PORT']
database                =   config['data_filepath']['RAW_DB']
username                =   config['data_filepath']['USERNAME']
password                =   config['data_filepath']['PASSWORD']

postgres_connection     =   None
cursor                  =   None


root_logger.info("---------------------------------------------")
root_logger.info("Beginning the source data extraction process...")
COMPUTE_START_TIME  =   time.time()


with open(customer_feedbacks_path, 'r') as customer_feedbacks_file:    
    
    try:
        customer_feedbacks_data = json.load(customer_feedbacks_file)
        # customer_feedbacks_data = customer_feedbacks_data[0:100]
        root_logger.info(f"Successfully located '{src_file}'")
        root_logger.info(f"File type: '{type(customer_feedbacks_data)}'")

    except:
        root_logger.error("Unable to locate source file...terminating process...")
        raise Exception("No source file located")
    

postgres_connection = psycopg2.connect(
                host        =   host,
                port        =   port,
                dbname      =   database,
                user        =   username,
                password    =   password,
        )
postgres_connection.set_session(autocommit=True)

def load_customer_feedbacks_data_to_raw_table(postgres_connection):
    try:
        
        # Set up constants
        CURRENT_TIMESTAMP               =   datetime.now()
        db_layer_name                   =   database
        schema_name                     =   'main'
        table_name                      =   'raw_customer_feedbacks_tbl'
        data_warehouse_layer            =   'RAW'
        source_system                   =   ['CRM', 'ERP', 'Mobile App', 'Website', '3rd party apps', 'Company database']
        row_counter                     =   0 
        column_index                    =   0 
        total_null_values_in_table      =   0 
        successful_rows_upload_count    =   0 
        failed_rows_upload_count        =   0 


        # Create a cursor object to execute the PG-SQL commands 
        cursor      =   postgres_connection.cursor()



        # Validate the Postgres database connection
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
        create_schema   =    f'''    CREATE SCHEMA IF NOT EXISTS {schema_name};
        '''

        check_if_schema_exists  =   f'''   SELECT schema_name from information_schema.schemata WHERE schema_name= '{schema_name}';
        '''


        # Set up SQL statements for table deletion and validation check  
        delete_raw_customer_feedbacks_tbl_if_exists     =   f''' DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;
        '''

        check_if_raw_customer_feedbacks_tbl_is_deleted  =   f'''   SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
        '''

        # Set up SQL statements for table creation and validation check 
        create_raw_customer_feedbacks_tbl = f'''                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                                                                                feedback_id         UUID         PRIMARY KEY,
                                                                                customer_id         UUID         NOT NULL,
                                                                                flight_booking_id   UUID         NOT NULL,
                                                                                feedback_date       BIGINT       NOT NULL,
                                                                                feedback_text       VARCHAR(255) NOT NULL
                                                                        );



        '''

        check_if_raw_customer_feedbacks_tbl_exists  =   f'''       SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );
        '''

       



        # Set up SQL statements for adding data lineage and validation check 
        add_data_lineage_to_raw_customer_feedbacks_tbl  =   f'''        ALTER TABLE {schema_name}.{table_name}
                                                                                ADD COLUMN  created_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                                ADD COLUMN  updated_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                                ADD COLUMN  source_system               VARCHAR(255),
                                                                                ADD COLUMN  source_file                 VARCHAR(255),
                                                                                ADD COLUMN  load_timestamp              TIMESTAMP,
                                                                                ADD COLUMN  dwh_layer                   VARCHAR(255)
                                                                        ;
        '''

        check_if_data_lineage_fields_are_added_to_tbl   =   f'''        
                                                                    SELECT * 
                                                                    FROM    information_schema.columns 
                                                                    WHERE   table_name      = '{table_name}' 
                                                                        AND     (column_name    = 'created_at'
                                                                        OR      column_name     = 'updated_at' 
                                                                        OR      column_name     = 'source_system' 
                                                                        OR      column_name     = 'source_file' 
                                                                        OR      column_name     = 'load_timestamp' 
                                                                        OR      column_name     = 'dwh_layer');
                                                                              
        '''
        
        check_total_row_count_before_insert_statement   =   f'''SELECT COUNT(*) FROM {schema_name}.{table_name}'''

        # Set up SQL statements for records insert and validation check
        insert_customer_feedbacks_data  =   f'''                       INSERT INTO {schema_name}.{table_name} (
                                                                                feedback_id, 
                                                                                customer_id, 
                                                                                flight_booking_id, 
                                                                                feedback_date, 
                                                                                feedback_text, 
                                                                                created_at, 
                                                                                updated_at, 
                                                                                source_system, 
                                                                                source_file, 
                                                                                load_timestamp, 
                                                                                dwh_layer

                                                                            )
                                                                            VALUES (
                                                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                                                    
                                                                            );
        '''

        check_total_row_count_after_insert_statement    =   f'''        SELECT COUNT(*) FROM {schema_name}.{table_name}
        '''


        
        count_total_no_of_columns_in_table  =   f'''            SELECT          COUNT(column_name) 
                                                                FROM            information_schema.columns 
                                                                WHERE           table_name      =   '{table_name}'
                                                                AND             table_schema    =   '{schema_name}'
        '''

        count_total_no_of_unique_records_in_table   =   f'''        SELECT COUNT(*) FROM 
                                                                            (SELECT DISTINCT * FROM {schema_name}.{table_name}) as unique_records   
        '''
        get_list_of_column_names    =   f'''                SELECT column_name FROM information_schema.columns 
                                                            WHERE   table_name = '{table_name}'
                                                            ORDER BY ordinal_position 
        '''

        




        # Create schema in Postgres
        CREATING_SCHEMA_PROCESSING_START_TIME   =   time.time()
        cursor.execute(create_schema)
        CREATING_SCHEMA_PROCESSING_END_TIME     =   time.time()


        CREATING_SCHEMA_VAL_CHECK_START_TIME    =   time.time()
        cursor.execute(check_if_schema_exists)
        CREATING_SCHEMA_VAL_CHECK_END_TIME      =   time.time()



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
        DELETING_SCHEMA_PROCESSING_START_TIME   =   time.time()
        cursor.execute(delete_raw_customer_feedbacks_tbl_if_exists)
        DELETING_SCHEMA_PROCESSING_END_TIME     =   time.time()

        
        DELETING_SCHEMA_VAL_CHECK_PROCESSING_START_TIME     =   time.time()
        cursor.execute(check_if_raw_customer_feedbacks_tbl_is_deleted)
        DELETING_SCHEMA_VAL_CHECK_PROCESSING_END_TIME       =   time.time()


        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE DELETION SUCCESS: Managed to drop {table_name} table in {db_layer_name}. Now advancing to recreating table... ")
            root_logger.info(f"SQL Query for validation check:  {check_if_raw_customer_feedbacks_tbl_is_deleted} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE DELETION FAILURE: Unable to delete {table_name}. This table may have objects that depend on it (use DROP TABLE ... CASCADE to resolve) or it doesn't exist. ")
            root_logger.error(f"SQL Query for validation check:  {check_if_raw_customer_feedbacks_tbl_is_deleted} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Create table if it doesn't exist in Postgres  
        CREATING_TABLE_PROCESSING_START_TIME    =   time.time()
        cursor.execute(create_raw_customer_feedbacks_tbl)
        CREATING_TABLE_PROCESSING_END_TIME  =   time.time()

        
        CREATING_TABLE_VAL_CHECK_PROCESSING_START_TIME  =   time.time()
        cursor.execute(check_if_raw_customer_feedbacks_tbl_exists)
        CREATING_TABLE_VAL_CHECK_PROCESSING_END_TIME    =   time.time()


        sql_result = cursor.fetchone()[0]
        if sql_result:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"TABLE CREATION SUCCESS: Managed to create {table_name} table in {db_layer_name}.  ")
            root_logger.info(f"SQL Query for validation check:  {check_if_raw_customer_feedbacks_tbl_exists} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"TABLE CREATION FAILURE: Unable to create {table_name}... ")
            root_logger.error(f"SQL Query for validation check:  {check_if_raw_customer_feedbacks_tbl_exists} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Add data lineage to table 
        ADDING_DATA_LINEAGE_PROCESSING_START_TIME   =   time.time()
        cursor.execute(add_data_lineage_to_raw_customer_feedbacks_tbl)
        ADDING_DATA_LINEAGE_PROCESSING_END_TIME     =   time.time()

        
        ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_START_TIME  =  time.time()
        cursor.execute(check_if_data_lineage_fields_are_added_to_tbl)
        ADDING_DATA_LINEAGE_VAL_CHECK_PROCESSING_END_TIME    =  time.time()


        sql_results = cursor.fetchall()
        
        if len(sql_results) == 6:
            root_logger.debug(f"")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.info(f"DATA LINEAGE FIELDS CREATION SUCCESS: Managed to create data lineage columns in {schema_name}.{table_name}.  ")
            root_logger.info(f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} ")
            root_logger.info(f"=============================================================================================================================================================================")
            root_logger.debug(f"")
        else:
            root_logger.debug(f"")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.error(f"DATA LINEAGE FIELDS CREATION FAILURE: Unable to create data lineage columns in {schema_name}.{table_name}.... ")
            root_logger.error(f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} ")
            root_logger.error(f"==========================================================================================================================================================================")
            root_logger.debug(f"")



        # Add insert rows to table 
        ROW_INSERTION_PROCESSING_START_TIME     =   time.time()
        cursor.execute(check_total_row_count_before_insert_statement)
        sql_result = cursor.fetchone()[0]
        root_logger.info(f"Rows before SQL insert in Postgres: {sql_result} ")
        root_logger.debug(f"")


        for customer_feedbacks in customer_feedbacks_data:
            values = (
                customer_feedbacks['feedback_id'], 
                customer_feedbacks['customer_id'], 
                customer_feedbacks['flight_booking_id'], 
                customer_feedbacks['feedback_date'], 
                customer_feedbacks['feedback_text'],
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                random.choice(source_system),
                src_file,
                CURRENT_TIMESTAMP,
                'RAW'
                )

            cursor.execute(insert_customer_feedbacks_data, values)


            # Validate if each row inserted into the table exists 
            if cursor.rowcount == 1:
                row_counter += 1
                successful_rows_upload_count += 1
                root_logger.debug(f'---------------------------------')
                root_logger.info(f'INSERT SUCCESS: Uploaded customer_feedbacks record no {row_counter} ')
                root_logger.debug(f'---------------------------------')
            else:
                row_counter += 1
                failed_rows_upload_count +=1
                root_logger.error(f'---------------------------------')
                root_logger.error(f'INSERT FAILED: Unable to insert customer_feedbacks record no {row_counter} ')
                root_logger.error(f'---------------------------------')


        cursor.execute(check_total_row_count_after_insert_statement)


        total_rows_in_table = cursor.fetchone()[0]
        root_logger.info(f"Rows after SQL insert in Postgres: {total_rows_in_table} ")
        root_logger.debug(f"")



        # ======================================= SENSITIVE COLUMN IDENTIFICATION =======================================

        # note_1 = """IMPORTANT NOTE: Invest time in understanding the underlying data fields to avoid highlighting the incorrect fields or omitting fields containing confidential information.          """
        # note_2 = """      Involving the relevant stakeholders in the process of identifying sensitive data fields from the source data is a crucial step to protecting confidential information. """
        # note_3 = """      Neglecting this step could expose customers and the wider company to serious harm (e.g. cybersecurity hacks, data breaches, unauthorized access to sensitive data), so approach this task with the utmost care. """
        
        # root_logger.warning(f'')
        # root_logger.warning(f'')
        # root_logger.warning('================================================')
        # root_logger.warning('           SENSITIVE COLUMN IDENTIFICATION              ')
        # root_logger.warning('================================================')
        # root_logger.warning(f'')
        # root_logger.error(f'{note_1}')
        # root_logger.error(f'')
        # root_logger.error(f'{note_2}')
        # root_logger.error(f'')
        # root_logger.error(f'{note_3}')
        # root_logger.warning(f'')
        # root_logger.warning(f'')
        # root_logger.warning(f'Now beginning the sensitive column identification stage ...')
        # root_logger.warning(f'')
        

        # Add a flag for confirming if sensitive data fields have been highlighted  
        sensitive_columns_selected = ['customer_id', 'flight_booking_id', 'feedback_date', 'feedback_text'
                            ]
        
        

        if len(sensitive_columns_selected) == 0:
            SENSITIVE_COLUMNS_IDENTIFIED = False
            root_logger.error(f"ERROR: No sensitive columns have been selected for '{table_name}' table ")
            root_logger.warning(f'')
        
        elif sensitive_columns_selected[0] is None:
            SENSITIVE_COLUMNS_IDENTIFIED = True
            root_logger.error(f"There are no sensitive columns for the '{table_name}' table ")
            root_logger.warning(f'')

        else:
            SENSITIVE_COLUMNS_IDENTIFIED = True
            root_logger.warning(f'Here are the columns considered sensitive in this table ...')
            root_logger.warning(f'')

        
        if SENSITIVE_COLUMNS_IDENTIFIED is False:
            sql_statement_for_listing_columns_in_table = f"""        
            SELECT column_name FROM information_schema.columns 
            WHERE   table_name = '{table_name}'
            ORDER BY ordinal_position 
            """
            cursor.execute(get_list_of_column_names)
            list_of_column_names = cursor.fetchall()
            column_names = [sql_result[0] for sql_result in list_of_column_names]
            
            root_logger.warning(f"You are required to select the sensitive columns in this table. If there are none, enter 'None' in the 'sensitive_columns_selected' object.")
            root_logger.warning(f'')
            root_logger.warning(f"Here are the columns to choose from:")
            root_logger.warning(f'')
            total_sensitive_columns = 0
            for sensitive_column_name in column_names:
                total_sensitive_columns += 1
                root_logger.warning(f'''{total_sensitive_columns} : '{sensitive_column_name}'  ''')



            root_logger.warning(f'')
            root_logger.warning(f'You can use this SQL query to list the columns in this table:')
            root_logger.warning(f'              {sql_statement_for_listing_columns_in_table}                ')
        
        else:
            total_sensitive_columns = 0
            for sensitive_column_name in sensitive_columns_selected:
                total_sensitive_columns += 1
                root_logger.warning(f'''{total_sensitive_columns} : '{sensitive_column_name}'  ''')
            if sensitive_columns_selected[0] is not None:
                root_logger.warning(f'')
                root_logger.warning(f'')
                root_logger.warning(f'Decide on the appropriate treatment for these tables. A few options to consider include:')
                root_logger.warning(f'''1. Masking fields               -   This involves replacing sensitive columns with alternative characters e.g.  'xxxx-xxxx', '*****', '$$$$'. ''')
                root_logger.warning(f'''2. Encrypting fields            -   This is converting sensitive columns to cipher text (unreadable text format).        ''')
                root_logger.warning(f'''3. Role-based access control    -   Placing a system that delegates privileges based on team members' responsibilities        ''')
            
            root_logger.warning(f'')
            root_logger.warning(f'Now terminating the sensitive column identification stage ...')
            root_logger.warning(f'Sensitive column identification stage ended. ')
            root_logger.warning(f'')


        root_logger.warning(f'')
        root_logger.warning(f'')





        # ======================================= DATA PROFILING METRICS =======================================


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
        root_logger.info('================================================')
        root_logger.info(f'')
        root_logger.info(f'Now calculating performance statistics (from a Python standpoint)...')
        root_logger.info(f'')
        root_logger.info(f'')


# delete some timming

        

# delete timming on excutuion



# delete more timming on excutuion


        # Add conditional statements for data profile metrics 

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





        # Commit the changes made in Postgres 
        root_logger.info("Now saving changes made by SQL statements to Postgres DB....")
        # postgres_connection.commit()
        root_logger.info("Saved successfully, now terminating cursor and current session....")


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



load_customer_feedbacks_data_to_raw_table(postgres_connection)

