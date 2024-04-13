import time 
import random
from pathlib import Path
from datetime import datetime
from commons.database import get_postgres_db_connection
from commons.logs import get_logger
from commons.utils import *


root_logger = None
if __name__=="__main__":
    current_filename = Path(__file__).stem

    root_logger = get_logger('layer1', current_filename)


root_logger.info("Start data extraction process...")
COMPUTE_START_TIME  =  time.time()

customer_info_data = load_json_file('Discount_Coupon.csv.json')

postgres_connection, cursor, database = get_postgres_db_connection()

def load_data_to_table(postgres_connection):
    try:
        db_layer_name = database
        schema_name = 'main'
        table_name = 'discount_coupon'
        source_system =   ['CRM', 'ERP', 'Database']
        row_counter = 0 
        total_null_values_in_table = 0 
        successful_rows_upload_count = 0
        failed_rows_upload_count = 0

        CURRENT_TIMESTAMP = datetime.now()

        # Set up SQL statements for schema creation and validation check  
        create_schema   =    f'''CREATE SCHEMA IF NOT EXISTS {schema_name};'''

        check_if_schema_exists  =   f'''SELECT schema_name from information_schema.schemata WHERE schema_name= '{schema_name}';'''

        delete_tbl_if_exists = f'''DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;'''

        check_if_tbl_is_deleted = f'''SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );'''

        create_tbl = f'''CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            Coupon_id SERIAL PRIMARY KEY,
            Month varchar(255),
            Product_Category varchar(255),
            Coupon_Code varchar(255),
            Discount_pct smallint
        );'''

        check_if_tbl_exists  =   f'''SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' );'''


        add_data_lineage_to_tbl  =   f''' ALTER TABLE {schema_name}.{table_name}
                                                                ADD COLUMN  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                ADD COLUMN  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                                                                ADD COLUMN  source VARCHAR(255);'''

        check_if_data_lineage_fields_are_added_to_tbl   =   f'''        
                                                                SELECT * 
                                                                FROM    information_schema.columns 
                                                                WHERE   table_name = '{table_name}' AND (column_name = 'created_at' OR column_name = 'updated_at');'''
        
        check_total_row_count_before_insert_statement   =   f'''SELECT COUNT(*) FROM {schema_name}.{table_name}'''


        insert_data  = f'''INSERT INTO {schema_name}.{table_name} (
            Month,
            Product_Category,
            Coupon_Code,
            Discount_pct,
            created_at,
            updated_at,
            source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);'''


        check_total_row_count_after_insert_statement    =   f'''SELECT COUNT(*) FROM {schema_name}.{table_name}'''


        
        count_total_no_of_columns_in_table  =   f'''            SELECT          COUNT(column_name) 
                                                                FROM            information_schema.columns 
                                                                WHERE           table_name      =   '{table_name}'
                                                                AND             table_schema    =   '{schema_name}'
        '''

        count_total_no_of_unique_records_in_table   =   f'''        SELECT COUNT(*) FROM 
                                                                            (SELECT DISTINCT * FROM {schema_name}.{table_name}) as unique_records   
        '''
        get_list_of_column_names    =   f'''                    SELECT      column_name
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



        # Add data lineage to table 
        cursor.execute(add_data_lineage_to_tbl)


        # sql_results = cursor.fetchall()
        
        # if len(sql_results) == 6:
        #     root_logger.debug(f"")
        #     root_logger.info(f"=============================================================================================================================================================================")
        #     root_logger.info(f"DATA LINEAGE FIELDS CREATION SUCCESS: Managed to create data lineage columns in {schema_name}.{table_name}.  ")
        #     root_logger.info(f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} ")
        #     root_logger.info(f"=============================================================================================================================================================================")
        #     root_logger.debug(f"")
        # else:
        #     root_logger.debug(f"")
        #     root_logger.error(f"==========================================================================================================================================================================")
        #     root_logger.error(f"DATA LINEAGE FIELDS CREATION FAILURE: Unable to create data lineage columns in {schema_name}.{table_name}.... ")
        #     root_logger.error(f"SQL Query for validation check:  {check_if_data_lineage_fields_are_added_to_tbl} ")
        #     root_logger.error(f"==========================================================================================================================================================================")
        #     root_logger.debug(f"")



        # Add insert rows to table 
        ROW_INSERTION_PROCESSING_START_TIME  =  time.time()

        cursor.execute(check_total_row_count_before_insert_statement)
        sql_result = cursor.fetchone()[0]
        root_logger.info(f"Rows before SQL insert in Postgres: {sql_result} ")
        root_logger.debug(f"")


        for datainfo in customer_info_data:
            values = (
                datainfo['Month'],
                datainfo['Product_Category'], 
                datainfo['Coupon_Code'], 
                datainfo['Discount_pct'],
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                source_system[random.randint(0, len(source_system)-1)]
            )

            cursor.execute(insert_data, values)


            # Validate if each row inserted into the table exists 
            if cursor.rowcount == 1:
                row_counter += 1
                successful_rows_upload_count += 1
                root_logger.debug(f'---------------------------------')
                root_logger.info(f'INSERT SUCCESS record no {row_counter} ')
                root_logger.debug(f'---------------------------------')
            else:
                row_counter += 1
                failed_rows_upload_count +=1
                root_logger.error(f'---------------------------------')
                root_logger.error(f'INSERT FAILED: Unable to insert datainfo record no {row_counter} ')
                root_logger.error(f'---------------------------------')

        cursor.execute(check_total_row_count_after_insert_statement)


        total_rows_in_table = cursor.fetchone()[0]
        root_logger.info(f"Rows after SQL insert in Postgres: {total_rows_in_table} ")
        root_logger.debug(f"")

        # ======================================= SENSITIVE COLUMN IDENTIFICATION ======================================= 

        # Add a flag for confirming if sensitive data fields have been highlighted  
        sensitive_columns_selected = ['Product_Category']

        SENSITIVE_COLUMNS_IDENTIFIED = True

        if len(sensitive_columns_selected) == 0:
            SENSITIVE_COLUMNS_IDENTIFIED = False
            root_logger.error(f"ERROR: No sensitive columns have been selected for '{table_name}' table ")
            root_logger.warning(f'')
        
        elif sensitive_columns_selected[0] is None:
            root_logger.error(f"There are no sensitive columns for the '{table_name}' table ")
            root_logger.warning(f'')

        if SENSITIVE_COLUMNS_IDENTIFIED:
            root_logger.info("Sensitve columns: " + str(sensitive_columns_selected))

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

        root_logger.info("Now saving changes made by SQL statements to Postgres DB....")
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

load_data_to_table(postgres_connection)