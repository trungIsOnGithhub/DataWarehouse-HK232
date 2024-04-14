import time

def measure_processing_time(start_time, end_time, target, logger):
    execution_time = (end_time - start_time) * 1000

    if (execution_time > 1000):
        logger.info(f'2. Execution time for {target}: {execution_time} ms ({ round(execution_time/1000, 2) } secs)')
        logger.info(f'')
    else:
        logger.info(f'2. Execution time for {target}: {execution_time} ms ')
        logger.info(f'')


def exec_query_and_check(cursor, exec_query, check_query, logger):
    EXECUTION_START_TIME = time.time()
    cursor.execute(exec_query)
    EXECUTION_END_TIME = time.time()

    CHECK_START_TIME = time.time()
    cursor.execute(check_query)
    CHECK_END_TIME = time.time()

    sql_result = cursor.fetchone()[0]
    if sql_result:
        logger.info(f"=================================================================================================")
        logger.info(f"EXECUTE SUCCESS {exec_query} schema")
        logger.info(f"=================================================================================================")
        logger.info(f"RESULT: {sql_result} ")


    else:
        logger.debug(f"")
        logger.error(f"=================================================================================================")
        logger.error(f"EXECUTE FAILED: {exec_query}")
        logger.error(f"CHECK QUERY: {check_query}")
        logger.error(f"=================================================================================================")
    
    logger.debug(f"")
    logger.debug(f"")

    measure_processing_time(EXECUTION_START_TIME, EXECUTION_END_TIME, 'Query Exec', logger)
    measure_processing_time(CHECK_START_TIME, CHECK_END_TIME, 'Query Check', logger)


def table_profiling_metrics(cursor, count_column_query, count_unique_records_query, get_column_names_query, total_rows_query, successful_rows_count, failed_rows_count, logger):
    total_rows_query = f'''SELECT COUNT(*) FROM {schema_name}.{table_name}'''
    cursor.execute(total_rows_query)
    total_rows_in_table = cursor.fetchone()[0]


    cursor.execute(count_column_query)
    total_columns_in_table = cursor.fetchone()[0]

    cursor.execute(count_unique_records_query)
    total_unique_records_in_table = cursor.fetchone()[0]
    total_duplicate_records_in_table = total_rows_in_table - total_unique_records_in_table


    cursor.execute(get_column_names_query)
    list_of_column_names = cursor.fetchall()
    column_names = [sql_result[0] for sql_result in list_of_column_names]

    if successful_rows_count != total_rows_in_table:
        logger.error(f"ERROR: There are only {successful_rows_count} records upload to table....")
        raise Exception('successful_rows_count != total_rows_in_table')
    elif failed_rows_count > 0:
        logger.error(f"ERROR: A total of {failed_rows_count} records failed to upload to table....")
        raise Exception()
    elif total_unique_records_in_table != total_rows_in_table:
        logger.error(f"ERROR: There are {total_duplicate_records_in_table} duplicated records in the uploads for table....")
        raise Exception("total_unique_records_in_table != total_rows_in_table")
    elif total_duplicate_records_in_table > 0:
        logger.error(f"ERROR: There are {total_duplicate_records_in_table} duplicated records in the uploads for table....")
        raise Exception("total_duplicate_records_in_table > 0")


    if successful_rows_count == total_rows_in_table:
        logger.info(f'-----------------------------------------------------------------------------------------')
        logger.info(f'Successful records uploaded total: {successful_rows_count} / {total_rows_in_table}   ')
        logger.info(f'Failed/Errored records uploaded total: {failed_rows_count} / {total_rows_in_table}       ')
        logger.info(f'Successful records uploaded %: {(successful_rows_count / total_rows_in_table) * 100}    ')
        logger.info(f'Failed/Errored records uploaded %: {(failed_rows_count/total_rows_in_table) * 100}       ')
        logger.info(f'-----------------------------------------------------------------------------------------')
    else:
        logger.warning(f'-----------------------------------------------------------------------------------------')
        logger.warning(f'Successful records uploaded total: {successful_rows_count} / {total_rows_in_table}   ')
        logger.warning(f'Failed/Errored records uploaded total: {failed_rows_count} / {total_rows_in_table}       ')
        logger.warning(f'Successful records uploaded %: {(successful_rows_count / total_rows_in_table) * 100}    ')
        logger.warning(f'Failed/Errored records uploaded %: {(failed_rows_count/total_rows_in_table) * 100}       ')
        logger.warning(f'-----------------------------------------------------------------------------------------')


    if total_unique_records_in_table == total_rows_in_table:
        logger.info(f'-----------------------------------------------------------------------------------------')
        logger.info(f'Number of unique records: {total_unique_records_in_table} / {total_rows_in_table}')
        logger.info(f'Number of duplicate records: {total_duplicate_records_in_table} / {total_rows_in_table}')
        logger.info(f'Unique records %: {(total_unique_records_in_table / total_rows_in_table) * 100} ')
        logger.info(f'Duplicate records %: {(total_duplicate_records_in_table / total_rows_in_table)  * 100} ')
        logger.info(f'-----------------------------------------------------------------------------------------')
    
    else:
        logger.warning(f'-----------------------------------------------------------------------------------------')
        logger.warning(f'Number of unique records: {total_unique_records_in_table} / {total_rows_in_table}')
        logger.warning(f'Number of duplicate records: {total_duplicate_records_in_table} / {total_rows_in_table}')
        logger.warning(f'Unique records %: {(total_unique_records_in_table / total_rows_in_table) * 100} ')
        logger.warning(f'Duplicate records %: {(total_duplicate_records_in_table / total_rows_in_table)  * 100} ')
        logger.warning(f'-----------------------------------------------------------------------------------------')

    return column_names