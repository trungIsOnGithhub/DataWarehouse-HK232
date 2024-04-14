from pathlib import Path
from datetime import datetime
from commons.database import get_postgres_db_connection
from commons.logs import get_logger
from commons.utils import *

root_logger = None
if __name__=="__main__":
    current_filename = Path(__file__).stem

    root_logger = get_logger('layer1', current_filename)


def set_up_access_controls():
    root_logger.info("Start data extraction process...")
    postgres_connection, cursor, database = get_postgres_db_connection()

    current_schema = 'live'

    def create_index_sql_query(schema, index_name, table, column):
        return f"CREATE INDEX {index_name} ON {schema}.{table}({column});"
    def check_index_exist_sql_query(table, index):
        return f"SELECT * FROM pg_indexes WHERE tablename = '{table}' AND indexname = {index}"
    def check_index_exist(table, index):
        cursor.execute(sql_query_2)
        query_result = cursor.fetchall();
        return not (len(query_result) == 0)

    sql_query_1 = create_index_sql_query(current_schema, 'idx_customer_location', 'customer_data' ,'location')
    sql_query_2 = create_index_sql_query(current_schema, 'idx_customer_online_sales', 'online_sales' ,'customerid')

    # Validate the Postgres database connection
    if postgres_connection.closed != 0:
        raise ConnectionError("CONNECTION ERROR: Unable to connect to the demo_company database...") 

    root_logger.info(f'=========================================== CREATING INDEXES =======================================')
    root_logger.info(f'')
    root_logger.info(f'')

    if not check_index_exist('idx_customer_location', 'customer_data'):
        cursor.execute(sql_query_1)
        root_logger.info(f"Successfully created index: '{sql_query_1}'")
        root_logger.info(f'-------------------------------------------------------------')
        root_logger.info(f'')

    if not check_index_exist('idx_customer_online_sales', 'online_sales'):
        cursor.execute(sql_query_2)
        root_logger.info(f"Successfully created index: '{sql_query_1}'")
        root_logger.info(f'-------------------------------------------------------------')
        root_logger.info(f'')

if __name__ == '__main__':
    try:
        set_up_access_controls()
    except (Exception) as err:
        root_logger.error(err)