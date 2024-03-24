from commons.database import get_postgres_db_connection
from pipelines.layer1.customer_info import load_customer_data_to_raw_table

if _name__ == "__main__":
    try:
        db_connection, cursor, dbname = get_postgres_db_connection()

        load_customer_data_to_raw_table(db_connection, cursor, dbname)
    except Exception as e:
            print('ERROR OCCURED WHEN RUNNING TASKS!!!')
            print(e)
    finally:
        if cursor is not None:
            cursor.close()
            print("DB Cursor closed!!")

        if db_connection is not None:
            db_connection.close()
            print("DB connection closed!!")