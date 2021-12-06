import snowflake.connector
import pandas as pd
import csv
import openpyxl


def what_you_need_to_set():
    """
    CHANGE MY SETTINGS BELOW!!!
    Also... make me an object from config.ini!

    :return: tuple of all details
    """
    sql_query = 'select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY LIMIT 100'
    user = 'algarbar@cisco.com'
    account = 'ciscodev.us-east-1'
    warehouse = 'platform_ops_wh'
    role = 'platform_ops_role'
    database = 'EDW_OPS_ETL_DB_DV3'
    schema = 'SS'
    output_file_name = 'output.xlsx'
    return sql_query, user, account, warehouse, role, database, schema, output_file_name


def open_database_connection(stuff_you_set_above):
    conn = snowflake.connector.connect(
        user=stuff_you_set_above[1]
        , account=stuff_you_set_above[2]
        , warehouse=stuff_you_set_above[3]
        , role=stuff_you_set_above[4]
        , database=stuff_you_set_above[5]
        , schema=stuff_you_set_above[6]
        , authenticator='externalbrowser')

    return conn


def commit(conn):
    """
    Perform pure commit
    """
    conn.commit()
    return None


def close_connection(conn):
    """
    Close the Snowflake Connection. Attach this method to any finally or exit block.
    """
    conn.close()
    return None


def download_data(sf_conn, stuff_you_set_above):
    cursor = sf_conn.cursor()
    try:
        sql_query = stuff_you_set_above[0]
        results = pd.read_sql(sql_query, con=sf_conn)
        results.to_excel(stuff_you_set_above[7])
    except Exception as e:
        print('ERROR: ' + str(e))


settings = what_you_need_to_set()
sf_conn = open_database_connection(settings)
download_data(sf_conn, settings)
close_connection(sf_conn)
