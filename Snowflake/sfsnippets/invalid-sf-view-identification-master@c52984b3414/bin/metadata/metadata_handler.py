from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
# snowflake.connector.paramstyle = 'qmark'
import hvac
import cx_Oracle
import sys
import pandas as pd
from datetime import datetime as dt
import re
import json
import configparser
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from datetime import timedelta
from utils import util
 


def create_oracle_connection(account='ORACLE'):
    """
    Create Oracle connection and return conn
    Calls open_oracle_connection

    :return: The oracle connection
    """
    try:
        property_file_contents = util.collect_property_file_contents(
            '../properties/config.ini', account)
        conn = open_oracle_connection(property_file_contents)
        return conn
    except Exception as e:
        print('ERROR: Unable to connect to ORACLE account. Will be unable to write output.')
        print('ERROR: ' + str(e))
        raise Exception(e)

        
def open_oracle_connection(config_properties):
    """
    Establish a database connection with Oracle

    :param config_properties: This is the properties obtained from the utl pull of config.ini
    :return: The database connection object
    """

    try:
        # Need to optimize to cx_Oracle.SessionPool
        dsn_tns = cx_Oracle.makedsn(config_properties['host'], config_properties['port'],
                                    service_name=config_properties['service_name'])  # build connection string
        conn = cx_Oracle.connect(user=config_properties['db_username'],
                                 password=config_properties['db_password'], dsn=dsn_tns)
        return conn
    except Exception as e:
        print(f"Unable to connect to {config_properties['host']}@{config_properties['service_name']} " +
              f"due to {str(e).strip()}")
        raise Exception(e)


def close_connection(conn):
    """
    Close a SF connection 

    :param conn: this is the conn object when creating the connection
    """
    if conn:
        conn.commit()
        conn.close()


def execute_oracle_df_qry(conn, qry):
    """
    Execute given query on Oracle

    :param qry: This is the qry to be executed on Oracle
    :return: Given dataframe with output of requested query
    """

    try:
        cursor = conn.cursor()
        curOpen = cursor.execute(qry)
        oraCols = [row[0] for row in curOpen.description]
        df_oraData = pd.DataFrame(curOpen.fetchall(), columns=(oraCols))
        df_oraData.columns = [x.upper() for x in df_oraData.columns.tolist()]
        cursor.close()
        return df_oraData
    except Exception as e:
        raise Exception(e)


def execute_oracle_qry(conn, qry):
    """
    Execute given query on Oracle without any return value

    :param conn: This is thevonnection to Oracle
    :param qry: This is the qry to be executed on Oracle
    """
    try:
        cursor = conn.cursor()
        curOpen = cursor.execute(qry)
        conn.commit()
        cursor.close()
    except Exception as e:
        raise Exception(e)

def executemany_oracle_qry(conn, qry, data):
    """
    Execute given queries on Oracle without any return value
    :param conn: This is thevonnection to Oracle
    :param qry: This is the list of qries to be executed on Oracle
    """
    try:
        cursor = conn.cursor()
        curOpen = cursor.executemany(qry, data, batcherrors = True)
        for errorObj in cursor.getbatcherrors():
            print("ERROR Row", errorObj.offset, "has error", errorObj.message)
        conn.commit()
        cursor.close()
    except Exception as e:
        raise Exception(e)

def create_sf_engine(sf_account):
    try:
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import create_engine
        # from snowflake.sqlalchemy import MergeInto
        from snowflake.sqlalchemy import URL

        config_properties = util.collect_property_file_contents('../properties/config.ini',
                                                                        sf_account.upper())
        pkb = get_pkb(config_properties)
        engine = create_engine(URL(
        account=config_properties['account'],
        user=config_properties['connecting_user'],
        warehouse=config_properties['connecting_warehouse'],
        database=config_properties['connecting_database'],
        schema=config_properties['connecting_schema'],
        role=config_properties['connecting_role']
        ),
        connect_args={
            'private_key': pkb,
            },
        )
        session = sessionmaker(bind=engine)()
        return session,engine
    except Exception as e:
        raise Exception (e)

def get_pkb(config_properties):
    # Connect to Keeper to collect secrets
    client = hvac.Client(
        url=config_properties['keeper_uri'],
        namespace=config_properties['keeper_namespace'],
        token=config_properties['keeper_token'],
    )

    # Secrets are stored within the key entitled 'data'

    keeper_secrets = client.read(
        config_properties['keeper_secret_path'])['data']
    passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
    private_key = keeper_secrets['private_key']

    # PEM key must be byte encoded
    key = bytes(private_key, 'utf-8')

    p_key = serialization.load_pem_private_key(
        key, password=passphrase.encode(), backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    
    return pkb

def open_sf_connection(config_properties):
    """
    Establish a database connection. Assumes Snowflake.

    :param config_properties: This is the properties obtained from the util pull of config.ini
    :return: The database connection object
    """
    try:
        pkb = get_pkb(config_properties)
        conn = snowflake.connector.connect(
            user=config_properties['connecting_user'], account=config_properties['account'],
            warehouse=config_properties['connecting_warehouse'],
            database='EDW_OPS_ETL_DB',
            schema='WI',
            role=config_properties['connecting_role'],
            private_key=pkb,
            insecure_mode=True)
        return conn
    except Exception as e:
        raise Exception (e)


def create_sf_connection(sf_account):
    """
    Create snowflake connection and return conn
    Calls open_sf_connection

    :param sf_account: An sf_account is the specific Snowflake environment being used
    :return: The snowflake connection
    """
    try:
        max_tries = 1
        while 1:
            try:
                property_file_contents = util.collect_property_file_contents('../properties/config.ini',
                                                                        sf_account.upper())
                conn = open_sf_connection(property_file_contents)
                return conn
            except Exception as e:
                if max_tries < 3:
                    max_tries += 1
                    continue
                else:
                    raise Exception(e)
    except Exception as e:
        print(
            f'ERROR: Unable to connect to {sf_account.upper()} account.')
        print('ERROR: ' + str(e))
        raise Exception(e)
        
def execute_sf_qry(conn, qry):
    """
    Execute given query on SF without any return value

    :param conn: This is the connection to SF
    :param qry: This is the qry to be executed on SF

    : return : this will return the response from SF
    """

    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(qry)
        df_sfData = pd.DataFrame(cur.fetchall())
        df_sfData.columns = map(str.upper, df_sfData.columns)
        cur.close()
        return df_sfData
    except Exception as e:
        raise Exception(e)