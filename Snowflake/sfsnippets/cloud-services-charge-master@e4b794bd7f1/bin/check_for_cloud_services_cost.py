"""
Connecting to Snowflake should be done with Keeper integration.
"""

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
import configparser
import hvac
import pandas as pd
import subprocess


def collect_property_file_contents(property_file, header):
    """
    Collects and makes available all contents of the property file. This should be identified by 'config.ini'

    :param property_file: The relative path to the property file
    :param header: The identifying bucket of property file contents. EG. [CISCODEV.US-EAST-1], [CONFIG]
    :return: The property file object
    """
    try:
        config = configparser.ConfigParser()
        config.read(property_file)

        configuration = config[header]
        return configuration
    except Exception as e:
        print('ERROR: Unable to open and collect property file contents for (property file: ' + property_file + ', header: ' + header + ')')
        print('ERROR: ' + str(e))


def open_database_connection(config_settings):
    """

    Establish a database connection. Assumes Snowflake.

    :param config_properties: This is the properties obtained from the util pull of config.ini
    :return: The database connection object
    """

    # Connect to Keeper to collect secrets
    client = hvac.Client(
        url=config_settings["keeper_uri"],
        namespace=config_settings["keeper_namespace"],
        token=config_settings["keeper_token"]
    )

    # Secrets are stored within the key entitled 'data'
    keeper_secrets = client.read(config_settings["secret_path"])['data']
    passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
    private_key = keeper_secrets['private_key']

    # PEM key must be byte encoded
    key = bytes(private_key, 'utf-8')

    p_key = serialization.load_pem_private_key(
        key
        , password=passphrase.encode()
        , backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER
        , format=serialization.PrivateFormat.PKCS8
        , encryption_algorithm=serialization.NoEncryption())

    conn = snowflake.connector.connect(
        user=config_settings["connecting_user"]
        , account=config_settings["account"]
        , warehouse=config_settings["connecting_warehouse"]
        , role=config_settings["connecting_role"]
        , database=config_settings["connecting_database"]
        , schema=config_settings["connecting_schema"]
        , private_key=pkb)

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
    conn.commit()
    conn.close()
    return None


def check_for_charge(conn):
    try:
        # Gather metadata for the user input's key column
        check_for_charge_sql = "select CREDITS_USED_CLOUD_SERVICES / CREDITS_USED_COMPUTE PRCT , * " \
                               "from METERING_DAILY_HISTORY " \
                               "where CREDITS_USED_CLOUD_SERVICES <> 0 AND PRCT >= .8 " \
                               "ORDER by USAGE_DATE desc limit 100"
        charge_costs = pd.read_sql(check_for_charge_sql, con=conn)

        if not charge_costs.empty:
            cmd = " echo -e \"Hello SF Admins,\n\nSome cost has been detected for Cloud Services. Please look into " \
                  "this. Costs associated for cloud services should not exceed 10% of total credit cost. " \
                  "See here: https://docs.snowflake.net/manuals/user-guide/credits.html#cloud-services-credit-usage" \
                  "\n\nThank you,\nAutomation Team\" | mail -s \"Unexpected Snowflake Cost Detected\" " \
                  "-r \"no-reply@cisco.com\" \"sf-admin@cisco.com\""

            print("command: " + str(cmd))
            subprocess.call(cmd, shell=True)
    except Exception as e:
      print(e)


account_list = ["CISCO.US-EAST-1", "CISCOSTAGE.US-EAST-1", "CISCODEV.US-EAST-1"]
for account in account_list:
    config_settings = collect_property_file_contents('../properties/config.ini', account)
    sf_conn = open_database_connection(config_settings)
    check_for_charge(sf_conn)
    close_connection(sf_conn)
