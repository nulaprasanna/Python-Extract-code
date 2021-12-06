import hvac
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import util
import pandas as pd


def open_database_connection(config_settings, connection_info):
    """
    Establishes a database connection to Snowflake.

    :param connection_info: The connection info consists of the ConnectionSnowflakeObject.
    :return: Connection Object
    """
    encrypted_passphrase = util.read_file(config_settings["PASSPHRASE_FILE"])
    encrypted_private_key = util.read_file(config_settings["PRIVATE_KEY_FILE"])

    passphrase = util.decrypt_text(config_settings["FILE_ENCRYPTION_PASSWORD"], encrypted_passphrase).getvalue().decode()
    private_key = util.decrypt_text(config_settings["FILE_ENCRYPTION_PASSWORD"], encrypted_private_key).getvalue().decode()

    conn = None
    try:
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
            user=connection_info.connecting_user
            , account=connection_info.account
            , warehouse=connection_info.connecting_warehouse
            , role=connection_info.connecting_role
            , database=connection_info.connecting_database
            , schema=connection_info.connecting_schema
            , private_key=pkb)

        print('Connected!')
    except Exception as e:
        print("Unable to connect to (" + connection_info.system_type_name +") database!!! ERROR: " + str(e))

    return conn


def close_connection(conn):
    """
    Close the Database Connection
    """
    database = "unknown"
    if hasattr(conn, 'dsn'):
        database = conn.dsn
    elif hasattr(conn, '_host'):
        database = conn._host
    print("Disconnected from database (" + str(database) + ")!")

    conn.close()
    return None


def collect_keeper_contents(snowflake_connection_details):
    """
    Collect all the secrets from keeper for the private key and passphrase

    :param snowflake_connection_details: The connection info consists of the ConnectionSnowflakeObject.
    :return: Tuple of (passphrase, privatekey)
    """

    # Config properties
    keeper_uri = snowflake_connection_details.keeper_uri
    keeper_namespace = snowflake_connection_details.keeper_namespace
    keeper_token = snowflake_connection_details.keeper_token
    secret_path = snowflake_connection_details.secret_path

    client = hvac.Client(
        url=keeper_uri,
        namespace=keeper_namespace,
        token=keeper_token
    )

    try:
        # Collect all the data from keeper for the provided input
        full_keeper_returned_data = client.read(secret_path)

        # Secrets are stored within the key entitled 'data'
        keeper_secrets = full_keeper_returned_data['data']

        # extract from the secrets the components you need
        passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
        private_key = keeper_secrets['private_key']

        passphrase_privatekey_tuple = (passphrase, private_key)

    except Exception as e:
        print('ERROR: Unable to collect secrets from keeper. error:' + str(e))
        # If Keeper Connection required, and no keeper available, program will fail. So exit.
        exit(1)

    return passphrase_privatekey_tuple


def collect_keys_for_snowflake(config_settings, snowflake_connection_details, is_force):
    """
    Reads the cached files of the private key and passphrase. If they do not exist, they will be created.

    :param config_settings: The [GENERAL] config settings
    :param snowflake_connection_details: The connection info consists of the ConnectionSnowflakeObject.
    :param is_force: Boolean. Will force collection of keeper contents
    :return: Tuple of (passphrase, privatekey)
    """
    encrypted_private_key = util.read_file(config_settings["PRIVATE_KEY_FILE"])
    encrypted_passphrase = util.read_file(config_settings["PASSPHRASE_FILE"])

    # Check for private key and passphrase
    if encrypted_private_key == "FILE_READ_ERROR" or encrypted_passphrase == "FILE_READ_ERROR" or is_force:
        print("Had to contact keeper for details")
        passphrase = collect_keeper_contents(snowflake_connection_details)[0]
        pkey = collect_keeper_contents(snowflake_connection_details)[1]

        passphrase_encrypted = util.encrypt_text(config_settings["FILE_ENCRYPTION_PASSWORD"], passphrase).getvalue()
        pkey_encrypted = util.encrypt_text(config_settings["FILE_ENCRYPTION_PASSWORD"], pkey).getvalue()

        util.write_file(config_settings["PRIVATE_KEY_FILE"], pkey_encrypted)
        util.write_file(config_settings["PASSPHRASE_FILE"], passphrase_encrypted)
    else:
        passphrase = util.decrypt_text(config_settings["FILE_ENCRYPTION_PASSWORD"], encrypted_passphrase).getvalue().decode()
        pkey = util.decrypt_text(config_settings["FILE_ENCRYPTION_PASSWORD"], encrypted_private_key).getvalue().decode()

    return (passphrase, pkey)


def run_sample_sql(snowflake_connection):
    """
    Collect all database info and write to a file.

    :param snowflake_connection: The connection info consists of the ConnectionSnowflakeObject.
    :return: None
    """

    database_information = pd.DataFrame()

    try:
        # Gather metadata for the user input's key column
        show_databases_sql = 'show databases'
        database_information = pd.read_sql(show_databases_sql, con=snowflake_connection)
        database_information.to_csv('myfile.csv')
    except Exception as e:
        print("Unable to collect database information!!! ERROR: " + str(e))
