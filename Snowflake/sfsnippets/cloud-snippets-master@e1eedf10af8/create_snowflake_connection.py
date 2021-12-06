"""
Connecting to Snowflake should be done with Keeper integration.
"""

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
import hvac


def open_database_connection(config_properties):
    """

    Establish a database connection. Assumes Snowflake.

    :param config_properties: This is the properties obtained from the util pull of config.ini
    :return: The database connection object
    """

    # Connect to Keeper to collect secrets
    client = hvac.Client(
        url=config_properties.keeper_uri,
        namespace=config_properties.keeper_namespace,
        token=config_properties.keeper_token
    )

    # Secrets are stored within the key entitled 'data'
    keeper_secrets = client.read(config_properties.secret_path)['data']
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
        user=config_properties.connecting_user
        , account=config_properties.account
        , warehouse=config_properties.connecting_warehouse
        , role=config_properties.connecting_role
        , database=config_properties.connecting_database
        , schema=config_properties.connecting_schema
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
