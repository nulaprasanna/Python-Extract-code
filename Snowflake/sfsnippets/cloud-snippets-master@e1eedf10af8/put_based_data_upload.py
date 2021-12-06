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


class ConfigProperties(object):
    def __init__(self):
        # TODO: REPLACE ALL VALUES
        self.connecting_user = 'platform_ops_svc'
        self.account = 'ciscodev'
        self.connecting_warehouse = 'PLATFORM_OPS_WH'
        self.connecting_role = 'PLATFORM_OPS_ROLE'
        self.connecting_database = 'EDW_OPS_ETL_DB_DV3'
        self.connecting_schema = 'STG'
        self.keeper_namespace = 'cloudDB'
        self.keeper_uri = 'https://east.keeper.cisco.com'
        self.keeper_token = 's.XiDgLcvVI......'
        self.secret_path = 'secret/snowflake/dev/...'


if __name__ == "__main__":
    # create some sample CSV data to upload
    sample_data = ''
    for x in range(0, 100):
        sample_data += str(x) + ',' + str(x*2) + '\n'
    f = open("my_sample_data.txt", "w")
    f.write(sample_data)
    f.close()

    config_properties = ConfigProperties()
    conn = open_database_connection(config_properties)
    cursor = conn.cursor()
    cursor.execute('CREATE OR REPLACE TABLE PYTHON_PUT_TEST (x_value string, y_value string)')
    cursor.execute('CREATE OR REPLACE TEMPORARY STAGE PYTHON_PUT_TEST_STAGE file_format = EDW_UTIL_DB.FILE_FORMATS.CSV_COMMA')
    cursor.execute('PUT file://.\\my_sample_data.txt @PYTHON_PUT_TEST_STAGE PARALLEL=10')
    cursor.execute('COPY INTO PYTHON_PUT_TEST FROM (SELECT * FROM @PYTHON_PUT_TEST_STAGE)')
