import logging
import sys
from copy import deepcopy
import cx_Oracle
import hvac
import pandas as pd
import snowflake.connector
import teradata
import jaydebeapi
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from models.ConnectionOracleObject import ConnectionOracleObject
from models.ConnectionTeradataObject import ConnectionTeradataObject
from utils.util import read_file


def open_database_connection(connection_info):
    """
    Establishes a database connection to Oracle, Snowflake, or Teradata.

    :param connection_info: The connection info consists of the ConnectionXXXObject for one of the supported types.
    :return: Connection Object
    """

    # Collect the type name such as 'ConnectionTeradataObject'
    db_type = type(connection_info).__name__
    logging.debug("Detected DB type: " + db_type)

    logging.info("Connecting to (" + connection_info.system_type_name + ")...")
    try:
        if db_type == 'ConnectionTeradataObject':
            # Since no access to the teradata module switched to use jaydebeapi.
            # Example: https://apps.na.collabserv.com/wikis/home?lang=en-us#!/wiki/W3549edaec77e_4369_8030_51be7d08b460/page/How%20to%20Connect%20to%20Teradata%20with%20R%20and%20Python
            # udaExec = teradata.UdaExec(appName="test", version="1.0", configureLogging=False, logConsole=False, logFile="../logs/teradataErrors.log")
            # conn = udaExec.connect(method="odbc", system=connection_info.system, username=connection_info.username, password=connection_info.password)
            conn = jaydebeapi.connect(jclassname="com.teradata.jdbc.TeraDriver",
                                      url="jdbc:teradata://" + connection_info.system + ".cisco.com/LOGMECH=TD2",
                                      driver_args=[connection_info.username, connection_info.password],
                                      jars=['../lib/tdgssconfig.jar', '../lib/terajdbc4.jar'])
            logging.info("CONNECTED!")
        if db_type == 'ConnectionOracleObject':
            oracle_connection_info = connection_info
            conn = cx_Oracle.connect(oracle_connection_info.username+"/"+oracle_connection_info.password+"@"+oracle_connection_info.connection_string)
            logging.info("CONNECTED!")
        if db_type == 'ConnectionSnowflakeObject':
            # Connect to Keeper to collect secrets
            client = hvac.Client(
                url=connection_info.keeper_uri,
                namespace=connection_info.keeper_namespace,
                token=connection_info.keeper_token
            )

            # Secrets are stored within the key entitled 'data'
            keeper_secrets = client.read(connection_info.secret_path)['data']
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
                user=connection_info.connecting_user
                , account=connection_info.account
                , warehouse=connection_info.connecting_warehouse
                , role=connection_info.connecting_role
                , database=connection_info.connecting_database
                , schema=connection_info.connecting_schema
                , private_key=pkb)

            logging.info("CONNECTED!")

    except Exception as e:
        logging.error("Unable to connect to (" + connection_info.system_type_name +") database!!! ERROR: " + str(e))
        sys.exit(1)

    return conn


def close_connection(conn):
    """
    Close the Database Connection
    """
    logging.info("Disconnected from database!")
    conn.close()
    return None


def clean_schema_names(source_type, request_object):
    """
    Corrects database inconsistencies which exist among data sources. For example, Snowflake has a 2 part table alias.

    :param source_type: The source type defined as a class type of the database connection object
    :param request_object: The RequestObject model type
    :return: A deep cloned RequestObject model consisting of corrected values.
    """
    clean_request_object = deepcopy(request_object)

    # Correct for Snowflake
    if "SnowflakeConnection" in source_type:
        clean_request_object.db_schema_name = clean_request_object.db_instance_name + "." + clean_request_object.db_schema_name

    # Correct for Teradata
    if clean_request_object.db_instance_name in ['TDPROD', 'TDPROD2', 'TDDEV', 'TDTEST']:
        clean_request_object.db_schema_name = clean_request_object.db_schema_name.replace("ETLVWDB", "ETLONLYDB")
        clean_request_object.db_schema_name = clean_request_object.db_schema_name.replace("VWDB", "DB")

    return clean_request_object


def collect_database_connection_details(metadata_connection, request_object, properties):
    """
    Using the EDS metadata repo, collects the various source connection information required.

    :param metadata_connection: The EDS metadata repo
    :param request_object: The RequestObject model type
    :param properties: The properties object. From this the oracle hash passphrase (METADATA_PASSWORD_DECRYPTION_PASSPHRASE) is required.
    :return: A dataframe consisting of all database connection details.
    """
    table_connection_details = pd.DataFrame()

    # Clean the TD schema names
    clean_request_object = clean_schema_names(str(type(metadata_connection)), request_object)

    try:
        # Gather metadata for the user input
        connection_details_query = read_file('../bin/metadata/DML/SELECT_LOGIN_INFORMATION.sql')
        connection_details_query = connection_details_query.replace("$$METADATA_PASSWORD_DECRYPTION_PASSPHRASE", properties["METADATA_PASSWORD_DECRYPTION_PASSPHRASE"])
        connection_details_query = connection_details_query.replace("$$ENVIRONMENT_NAME", clean_request_object.env_name)
        connection_details_query = connection_details_query.replace("$$DB_INSTANCE_NAME", clean_request_object.db_instance_name)
        connection_details_query = connection_details_query.replace("$$DB_SCHEMA_NAME", clean_request_object.db_schema_name)
        connection_details_query = connection_details_query.replace("$$TABLE_NAME", clean_request_object.table_name)
        table_connection_details = pd.read_sql(connection_details_query, con=metadata_connection)

        # Ensure Valid Request
        if table_connection_details.empty:
            logging.warning("Could not find any such table (" + clean_request_object.db_schema_name + "." + clean_request_object.table_name +") with connection details in the metadata repository.")

    except Exception as e:
        logging.error("Unable to collect connection details for input table (" + clean_request_object.db_schema_name + "." + clean_request_object.table_name +")!!! ERROR: " + str(e))

    return table_connection_details


def collect_table_key_columns(metadata_connection, request_object):
    """
    From the EDS metadata repo, collects the natural key if one is available. If one is not available, it sends all columns.

    :param metadata_connection: The EDS metadata repo
    :param request_object: The RequestObject model type
    :return: A dataframe of all table key columns INCLUDING the key identifier
    """
    table_unique_key = pd.DataFrame()

    # Clean the TD schema names
    clean_request_object = clean_schema_names(str(type(metadata_connection)), request_object)

    try:
        # Gather metadata for the user input's key column
        unique_key_query = read_file('../bin/metadata/DML/SELECT_ACTIVE_UNIQUE_KEY.sql')
        unique_key_query = unique_key_query.replace("$$ENVIRONMENT_NAME", clean_request_object.env_name)
        unique_key_query = unique_key_query.replace("$$DB_INSTANCE_NAME", clean_request_object.db_instance_name)
        unique_key_query = unique_key_query.replace("$$DB_SCHEMA_NAME", clean_request_object.db_schema_name)
        unique_key_query = unique_key_query.replace("$$TABLE_NAME", clean_request_object.table_name)
        table_unique_key = pd.read_sql(unique_key_query, con=metadata_connection)

        if table_unique_key.empty:
            logging.warning("Could not find any unique key for this table (" + clean_request_object.db_schema_name + "." + clean_request_object.table_name +").")
            logging.info("Could not collect key columns so the output is providing all columns.")
            table_unique_key = collect_all_table_columns(metadata_connection, clean_request_object)

    except Exception as e:
        logging.error("Unable to collect natural key columns for input table (" + clean_request_object.db_schema_name + "." + clean_request_object.table_name +")!!! ERROR: " + str(e))

    return table_unique_key


def collect_all_table_columns(metadata_connection, request_object):
    """
    From the EDS metadata repo, collect all columns which exist in the table.

    :param metadata_connection: The EDS metadata repo
    :param request_object: The RequestObject model type
    :return: A dataframe of all columns which exist in the table
    """
    all_table_columns = pd.DataFrame()

    # Validate able to collect all columns
    try:
        all_table_columns_query = read_file('../bin/metadata/DML/SELECT_ALL_TABLE_COLUMNS.sql')
        all_table_columns_query = all_table_columns_query.replace("$$ENVIRONMENT_NAME", request_object.env_name)
        all_table_columns_query = all_table_columns_query.replace("$$DB_INSTANCE_NAME", request_object.db_instance_name)
        all_table_columns_query = all_table_columns_query.replace("$$DB_SCHEMA_NAME", request_object.db_schema_name)
        all_table_columns_query = all_table_columns_query.replace("$$TABLE_NAME", request_object.table_name)
        all_table_columns = pd.read_sql(all_table_columns_query, con=metadata_connection)

        if all_table_columns.empty:
            logging.warning("Could not find any columns for this table (" + request_object.db_schema_name + "." + request_object.table_name +"). Inspect the metadata repository.")

    except Exception as e:
        logging.error("Unable to collect all columns for input table (" + request_object.db_schema_name + "." + request_object.table_name +")!!! ERROR: " + str(e))

    return all_table_columns


def create_connection_object(table_connection_details):
    """
    Various source systems require different connection parameters. This method accepts the database details, and
    according to the database type, will construct the appropriate database connection object.

    :param table_connection_details: Those details collected from collect_database_connection_details()
    :return: The appropriate database connection object. Could be of any DB model type.
    """
    try:
        if table_connection_details["DB_TYPE"][0] == "TERADATA":
            logging.info("Identified input as a Teradata source.")
            connection_obj = ConnectionTeradataObject(table_connection_details["DB_INSTANCE_NAME"][0]
                                                         , table_connection_details.loc[(table_connection_details['PARAMETER_NAME'] == "SOURCE_LOGIN")]["PARAMETER_VALUE"].values[0]
                                                         , table_connection_details.loc[(table_connection_details['PARAMETER_NAME'] == "SOURCE_LOGIN_PASSWORD")]["PARAMETER_VALUE"].values[0])
            return connection_obj
        elif table_connection_details["DB_TYPE"][0] == "ORACLE":
            logging.info("Identified input as an Oracle source.")

            connection_string = table_connection_details["DB_INSTANCE_NAME"][0]
            username = table_connection_details.loc[(table_connection_details['PARAMETER_NAME'] == "SOURCE_LOGIN")]["PARAMETER_VALUE"].values[0]
            password = table_connection_details.loc[(table_connection_details['PARAMETER_NAME'] == "SOURCE_LOGIN_PASSWORD")]["PARAMETER_VALUE"].values[0]
            connection_obj = ConnectionOracleObject(connection_string, username, password)
        # TODO: ADD TYPE FOR SNOWFLAKE- This is presently not in the EDS repo.

            return connection_obj
    except Exception as e:
        logging.error("Unable to create a connection object for the provided (" + table_connection_details.system_type_name + ") system!!! ERROR: " + str(e))


def validate_natural_key(source_connection, request_object, table_unique_key):
    """
    For the provided key, ensure that there are no duplicate records identified

    :param source_connection: The data source which contains the data
    :param request_object: The RequestObject model type
    :param table_unique_key: A dataframe of the table unique key in "COLUMN_NAME".
    :return: The dataframe results of the SQL query
    """
    try:
        # Ensure that the natural key is valid
        natural_key_string = ','.join(map(str, table_unique_key["COLUMN_NAME"].values))
        validate_natural_key_query = read_file('../bin/metadata/DML/SELECT_VALIDATE_NATURAL_KEY.sql')
        validate_natural_key_query = validate_natural_key_query.replace("$$NATURAL_KEY", natural_key_string)
        validate_natural_key_query = validate_natural_key_query.replace("$$DB_SCHEMA_NAME", request_object.db_schema_name)
        validate_natural_key_query = validate_natural_key_query.replace("$$TABLE_NAME", request_object.table_name)
        natural_key_validation_results = pd.read_sql(validate_natural_key_query, con=source_connection)

        return natural_key_validation_results

    except Exception as e:
        logging.error("Unable to validate key for input table (" + request_object.db_schema_name + "." + request_object.table_name +")!!! ERROR: " + str(e))


def collect_table_record_count(source_connection, request_object):
    """
    For the provided table, determine the number of records in the source.

    :param source_connection: The data source which contains the data
    :param request_object: The RequestObject model type
    :return: The dataframe results of the SQL query
    """
    clean_request_object = clean_schema_names(str(type(source_connection)), request_object)

    try:
        table_count_query = read_file('../bin/metadata/DML/SELECT_COUNT_OF_RECORDS.sql')
        table_count_query = table_count_query.replace("$$DB_SCHEMA_NAME", clean_request_object.db_schema_name)
        table_count_query = table_count_query.replace("$$TABLE_NAME", clean_request_object.table_name)
        table_count = pd.read_sql(table_count_query, con=source_connection).astype(float, errors='ignore')
        table_count.columns = map(str.lower, table_count.columns)

        return table_count
    except Exception as e:
        logging.error("Unable to collect count of table (" + clean_request_object.db_schema_name + "." + clean_request_object.table_name +")!!! ERROR: " + str(e))


def collect_sum_of_columns(source_connection, request_object, columns_list):
    """

    :param source_connection: The data source which contains the data
    :param request_object: The RequestObject model type
    :param columns_list: A dataframe consisting of the columns in array "COLUMN_NAME"
    :return:  The dataframe results of the SQL query
    """
    # TODO: Implement the ability to select non numerical columns
    #           Algo ???:
    #                 1) Perform cell raw to hex
    #                 2) Hex to Int
    #                 3) SUM(Int)
    clean_request_object = clean_schema_names(str(type(source_connection)), request_object)

    sum_sql = ('SUM(' + columns_list["COLUMN_NAME"] + ") as SUM_" + columns_list["COLUMN_NAME"]).values
    sum_sql = ','.join(map(str, sum_sql))

    try:
        table_column_sums_query = read_file('../bin/metadata/DML/SELECT_SUM_OF_COLUMNS.sql')
        table_column_sums_query = table_column_sums_query.replace("$$DB_SCHEMA_NAME", clean_request_object.db_schema_name)
        table_column_sums_query = table_column_sums_query.replace("$$TABLE_NAME", clean_request_object.table_name)
        table_column_sums_query = table_column_sums_query.replace("$$SUM_COLUMNS", sum_sql)
        table_column_sums = pd.read_sql(table_column_sums_query, con=source_connection).astype(float, errors='ignore')
        table_column_sums.columns = map(str.lower, table_column_sums.columns)

        return table_column_sums
    except Exception as e:
        logging.error("Unable to collect column sums for table (" + clean_request_object.db_schema_name + "." + clean_request_object.table_name +")!!! ERROR: " + str(e))


def collect_a_random_set_of_records(source_connection, request_object, number_of_records):
    """
    For the provided table, select a random set of records equal to the number_of_records. 0 means all records

    :param source_connection: The data source which contains the data
    :param request_object: The RequestObject model type
    :param number_of_records: A limiting number of records to fetch.
    :return: A dataframe consisting of a selected number of records
    """
    # TODO: Instead of a limit select an actual random set of records.
    number_of_records = str(number_of_records)
    source_type = str(type(source_connection)).lower()

    clean_request_object = clean_schema_names(source_type, request_object)

    if number_of_records == "0":
            limit_condition = ""
    elif "snowflake" in source_type:
        limit_condition = "LIMIT " + number_of_records
    elif "oracle" in source_type:
        limit_condition = "WHERE rownum <= " + number_of_records
    elif "teradata" in source_type:
        limit_condition = "SAMPLE " + number_of_records
    else:
        limit_condition = "WHERE 0 = 1"

    try:
        random_records_query = read_file('../bin/metadata/DML/SELECT_RANDOM_RECORDS.sql')
        random_records_query = random_records_query.replace("$$DB_SCHEMA_NAME", clean_request_object.db_schema_name)
        random_records_query = random_records_query.replace("$$TABLE_NAME", clean_request_object.table_name)
        random_records_query = random_records_query.replace("$$LIMIT_CONDITION", limit_condition)
        random_records = pd.read_sql(random_records_query, con=source_connection).astype(float, errors='ignore')
        random_records.columns = map(str.lower, random_records.columns)

        return random_records
    except Exception as e:
        logging.error("Unable to collect random records from table (" + clean_request_object.db_schema_name + "." + clean_request_object.table_name +")!!! ERROR: " + str(e))


def select_particular_records(source_connection, request_object, record_select_override):
    """
    Provided a WHERE filter on which to select a basis of records, this will select those particular records.

    Most often, this can be used in conjunction with the random records selection method.

    :param source_connection: The data source which contains the data
    :param request_object: The RequestObject model type
    :param record_select_override: The WHERE clause by which to select the records. Does NOT contain the word WHERE
    :return: A dataframe of the requested records
    """
    source_type = str(type(source_connection))

    clean_request_object = clean_schema_names(source_type, request_object)

    try:
        particular_records_query = read_file('../bin/metadata/DML/SELECT_PARTICULAR_RECORDS.sql')
        particular_records_query = particular_records_query.replace("$$DB_SCHEMA_NAME", clean_request_object.db_schema_name)
        particular_records_query = particular_records_query.replace("$$TABLE_NAME", clean_request_object.table_name)
        particular_records_query = particular_records_query.replace("$$WHERE_CONDITION", record_select_override)
        particular_records = pd.read_sql(particular_records_query, con=source_connection).astype(float, errors='ignore')
        particular_records.columns = map(str.lower, particular_records.columns)

        return particular_records
    except Exception as e:
        logging.error("Unable to collect those particular records from table (" + clean_request_object.db_schema_name + "." + clean_request_object.table_name +")!!! ERROR: " + str(e))


def collect_similar_data_to_input(source_connection, request_object, input_data, table_unique_key):
    """
    For a provided dataframe of data and a DF unique key on which to identify records, this method will construct a
    WHERE clause which can be used in the select_particular_records() method as a predicate.

    :param source_connection: The data source which contains the data
    :param request_object: The RequestObject model type
    :param input_data: The dataframe of table data to be used for a limit
    :param table_unique_key: The dataframe captured above containing the table's unique key
    :return: A string WHERE condition which can be used to select particular data.
    """
    selectable_data = deepcopy(input_data)
    unique_key_list = []

    # TODO: WTF is all this regex BS and sloppy DF parsing? Do better.
    for key in table_unique_key["COLUMN_NAME"].values:
        unique_key_list.append(key)

    for data_column in selectable_data:
        if data_column not in unique_key_list:
            del selectable_data[data_column]

    sql = ""
    for row in range(0, len(selectable_data)):
        for i in range(0, len(selectable_data.columns)):
            if i == 0:
                sql += "("
            sql += str(selectable_data.columns.values[i]) + " = '" + str(selectable_data.values[row][i]) + "'"
            if i != len(selectable_data.columns) - 1:
                sql += " and "
        sql += ") OR "
    sql = sql[0:-3]

    return sql
