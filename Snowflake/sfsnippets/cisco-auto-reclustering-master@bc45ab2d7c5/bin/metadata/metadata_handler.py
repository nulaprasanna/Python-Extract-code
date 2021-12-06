import logging
import sys
import cx_Oracle
import hvac
import json
import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import utils.util as util


def open_database_connection(connection_info):
    """
    Establishes a database connection to Oracle or Snowflake.

    :param connection_info: The connection info consists of the ConnectionXXXObject for one of the supported types.
    :return: Connection Object
    """

    # Collect the type name such as 'ConnectionTeradataObject'
    db_type = type(connection_info).__name__
    logging.debug("Detected DB type: " + db_type)

    logging.info("Connecting to (" + connection_info.system_type_name + ")...")
    try:
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
    database = "unknown"
    if hasattr(conn, 'dsn'):
        database = conn.dsn
    elif hasattr(conn, '_host'):
        database = conn._host
    logging.info("Disconnected from database (" + str(database) + ")!")

    conn.commit()
    conn.close()
    return None


def collect_tables_for_reconciliation(snowflake_connection):
    """
    Find all tables in Snowflake which have a clustering key defined.

    :param snowflake_connection: Connection object for Snowflake
    :return: Pandas DF of all tables which have clustering keys defined
    """
    table_list = pd.DataFrame()

    logging.info("Fetching the table reconciliation list ...")
    try:

        table_list_query = util.read_file('../bin/metadata/DML/SELECT_ALL_TABLES_REQUIRING_CLUSTERING.sql')
        table_list = pd.read_sql(table_list_query, con=snowflake_connection)

        if table_list.empty:
            logging.warning("Could not find any tables to consider for reclustering.")

    except Exception as e:
        logging.error("Unable to collect table list!!! ERROR: " + str(e))

    logging.info("Fetch Complete!")
    return table_list


def truncate_reconciliation_records(metadata_connection):
    """
    Clear all the records present in the incremental table of reconcil

    :param metadata_connection: The connection to the metadata layer
    :return: None
    """
    logging.info("Attempting to truncate the table reconciliation list ...")
    try:
        cursor = metadata_connection.cursor()
        statement = util.read_file('../bin/metadata/DML/TRUNCATE_INCREMENTAL_RECON_RECORDS.sql')
        cursor.execute(statement)

    except Exception as e:
        logging.error("Unable to truncate the reconciliation list!!! ERROR: " + str(e))

    logging.info("Truncate of reconciliation list Complete!")


def suspend_auto_recluster(snowflake_connection, reconciliation_records):
    """
    Disable auto recluster on the tables where it is enabled.

    :param snowflake_connection: Connection object to Snowflake
    :param reconciliation_records:  Pandas list of all tables which have clustering keys defined.
    :return: None
    """
    logging.info("Attempting to ensure the tables are not set to auto recluster ...")
    try:
        cursor = snowflake_connection.cursor()
        for row in range(0, len(reconciliation_records)):
            if reconciliation_records['AUTO_CLUSTERING_ON'].values[row] == 'YES':
                alter_table_recluster_sql = util.read_file('../bin/metadata/DML/ALTER_TABLE_SET_NO_RECLUSTER.sql')
                alter_table_recluster_sql = alter_table_recluster_sql.replace("$$TABLE_CATALOG", reconciliation_records['TABLE_CATALOG'].values[row])
                alter_table_recluster_sql = alter_table_recluster_sql.replace("$$TABLE_SCHEMA", reconciliation_records['TABLE_SCHEMA'].values[row])
                alter_table_recluster_sql = alter_table_recluster_sql.replace("$$TABLE_NAME", reconciliation_records['TABLE_NAME'].values[row])
                logging.info("Table ("
                             + reconciliation_records['TABLE_CATALOG'].values[row]
                             + "."
                             + reconciliation_records['TABLE_SCHEMA'].values[row]
                             + "."
                             + reconciliation_records['TABLE_NAME'].values[row]
                             + ") being changed to not auto cluster.")
                cursor.execute(alter_table_recluster_sql)
    except Exception as e:
        logging.error("Unable to change tables to not auto cluster!!! ERROR: " + str(e))

    logging.info("All tables set to not recluster automatically!")


def insert_reconciliation_records(metadata_connection, reconciliation_records):
    """
    Insert all tables which contain clustering keys into the metadata driving table

    :param metadata_connection: Connection to the metadata layer
    :param reconciliation_records:  Pandas list of all tables which have clustering keys defined.
    :return: None
    """
    logging.info("Attempting to upload the table reconciliation list ...")
    try:
        cursor = metadata_connection.cursor()
        for row in range(0, len(reconciliation_records)):
            statement = util.read_file('../bin/metadata/DML/INSERT_INCREMENTAL_RECON_RECORDS.sql')
            cursor.execute(statement, (
                             reconciliation_records['ACCOUNT'].values[row]
                           , reconciliation_records['TABLE_CATALOG'].values[row]
                           , reconciliation_records['TABLE_SCHEMA'].values[row]
                           , reconciliation_records['TABLE_NAME'].values[row]))

    except Exception as e:
        logging.error("Unable to insert table list!!! ERROR: " + str(e))

    logging.info("Insert Complete!")


def merge_reconciliation_records(metadata_connection):
    """
    Combine all the records from the incremental reconciliation table into the driving table list.

    :param metadata_connection: Connection to the metadata layer
    :return: None
    """
    logging.info("Attempting to merge the table reconciliation list ...")
    try:
        cursor = metadata_connection.cursor()
        statement = util.read_file('../bin/metadata/DML/MERGE_INCREMENTAL_REQUIRED_TABLES.sql')
        logging.debug("Merge Statement: " + str(statement))
        cursor.execute(statement)
    except Exception as e:
        logging.error("Unable to merge table list!!! ERROR: " + str(e))

    logging.info("Merge of Reconciliation List Complete!")


def collect_active_recluster_list(metadata_connection, group, id):
    """
    Collect the details from Oracle table CISCO_AUTO_RECLUSTER.

    :param metadata_connection: The connection to the metadata layer
    :param group: The collection of tables for which to consider reclustering. OPTIONAL
    :param id: The specific table to consider for reclustering. OPTIONAL
    :return: Pandas dataframe of all active tables possibly requiring clustering.
    """
    table_list = pd.DataFrame()

    logging.info("Fetching the list of tables for which clustering may be required...")
    try:
        if id is not None:
            table_list_query = util.read_file('../bin/metadata/DML/SELECT_ACTIVE_RECLUSTER_TABLES_BY_ID.sql')
            table_list_query = table_list_query.replace("$$ID", id)
            table_list = pd.read_sql(table_list_query, con=metadata_connection)
        elif group is not None:
            table_list_query = util.read_file('../bin/metadata/DML/SELECT_ACTIVE_RECLUSTER_TABLES_BY_GROUP.sql')
            table_list_query = table_list_query.replace("$$GROUP_NAME", group)
            table_list = pd.read_sql(table_list_query, con=metadata_connection)
        else:
            table_list_query = util.read_file('../bin/metadata/DML/SELECT_ACTIVE_RECLUSTER_TABLES.sql')
            table_list = pd.read_sql(table_list_query, con=metadata_connection)

        if table_list.empty:
            logging.warning("Could not find any active tables to consider for reclustering.")

        logging.debug("Table List: " + str(table_list['ID'].values))
    except Exception as e:
        logging.error("Unable to collect table list!!! ERROR: " + str(e))

    logging.info("Fetch Complete!")
    return table_list


def collect_clustering_information(snowflake_connection, table_name):
    """
    For the table specified, connect to snowflake and retrieve the $ Clustering Information.

    :param snowflake_connection: The row specific table clustering information
    :param table_name: The fully qualified table name which needs clustering details gathered.
    :return: Pandas dataframe of the clustering information
    """
    clustering_information = pd.DataFrame()

    logging.debug("Fetching clustering status from Snowflake for (" + str(table_name) + ").")

    try:
        # Gather metadata for the user input's key column
        clustering_info_sql = util.read_file('../bin/metadata/DDL/SELECT_CLUSTERING_INFORMATION.sql')
        clustering_info_sql = clustering_info_sql.replace("$$TABLE_NAME", table_name)
        clustering_information = pd.read_sql(clustering_info_sql, con=snowflake_connection)

        if clustering_information.empty:
            logging.warning("Could not find clustering information for the specified table.")

        clustering_information = json.loads(clustering_information.loc[0].values[0])

    except Exception as e:
        logging.error("Unable to collect clustering information!!! ERROR: " + str(e))
        clustering_information = None

    logging.debug("Fetched clustering status complete!")
    return clustering_information


def execute_recluster(snowflake_connection, table_details):
    """
    For the provided table, execute the reclustering command.

    :param snowflake_connection: The row specific Snowflake connection
    :param table_details: The table requiring reclustering. This is a {@link ClusterRow} object
    :return: None
    """
    try:
        cursor = snowflake_connection.cursor()
        cursor.execute("use warehouse " + str(table_details.warehouse_override))
        cursor.execute("use " + str(table_details.db_name) + "." + str(table_details.schema_name))
        cursor.execute("alter table " + str(table_details.table_name) + " recluster", _no_results=True)
    except Exception as e:
        logging.error("Unable to run recluster for (ID: " + table_details.id + ")!!!")
    return None


def register_job_run(metadata_connection, clustering_information_json, table_details):
    """
    Add a corresponding row to the Oracle CISCO_AUTO_RECLUSTER_HISTORY table indicating that the specified table
    has been clustered.

    :param metadata_connection: The connection to the metadata database
    :param clustering_information_json: The complete json dump provided from Snowflake via the $clustering info command.
    :param table_details: The {@link ClusterRow} object containing the metadata details.
    :return: None
    """
    cursor = metadata_connection.cursor()

    insert_registration_sql = util.read_file('../bin/metadata/DML/INSERT_CLUSTERING_REGISTRATION.sql')
    cursor.execute(insert_registration_sql, (str(table_details.id)
                                             , str(table_details.driving_mech)
                                             , str(table_details.total_constant_partition_count)
                                             , str(table_details.average_depth)
                                             , str(table_details.other_driver)
                                             , str(table_details.size_override)
                                             , str(table_details.warehouse_override)
                                             , str(json.dumps(clustering_information_json))))
    cursor.execute("commit")

    return None