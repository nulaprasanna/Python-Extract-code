import metadata.metadata_handler as metadata_handler
from models.ClusterRowObject import ClusterRow
from models.ClusteringInformationObject import ClusteringInformation
from models.ConnectionOracleObject import ConnectionOracleObject
from models.ConnectionSnowflakeObject import ConnectionSnowflakeObject
import utils.util as util
import logging
import argparse

# Short Term
# TODO: Add test cases
# TODO: Create an API to trigger manual run

# Long Term:
# TODO: Dynamically determine warehouse on which to run
# TODO: Check that max runs are not breached
# TODO: Implement Size Override with Zin's tool
# TODO: Ensure that table is not actively being reclustered
# TODO: Check if hit max multiple days in a row
# TODO: Develop a reconciliation method to find tables which need to enter the system


def create_table_profile(clustering_information, clustering_table):
    """
    Users may be interested to understand the effects of executing the program without actually running the recluster
    command. This method collects and writes the relevant data to the profile output specified in config.ini [GENERAL]

    :param clustering_information: The results of the $ Clustering Information from SF as a {@link ClusteringInformation} object.
    :param clustering_table: The {@link ClusterRow} object to be clustered.
    :return: None. Output is written to a file.
    """
    data = str(clustering_table.id)
    data += "," + str(clustering_table.group_name)
    data += "," + str(clustering_table.account)
    data += "," + str(clustering_table.db_name)
    data += "," + str(clustering_table.schema_name)
    data += "," + str(clustering_table.table_name)
    data += "," + str(clustering_table.driving_mech)
    data += "," + str(clustering_table.total_constant_partition_count)
    data += "," + str(clustering_table.average_depth)
    data += "," + str(clustering_table.other_driver)
    data += "," + str(clustering_table.total_constant_partition_count / 100 * (1 / clustering_table.average_depth)) # This is what is set for BOTH
    data += "," + str(clustering_information.total_partition_count)
    data += "," + str((clustering_information.total_constant_partition_count / clustering_information.total_partition_count * 100)) # TCPC
    data += "," + str(clustering_information.average_depth)
    data += "," + str((clustering_information.total_constant_partition_count / clustering_information.total_partition_count) * (1 / clustering_information.average_depth)) # BOTH Calculation
    data += "," + str(check_if_clustering_required(clustering_information, clustering_table))
    data += "\n"
    util.write_file(general_config_settings['PROFILE_FILE_PATH'], data)

    return None


def check_if_clustering_required(clustering_information, clustering_table):
    """
    This method compares the results of the $clustering information output against the user required fields. If the
    user's required clustering demands are not met, clustering will be passed as True.

    :param clustering_information: The results of the $ Clustering Information from SF as a {@link ClusteringInformation} object.
    :param clustering_table: The {@link ClusterRow} object to be clustered.
    :return: Boolean True/False.
    """
    is_clustering_required = False

    if clustering_table.driving_mech.upper() == 'TCPC':
        TCPC = (clustering_information.total_constant_partition_count / clustering_information.total_partition_count * 100)
        if TCPC < clustering_table.total_constant_partition_count:
            is_clustering_required = True
    elif clustering_table.driving_mech.upper() == 'AVG_DEPTH':
        if clustering_information.average_depth > clustering_table.average_depth:
            is_clustering_required = True
    elif clustering_table.driving_mech.upper() == 'BOTH':
        TCPC = (clustering_information.total_constant_partition_count / clustering_information.total_partition_count)
        AVG_DEPTH_WEIGHT = (1 / clustering_information.average_depth)
        if TCPC * AVG_DEPTH_WEIGHT < clustering_table.total_constant_partition_count / 100 * (1 / clustering_table.average_depth):
            is_clustering_required = True
    elif hasattr(clustering_information, clustering_table.driving_mech):
        if getattr(clustering_information, clustering_table.driving_mech) > clustering_table.other_driver:
            is_clustering_required = True
    else:
        logging.warning("Invalid Driving Mech found for table (ID: " + clustering_table.id + ")!")

    return is_clustering_required


if __name__ == "__main__":
    # Collect only required user input
    args = argparse.ArgumentParser()
    args.add_argument("--executeType", required=False,
                      help="Enter Execute Type. Execute type includes: PROFILE & CLUSTER. PROFILE will create an output"
                           " file listing the outcome which would happen from type CLUSTER. Default == PROFILE")
    args.add_argument("--group", required=False,
                      help="Enter group for execution. The group refers to the specific GROUP_NAME in "
                           "CISCO_AUTO_RECLUSTER which needs to be executed. All ID values in this group are eligible.")
    args.add_argument("--id", required=False,
                      help="Enter id for execution. The id refers to the specific id in CISCO_AUTO_RECLUSTER which"
                           " needs to be executed. If an ID value is provided the provided group is ignored.")
    args.add_argument("--passphrase", required=False,
                      help="Enter the passphrase for execution verification. The passphrase, when provided, is "
                           "required to match the assigned value. This is intended for middleware execution, only.")

    group = vars(args.parse_args())["group"]
    executeType = vars(args.parse_args())["executeType"]
    id = vars(args.parse_args())["id"]
    passphrase = vars(args.parse_args())["passphrase"]

    # create loggers and apply settings from config.ini [GENERAL]
    general_config_settings = util.collect_property_file_contents('../properties/config.ini', 'GENERAL')
    logging.basicConfig(level=getattr(logging, general_config_settings['GENERAL_LOGGING_LEVEL']), filename=general_config_settings['LOG_FILE_PATH'], format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    for logger_name in ['snowflake.connector', 'botocore', 'boto3']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, general_config_settings['SNOWFLAKE_LOGGING_LEVEL']))

    # Connect to the metadata database
    metadata_DB = general_config_settings['METADATA_DATABASE']
    metadata_connection_details = util.collect_property_file_contents('../properties/config.ini', metadata_DB)
    metadata_connection_details = ConnectionOracleObject(metadata_connection_details['connection_string']
                                                         , metadata_connection_details['username']
                                                         , metadata_connection_details['password'])
    metadata_connection = metadata_handler.open_database_connection(metadata_connection_details)

    # Collect the tables to process
    table_list = metadata_handler.collect_active_recluster_list(metadata_connection, group, id)
    logging.info("Processing all tables fetched.")

    # TODO: Implement parallel processing for all the below if the number of tables exceeds a certain amount. 1000+
    util.clear_file(general_config_settings['PROFILE_FILE_PATH'], "ID,GROUP_NAME,ACCOUNT,DATABASE,SCHEMA,TABLE,DRIVING MECHANISM,SET TCPC,SET AVG DEPTH,OTHER_DRIVER, SET BOTH CALCULATION,ACTUAL TOTAL PARTITIONS,ACTUAL TCPC, ACTUAL AVERAGE DEPTH,ACTUAL BOTH CALCULATION,WOULD BE CLUSTERED\n")
    # For each table which may require re-clustering, make the determination and perform the action
    for row in range(0, len(table_list)):
        table_details = ClusterRow(table_list['ID'].values[row]
                                   , table_list['ACCOUNT'].values[row]
                                   , table_list['DB_NAME'].values[row]
                                   , table_list['SCHEMA_NAME'].values[row]
                                   , table_list['TABLE_NAME'].values[row]
                                   , table_list['RUN_SCHEDULE'].values[row]
                                   , table_list['DRIVING_MECH'].values[row]
                                   , table_list['TCPC'].values[row]
                                   , table_list['AVG_DEPTH'].values[row]
                                   , table_list['OTHER_DRIVER'].values[row]
                                   , table_list['MAX_RUNS'].values[row]
                                   , table_list['SIZE_OVERRIDE'].values[row]
                                   , table_list['WAREHOUSE_OVERRIDE'].values[row]
                                   , table_list['ACTIVE_FLAG'].values[row]
                                   , table_list['GROUP_NAME'].values[row]
                                   , table_list['PASSPHRASE'].values[row])

        qualified_table_name = table_details.db_name + '.' + table_details.schema_name + '.' + table_details.table_name

        logging.debug("Processing table (" + qualified_table_name + ").")

        # Connect to Snowflake and perform the $ clustering information
        snowflake_connection_details = ConnectionSnowflakeObject(util.collect_property_file_contents('../properties/config.ini', table_details.account))
        snowflake_connection = metadata_handler.open_database_connection(snowflake_connection_details)
        clustering_information_json = metadata_handler.collect_clustering_information(snowflake_connection, qualified_table_name)

        if clustering_information_json is None:
            logging.warning("Table (" + str(qualified_table_name) + ") is not clustered or does not exist. Thus, no action will be taken.")
            print("Table (" + str(qualified_table_name) + ") is not clustered or does not exist. Thus, no action will be taken.")
        else:
            clustering_information = ClusteringInformation(clustering_information_json)
            logging.debug("Profiling table (" + qualified_table_name + ") complete.")
            create_table_profile(clustering_information, table_details)

            # Check if the collected data indicates clustering is required
            if check_if_clustering_required(clustering_information, table_details) and executeType == "CLUSTER":
                if passphrase and passphrase != table_details.passphrase:
                    logging.warning("The provided passphrase does not match for table id: " + str(table_details.id) + ". Thus, skipping.")
                    print("The provided passphrase does not match for table (" + str(table_details.table_name)
                          + ") id: " + str(table_details.id) + ". Thus, skipping.")
                else:
                    print('Cluster - \t\t\t' + str(table_details.table_name))
                    metadata_handler.execute_recluster(snowflake_connection, table_details)
                    logging.debug("Clustering table (" + qualified_table_name + ") started.")
                    logging.debug("Registering Job run into History table for (" + qualified_table_name + ")!")
                    metadata_handler.register_job_run(metadata_connection, clustering_information_json, table_details)
                    logging.debug("Finished registration for (" + qualified_table_name + ")!")
            elif executeType == "CLUSTER":
                logging.debug("No clustering for table (" + qualified_table_name + ") required.")
                print('No Cluster - \t\t' + str(table_details.table_name))

            metadata_handler.close_connection(snowflake_connection)

    metadata_handler.close_connection(metadata_connection)
    logging.info("Processing for all tables fetched COMPLETE!")
