import metadata.metadata_handler as metadata_handler
from models.ConnectionOracleObject import ConnectionOracleObject
from models.ConnectionSnowflakeObject import ConnectionSnowflakeObject
import utils.util as util
import logging

# Short Term

# Long Term:

if __name__ == "__main__":
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

    # Truncate the current reconciliation records
    metadata_handler.truncate_reconciliation_records(metadata_connection)

    # Connect to Snowflake
    snowflake_connection_details = ConnectionSnowflakeObject(util.collect_property_file_contents('../properties/config.ini', 'CISCO.US-EAST-1'))
    snowflake_connection = metadata_handler.open_database_connection(snowflake_connection_details)

    # Collect all tables which have clustering keys defined in Snowflake
    tables_requiring_reconciliation = metadata_handler.collect_tables_for_reconciliation(snowflake_connection)
    metadata_handler.insert_reconciliation_records(metadata_connection, tables_requiring_reconciliation)

    # Perform Merge of reconciliation records to the driving table
    metadata_handler.merge_reconciliation_records(metadata_connection)

    # Change to not auto cluster
    metadata_handler.suspend_auto_recluster(snowflake_connection, tables_requiring_reconciliation)

    metadata_handler.close_connection(snowflake_connection)
    metadata_handler.close_connection(metadata_connection)
    logging.info("Processing for all tables fetched COMPLETE!")
