import pandas as pd
import utils.util as utils
import logging
import argparse
from models.ConnectionSnowflakeObject import ConnectionSnowflakeObject
from metadata import metadata_handler


def get_sample_data(filename):
    """For testing, can provide sample input data """
    results = pd.read_csv(filename)
    return results


if __name__ == "__main__":

    # Collect Snowflake account for which to run
    args = argparse.ArgumentParser()
    args.add_argument("--account", required=True, help="Enter the Snowflake account. E.G. CISCO.US-EAST-1")
    account = vars(args.parse_args())["account"]

    # create loggers and apply settings from config.ini [GENERAL]
    general_config_settings = utils.collect_property_file_contents('properties/config.ini', 'GENERAL')
    snowflake_account_properties = utils.collect_property_file_contents('properties/config.ini', account)
    logging.basicConfig(level=getattr(logging, general_config_settings['GENERAL_LOGGING_LEVEL'])
                        , filename=snowflake_account_properties['LOG_FILE_PATH']
                        , format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    for logger_name in ['snowflake.connector', 'botocore', 'boto3']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, general_config_settings['SNOWFLAKE_LOGGING_LEVEL']))

    snowflake_connection_details = ConnectionSnowflakeObject(snowflake_account_properties)

    # Begin execution for collection of bad views
    logging.info("-----------------------------------")
    logging.info("Beginning fetch of views for Snowflake account: " + str(account))
    snowflake_connection = metadata_handler.open_database_connection(snowflake_connection_details)
    view_list = metadata_handler.fetch_view_list(snowflake_connection)
    # view_list = get_sample_data('bin\\views_to_check.csv')
    results = metadata_handler.check_batch(snowflake_connection, view_list, snowflake_account_properties["TEST_VIEW"])

    metadata_handler.upload_view_list(snowflake_connection, results[1], snowflake_account_properties["BAD_VIEW_TARGET_TABLE"])
    metadata_handler.close_connection(snowflake_connection)
