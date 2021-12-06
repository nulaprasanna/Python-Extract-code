import utils.util as utils
from ctk_database.bin.models.DatabaseSystems import OracleDatabase as oracle
import os

# Apply me to systems which do not have Oracle handy.
# os.environ['ORACLE_HOME'] = "/usr/cisco/packages/oracle/current"
# os.environ['LD_LIBRARY_PATH'] = "/usr/cisco/packages/oracle/current/lib:/usr/cisco/packages/oracle/current/lib:$LD_LIBRARY_PATH"
# os.environ['PATH'] = "${ORACLE_HOME}/bin:$PATH"


def fetch_historical_data(filepath_for_output_data_file):
    """
    Connect to oracle and fetch the historical data to be used for model creation

    :param filepath_for_output_data_file: Filepath to which collected data will be written
    :return: None
    """
    sql_query = utils.read_file("../metadata/sql/DML/select_data_for_model_build.sql")

    source_database_name = 'ODSPROD-EJCRO'
    source_database_properties = utils.collect_property_file_contents('../../properties/config.ini', source_database_name)
    database = oracle.OracleDatabase(source_database_name
                                     , source_database_properties['system_type']
                                     , source_database_properties['lifecycle']
                                     , user_name=source_database_properties['user_name']
                                     , password=source_database_properties['password']
                                     , is_auto_commit=False
                                     , connect_string=source_database_properties['connect_string'])
    database_connection = database.connect()

    model_data = database.query(database_connection, sql_query).results
    database.download_data(model_data, filepath_for_output_data_file, download_type='csv', sep=',', index=False, encoding='utf-8')

    # TODO: replace with add proper logging
    print('\n*******Dataset pulled from ODSPROD into File********')
