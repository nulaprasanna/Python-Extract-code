import pandas as pd
import logging
from metadata import metadata_handler
from models.ConnectionOracleObject import ConnectionOracleObject
from models.ConnectionTeradataObject import ConnectionTeradataObject
from models.TableInputObject import TableInputObject
from models.ValidateColumnSumsObject import ValidateColumnSumsObject
from models.ValidateRecordMatchObject import ValidateRecordMatchObject
from models.ValidateTableCountObject import ValidateTableCountObject
from models.ValidateTableUniqueKeyObject import ValidateTableUniqueKeyObject
from utils.util import collect_property_file_contents

# Get Properties and set Logging if desired
from metadata.metadata_handler import collect_table_record_count
from models.ConnectionSnowflakeObject import ConnectionSnowflakeObject

properties = collect_property_file_contents('../properties/config.ini', 'CONFIG')
logging.basicConfig(level=logging.INFO, filename="../bin/logs/output.log")

# Accept User Request
td_table = TableInputObject("DV3", "TDDEV", "WORKDB_DV3", "DI_AUDIT_TEST")
sf_table = TableInputObject("DV3", "EDW_OPS_ETL_DB_DV3", "SS", "DI_AUDIT_TEST")
oracle_table = TableInputObject("PRD", "ODSPROD", "DATALAKE", "DI_AUDIT_TEST")

# Connect to Snowflake
sf_dev_properties = collect_property_file_contents('../properties/config.ini', 'CISCODEV.US-EAST-1')
snowflake_connection_details = ConnectionSnowflakeObject(sf_dev_properties["ACCOUNT_NAME"]
                                                         , sf_dev_properties["CONNECTING_USER"]
                                                         , sf_dev_properties["CONNECTING_WAREHOUSE"]
                                                         , sf_dev_properties["CONNECTING_ROLE"]
                                                         , sf_dev_properties["CONNECTING_DATABASE"]
                                                         , sf_dev_properties["CONNECTING_SCHEMA"]
                                                         , sf_dev_properties["KEEPER_TOKEN"]
                                                         , sf_dev_properties["SECRET_PATH"]
                                                         , sf_dev_properties["KEEPER_URI"])
snowflake_connection = metadata_handler.open_database_connection(snowflake_connection_details)

# Connect to Oracle
oracle_connection_details = ConnectionOracleObject("ODSPROD.CISCO.COM", "DATALAKE_RO", properties["ORACLE_PASSWORD"])
oracle_connection = metadata_handler.open_database_connection(oracle_connection_details)

# Connect to Teradata
#  , authentication="LDAP" on udaexec connect
td_connection_details = ConnectionTeradataObject('TDDEV', 'algarbar', properties["TERADATA_PASSWORD"])
teradata_connection = metadata_handler.open_database_connection(td_connection_details)

# Collect info
oracle_record_count = collect_table_record_count(oracle_connection, oracle_table)
sf_record_count = collect_table_record_count(snowflake_connection, sf_table)
td_record_count = collect_table_record_count(teradata_connection, td_table)

compare_column_list = pd.DataFrame({"COLUMN_NAME": ['ID', 'NUM_VAL']})
oracle_columns_sum = metadata_handler.collect_sum_of_columns(oracle_connection, oracle_table, compare_column_list)
sf_columns_sum = metadata_handler.collect_sum_of_columns(snowflake_connection, sf_table, compare_column_list)
td_columns_sum = metadata_handler.collect_sum_of_columns(teradata_connection, td_table, compare_column_list)

td_records = metadata_handler.collect_a_random_set_of_records(teradata_connection, td_table, 0)
sf_records = metadata_handler.collect_a_random_set_of_records(snowflake_connection, sf_table, 0)
oracle_records = metadata_handler.collect_a_random_set_of_records(oracle_connection, oracle_table, 0)

table_unique_key = pd.DataFrame({"COLUMN_NAME": ['ID']})
oracle_validate_key_results_match = ValidateTableUniqueKeyObject(metadata_handler.validate_natural_key(oracle_connection, oracle_table, table_unique_key))
teradata_validate_key_results_match = ValidateTableUniqueKeyObject(metadata_handler.validate_natural_key(teradata_connection, td_table, table_unique_key))
snowflake_validate_key_results_match = ValidateTableUniqueKeyObject(metadata_handler.validate_natural_key(snowflake_connection, sf_table, table_unique_key))

# Close Connections
metadata_handler.close_connection(snowflake_connection)
metadata_handler.close_connection(oracle_connection)
metadata_handler.close_connection(teradata_connection)

# Do stuff with output
validate_table_counts_match = ValidateTableCountObject(td_record_count, oracle_record_count)
validate_column_comparision = ValidateColumnSumsObject(td_columns_sum, oracle_columns_sum)
validate_random_records_exactly_match = ValidateRecordMatchObject(td_records, oracle_records)

print('-----UNIQUE KEY CHECKS-----')
print("Oracle: " + str(oracle_validate_key_results_match.match))
print("TD: " + str(teradata_validate_key_results_match.match))
print("Snowflake: " + str(snowflake_validate_key_results_match.match))

print('\n\n-----RECORD COUNT CHECKS-----')
print('match:' + str(validate_table_counts_match.match)
      + "\nsource count: "
      + str(validate_table_counts_match.source_count)
      + "\ntarget count: " + str(validate_table_counts_match.target_count))

print('\n\n-----COLUMN VALUE CHECKS-----')
print("match: " + str(validate_column_comparision.match))
print('Source Sum:' + str(validate_column_comparision.source_sum))
print('Target Sum:' + str(validate_column_comparision.target_sum))

print('\n\n-----DATA VALUE CHECKS-----')
print('match: ' + str(validate_random_records_exactly_match.match))
print('source records: ' + str(validate_random_records_exactly_match.source_records))
print('target records: ' + str(validate_random_records_exactly_match.target_records))
