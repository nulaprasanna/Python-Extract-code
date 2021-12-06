import pandas as pd
import logging
from metadata import metadata_handler
from models.ConnectionOracleObject import ConnectionOracleObject
from models.TableInputObject import TableInputObject
from models.ValidateColumnSumsObject import ValidateColumnSumsObject
from models.ValidateRecordMatchObject import ValidateRecordMatchObject
from models.ValidateTableCountObject import ValidateTableCountObject
from models.ValidateTableUniqueKeyObject import ValidateTableUniqueKeyObject
from utils.util import collect_property_file_contents

# Get Properties and set Logging if desired
properties = collect_property_file_contents('../properties/config.ini', 'CONFIG')
logging.basicConfig(level=logging.INFO, filename="../bin/logs/output.log")

# Accept User Request
request_object = TableInputObject("PRD", "TDPROD", "COMREFVWDB", "N_ISO_COUNTRY")

# Establish a connection to the EDS_DATA_CATALOG for metadata
metadata_connection_details = ConnectionOracleObject(properties["METADATA_CONNECTION_STRING"]
                                                     , properties["METADATA_USERNAME"]
                                                     , properties["METADATA_PASSWORD"])
metadata_connection = metadata_handler.open_database_connection(metadata_connection_details)

# Collect the requested table's metadata from the metadata repo
table_connection_details = metadata_handler.collect_database_connection_details(metadata_connection, request_object, properties)

# Collect the requested table's unique key from the metadata repo
table_unique_key = metadata_handler.collect_table_key_columns(metadata_connection, request_object)

# Close the metadata connection
metadata_handler.close_connection(metadata_connection)

# Establish a connection to the source using the collected metadata
source_connection = metadata_handler.open_database_connection(metadata_handler.create_connection_object(table_connection_details))

# Collect some random records from the requested table
random_records = metadata_handler.collect_a_random_set_of_records(source_connection, request_object, 2000)

# Collect the same records from the table to compare (usually would be a different source but same table)
record_filter = str(metadata_handler.collect_similar_data_to_input(source_connection, request_object, random_records, table_unique_key))
same_selected_records = metadata_handler.select_particular_records(source_connection, request_object, record_filter)

# Collect the Table count
table_count = metadata_handler.collect_table_record_count(source_connection, request_object)

# Create a table aggregate for comparision
pd_column_list = pd.DataFrame({"COLUMN_NAME": ['EXTRACT(hour from EDW_UPDATE_DATETIME)']})
columns_sum = metadata_handler.collect_sum_of_columns(source_connection, request_object, pd_column_list)

# Determine if the key is valid
validate_key_results_match = ValidateTableUniqueKeyObject(metadata_handler.validate_natural_key(source_connection, request_object, table_unique_key))

# Determine if random records match
validate_random_records_exactly_match = ValidateRecordMatchObject(random_records, same_selected_records)

# Determine if counts match (usually would be a different source but same table)
validate_table_counts_match = ValidateTableCountObject(table_count, table_count)

# Determine if the sum aggregates match (usually would be a different source but same table)
validate_column_comparision = ValidateColumnSumsObject(columns_sum, columns_sum)

# Close the connection to the source system
metadata_handler.close_connection(source_connection)

# Print the results
print("Is the Natural key valid: " + str(validate_key_results_match.match))
print("Do Counts match: " + str(validate_table_counts_match.match))
print("Do sums of the columns match: " + str(validate_column_comparision.match))
print("Does a subset of random data match " + str(validate_random_records_exactly_match.match))
