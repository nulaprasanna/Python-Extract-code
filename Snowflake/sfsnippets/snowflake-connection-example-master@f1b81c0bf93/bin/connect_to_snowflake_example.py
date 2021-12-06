import connection_handler
import util
from models.ConnectionSnowflakeObject import ConnectionSnowflakeObject

# Reference YOUR config file
PROPERTY_FILE_LOCATION = '../properties/config.ini'

# get general config settings and Snowflake settings
general_settings = util.collect_property_file_contents(PROPERTY_FILE_LOCATION, 'GENERAL')
snowflake_connection_settings = ConnectionSnowflakeObject(util.collect_property_file_contents(PROPERTY_FILE_LOCATION, general_settings["SNOWFLAKE_ACCOUNT"]))

# Check for cached private key & passphrase files - Otherwise create them and get the tuple
passphrase_pkey = connection_handler.collect_keys_for_snowflake(general_settings, snowflake_connection_settings, False)

# Try connecting to Snowflake with the collected details
conn = connection_handler.open_database_connection(general_settings, snowflake_connection_settings)

# Connection failed check
if conn is None:
    # First force collection of new keeper contents. Number of retries is configurable in settings
    connection_handler.collect_keys_for_snowflake(general_settings, snowflake_connection_settings, True)

    # Attempt retry
    for attempt in range(1, int(snowflake_connection_settings.connection_retry_attempts)+1):
        print("retry attempt: #" + str(attempt))
        conn = connection_handler.open_database_connection(general_settings, snowflake_connection_settings)

connection_handler.run_sample_sql(conn)

# Close any open connection
if conn is not None:
    connection_handler.close_connection(conn)
