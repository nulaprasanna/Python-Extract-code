class ConnectionSnowflakeObject(object):
    """
    A ConnectionSnowflakeObject  will contain all required fields to establish a Snowflake connection.

    :param object - The Snowflake connection details
    """

    def __init__(self, response):
        """
        """
        self.system_type_name = 'SNOWFLAKE'
        if 'CONNECTING_ROLE' in response:
            self.connecting_role = response['CONNECTING_ROLE']

        if 'CONNECTING_WAREHOUSE' in response:
            self.connecting_warehouse = response['CONNECTING_WAREHOUSE']

        if 'CONNECTING_DATABASE' in response:
            self.connecting_database = response['CONNECTING_DATABASE']

        if 'CONNECTING_SCHEMA' in response:
            self.connecting_schema = response['CONNECTING_SCHEMA']

        if 'QUERY_HISTORY_DATABASE' in response:
            self.query_history_database = response['QUERY_HISTORY_DATABASE']

        if 'QUERY_HISTORY_SCHEMA' in response:
            self.query_history_schema = response['QUERY_HISTORY_SCHEMA']

        if 'PRIVATE_KEY_FILENAME' in response:
            self.private_key_filename = response['PRIVATE_KEY_FILENAME']

        if 'PRIVATE_KEY_PASSPHRASE' in response:
            self.private_key_passphrase = response['PRIVATE_KEY_PASSPHRASE']

        if 'CONNECTING_USER' in response:
            self.connecting_user = response['CONNECTING_USER']

        if 'ACCOUNT' in response:
            self.account = response['ACCOUNT']

        if 'SHOW_WAREHOUSE_DELAY' in response:
            self.show_warehouse_delay = int(response['SHOW_WAREHOUSE_DELAY'])

        if 'CREDIT_USAGE_DELAY' in response:
            self.credit_usage_delay = int(response['CREDIT_USAGE_DELAY'])

        if 'KEEPER_URI' in response:
            self.keeper_uri = response['KEEPER_URI']

        if 'KEEPER_NAMESPACE' in response:
            self.keeper_namespace = response['KEEPER_NAMESPACE']

        if 'KEEPER_TOKEN' in response:
            self.keeper_token = response['KEEPER_TOKEN']

        if 'KEEPER_SECRET_PATH' in response:
            self.secret_path = response['KEEPER_SECRET_PATH']
