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

        if 'CONNECTING_USER' in response:
            self.connecting_user = response['CONNECTING_USER']

        if 'ACCOUNT' in response:
            self.account = response['ACCOUNT']

        if 'KEEPER_URI' in response:
            self.keeper_uri = response['KEEPER_URI']

        if 'KEEPER_NAMESPACE' in response:
            self.keeper_namespace = response['KEEPER_NAMESPACE']

        if 'KEEPER_TOKEN' in response:
            self.keeper_token = response['KEEPER_TOKEN']

        if 'KEEPER_SECRET_PATH' in response:
            self.secret_path = response['KEEPER_SECRET_PATH']

        if 'CONNECTION_RETRY_ATTEMPTS' in response:
            self.connection_retry_attempts = response['CONNECTION_RETRY_ATTEMPTS']
