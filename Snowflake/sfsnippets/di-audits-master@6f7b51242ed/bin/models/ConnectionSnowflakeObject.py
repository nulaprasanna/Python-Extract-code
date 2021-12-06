class ConnectionSnowflakeObject(object):
    """
    A ConnectionSnowflakeObject  will contain all required fields to establish a Snowflake connection.

    :param object - The Snowflake connection details
    """

    def __init__(self, ACCOUNT, CONNECTING_USER, CONNECTING_WAREHOUSE, CONNECTING_ROLE, CONNECTING_DATABASE, CONNECTING_SCHEMA, KEEPER_TOKEN, SECRET_PATH, KEEPER_URI):
        """
        :param ACCOUNT: The Snowflake account name. EG: ciscodev.us-east-1
        :param CONNECTING_USER: The connecting username passed to Snowflake. EG: EDW_XXX_SVC
        :param CONNECTING_WAREHOUSE: The connecting warehouse name passed to Snowflake. EG. EDW_I_XXX_WH
        :param CONNECTING_ROLE: The operating role to be used on connection
        :param CONNECTING_DATABASE: The operating database to be auto aliased on connection
        :param CONNECTING_SCHEMA: The operating schema to be auto aliased on connection
        :param KEEPER_TOKEN: The token to authorize a user's access to a Keeper secret
        :param SECRET_PATH: The location within keeper to a secret
        :param KEEPER_URI: The API URL (not URI) for accessing a Keeper instance. EG:https://alphaeast.keeper.cisco.com/
        """
        self.system_type_name = 'SNOWFLAKE'
        self.account = ACCOUNT
        self.connecting_user = CONNECTING_USER
        self.connecting_warehouse = CONNECTING_WAREHOUSE
        self.connecting_role = CONNECTING_ROLE
        self.connecting_database = CONNECTING_DATABASE
        self.connecting_schema = CONNECTING_SCHEMA
        self.keeper_token = KEEPER_TOKEN
        self.secret_path = SECRET_PATH
        self.keeper_uri = KEEPER_URI
        self.keeper_namespace = 'cloudDB'
