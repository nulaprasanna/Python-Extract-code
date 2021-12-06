class ConnectionOracleObject(object):
    """
    A ConnectionOracleObject will contain all required fields to establish an Oracle connection.

    :param object - The Oracle connection details
    """

    def __init__(self, CONNECTION_STRING, USERNAME, PASSWORD):
        """
        :param CONNECTION_STRING: The SID or full descriptior of the DB. EG: ODSPROD
        :param USERNAME: The schema of the login user
        :param PASSWORD: The password used for the connecting schema
        """
        self.system_type_name = 'ORACLE'
        self.connection_string = CONNECTION_STRING
        self.username = USERNAME
        self.password = PASSWORD
