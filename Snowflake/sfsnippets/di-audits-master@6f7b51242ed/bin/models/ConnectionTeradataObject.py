class ConnectionTeradataObject(object):
    """
    A ConnectionTeradataObject will contain all required fields to establish a Teradata connection.

    :param object - The Teradata connection details
    """

    def __init__(self, SYSTEM, USERNAME, PASSWORD):
        """
        :param SYSTEM: The Teradata instance. EG: TDPROD
        :param USERNAME: The connecting Teradata username
        :param PASSWORD: The password to the connecting Teradata user
        """
        self.system_type_name = 'TERADATA'
        self.system = SYSTEM
        self.username = USERNAME

        # TODO: Identify further occurrences of issue characters in Teradata login
        self.password = PASSWORD.replace("$", "$$")
