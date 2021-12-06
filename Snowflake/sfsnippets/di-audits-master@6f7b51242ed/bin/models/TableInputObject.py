class TableInputObject(object):
    """
    A TableInputObject will contain all required fields to identify a particular table from across all databases

    *** Consult the EDS_DATA_CATALOG ERD ***

    :param object - The identified table
    """

    def __init__(self, ENVIRONMENT_NAME, DB_INSTANCE_NAME, DB_SCHEMA_NAME, TABLE_NAME):
        """
        :param ENVIRONMENT_NAME: The environment lifecycle. EG: DV1,DV3,PRD
        :param DB_INSTANCE_NAME: The database identifier. EG: TDPROD, ODSPROD
        :param DB_SCHEMA_NAME: The Database's schema. EG: REFADM, BR, SS, COMREFVWDB
        :param TABLE_NAME: The table name. Duh.
        """
        self.env_name = ENVIRONMENT_NAME
        self.db_instance_name = DB_INSTANCE_NAME
        self.db_schema_name = DB_SCHEMA_NAME
        self.table_name = TABLE_NAME
