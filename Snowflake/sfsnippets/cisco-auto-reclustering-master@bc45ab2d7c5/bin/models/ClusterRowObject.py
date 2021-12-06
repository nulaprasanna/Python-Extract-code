class ClusterRow(object):
    """
    The ClusterRow object corresponds to the schema provided in the Oracle CISCO_AUTO_RECLUSTER table.
    """
    def __init__(self, id, account, db_name, schema_name, table_name, run_schedule, driving_mech
                 , total_constant_partition_count, average_depth, other_driver, max_runs, size_override
                 , warehouse_override, active_flag, group_name, passphrase):
        """
        :param id: Primary key
        :param account: Snowflake Account E.G. CISCODEV.US-EAST-1
        :param db_name: Database Name of object to cluster
        :param schema_name: Schema name of object to cluster
        :param table_name: Table name of object to cluster
        :param run_schedule: How often to run. Code. (E.G. Daily/Hourly/Weekly/Mondays...)
        :param driving_mech: Basis on which to choose to recluster. (E.G. TCPC/AVG_DEPTH/BOTH) Can be a value in $clustering info
        :param total_constant_partition_count: $ Clustering information TCPC
        :param average_depth: $ Clustering information average depth
        :param other_driver: $ Clustering information json key element
        :param max_runs: The maximum number of runs to expecute in a day
        :param size_override: The executing warehosue will be resized to this value.
        :param warehouse_override: Redirects the execution to be against this warehouse.
        :param active_flag: YES or NO. YES means to consider for reclustering.
        :param group_name: A group of tables for which to execute
        :param passphrase: A passphrase controls execution when provided. This is intended for middleware execution.
        """
        self.id = id
        self.account = account
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.run_schedule = run_schedule
        self.driving_mech = driving_mech
        self.total_constant_partition_count = total_constant_partition_count
        self.average_depth = average_depth
        self.other_driver = other_driver
        self.max_runs = max_runs
        self.size_override = size_override
        self.warehouse_override = warehouse_override
        self.active_flag = active_flag
        self.group_name = group_name
        self.passphrase = passphrase
