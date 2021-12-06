class ClusteringInformation(object):
    """
    The ClusteringInformation object will contain all the pertinent details from the Snowflake $clustering command.
    """
    def __init__(self, response):
        """
        Documentation:
        https://docs.snowflake.net/manuals/sql-reference/functions/system_clustering_information.html

        : cluster_by_keys - This is the chosen clustering key. It is a selection of column(s) in a table.
        : total_constant_partition_count - Total number of micro-partitions for which the value of the specified columns have reached a constant state.
        : total_partition_count - Total number of micro-partitions that comprise the table.
        : average_overlaps - Average number of overlapping micro-partitions for each micro-partition in the table
        : average_depth - Average overlap depth of each micro-partition in the table.
        : partition_depth_histogram - A histogram depicting the distribution of overlap depth for each micro-partition in the table.

        :param response: The complete object from Snowflake
        """
        if 'cluster_by_keys' in response:
            self.cluster_by_keys = response['cluster_by_keys']

        if 'total_constant_partition_count' in response:
            self.total_constant_partition_count = response['total_constant_partition_count']
            if self.total_constant_partition_count == 0:
                self.total_constant_partition_count = .01

        if 'total_partition_count' in response:
            self.total_partition_count = response['total_partition_count']
            if self.total_partition_count == 0:
                self.total_partition_count = .01

        if 'average_overlaps' in response:
            self.average_overlaps = response['average_overlaps']
            if self.average_overlaps == 0:
                self.average_overlaps = .01

        if 'average_depth' in response:
            self.average_depth = response['average_depth']
            if self.average_depth == 0:
                self.average_depth = .01

        if 'partition_depth_histogram' in response:
            self.partition_depth_histogram = response['partition_depth_histogram']
