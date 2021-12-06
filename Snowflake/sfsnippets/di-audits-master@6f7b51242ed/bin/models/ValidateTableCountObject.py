class ValidateTableCountObject(object):
    """
    A ValidateTableCountObject will provide a boolean 'match' to express if the dataframes contain similar data.

    :param object - The comparision object
    """

    def __init__(self, SOURCE_COUNT, TARGET_COUNT):
        """
        Identify if the count matches.

        :param SOURCE_COUNT: The source table count as a single column, row dataframe
        :param TARGET_COUNT: The target table count as a single column, row dataframe
        """
        match = False
        percent_of_target_to_source = (TARGET_COUNT.values[0] / SOURCE_COUNT.values[0]) * 100

        if int(percent_of_target_to_source == 100):
            match = True

        self.match = match
        self.source_count = SOURCE_COUNT
        self.target_count = TARGET_COUNT
        self.percent_of_target_to_source = percent_of_target_to_source
