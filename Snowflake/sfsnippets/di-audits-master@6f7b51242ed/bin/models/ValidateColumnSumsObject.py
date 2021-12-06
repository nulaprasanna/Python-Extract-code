import pandas as pd


class ValidateColumnSumsObject(object):
    """
    A ValidateColumnSums will provide a boolean 'match' to express if the dataframes contain similar data.
    TODO: As the values could be presented within any order inside the dataframe, a merge comparision would be better.
    TODO: This implementation assumes the column order comparision is identical.

    :param object - The comparision object
    """

    def __init__(self, SOURCE_COLUMN_SUMS, TARGET_COLUMN_SUMS):
        """
        Identify if the aggregates match.

        :param SOURCE_COLUMN_SUMS: A dataframe of 1 row consisting of column sums
        :param TARGET_COLUMN_SUMS: A dataframe of 1 row consisting of column sums in the order of SOURCE_COLUMN_SUMS
        """
        if pd.DataFrame.equals(SOURCE_COLUMN_SUMS, TARGET_COLUMN_SUMS):
            match = True
        else:
            match = False

        self.source_sum = SOURCE_COLUMN_SUMS
        self.target_sum = TARGET_COLUMN_SUMS
        self.match = match
