import logging
import pandas as pd


class ValidateRecordMatchObject(object):
    """
    A ValidateRecordMatchObject will provide a boolean 'match' to express if the dataframes contain similar data.

    :param object - The comparision object
    """

    def __init__(self, SOURCE_RECORDS, TARGET_RECPRDS):
        """
        Identify if the dataframe records match.

        :param SOURCE_RECORDS: A set of records from the source system.
        :param TARGET_RECPRDS: A set of records from the target system. These should be the same records as the source in any order.
        """
        match = False
        if str(SOURCE_RECORDS.columns.values) == str(TARGET_RECPRDS.columns.values):
            merged_values = pd.merge(SOURCE_RECORDS, TARGET_RECPRDS, how="left", on=list(TARGET_RECPRDS.columns.values))

            if len(merged_values) == len(SOURCE_RECORDS) and len(merged_values) == len(TARGET_RECPRDS):
                match = True
            else:
                logging.debug("The records did not match due to differences between the data.")
        else:
            logging.debug("The records did not match due to differences between the columns.")

        self.source_records = SOURCE_RECORDS
        self.target_records = TARGET_RECPRDS
        self.match = match
