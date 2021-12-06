class ValidateTableUniqueKeyObject(object):
    """
    A ValidateTableUniqueKeyObject will provide a boolean 'match' to express if duplicates were detected.

    A match == True means no duplicates

    :param object - The comparision object
    """

    def __init__(self, DUPLICATES_DETECTED):
        """
        Identify if duplicates are detected

        :param DUPLICATES_DETECTED: The number of duplicates found
        """
        match = False
        duplicates = int(DUPLICATES_DETECTED.values[0])

        if duplicates == 0:
            match = True

        self.match = match
        self.duplicates = duplicates
