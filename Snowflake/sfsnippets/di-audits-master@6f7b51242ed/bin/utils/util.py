import configparser

'''
util.py - This collection of methods should be leveraged for any file based interaction.
'''


def collect_property_file_contents(property_file, header):
    """
    Collects and makes available all contents of the property file. This should be identified by 'config.ini'
    :param property_file: The relative path to the property file
    :param header: The identifying bucket of property file contents. EG. [CISCODEV.US-EAST-1], [CONFIG]
    :return: The property file object
    """
    try:
        config = configparser.ConfigParser()
        config.read(property_file)

        configuration = config[header]
        return configuration
    except Exception as e:
        print('ERROR: Unable to open and collect property file contents for (property file: ' + property_file + ', header: ' + header + ')')
        print('ERROR: ' + str(e))


def read_file(filepath):
    """
    Reads and supplies back a string of the file's contents

    :param filepath: The relative path to the file to be parsed
    :return: A str of the file contents
    """
    try:
        with open(filepath, 'r') as SQL_COMMAND:
            return SQL_COMMAND.read()
    except Exception as e:
        print('ERROR: Unable to open and collect file contents for (File: ' + filepath + ')')
        print('ERROR: ' + str(e))
