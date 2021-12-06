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


def read_file(file_path):
    """
    Open and collect the file contents at the specified path.

    :param file_path: duh.
    :return: String of the file contents
    """
    try:
        with open(file_path, 'r') as CONTENT:
            return CONTENT.read()
    except Exception as e:
        print('ERROR: Unable to open and collect file contents for (File: ' + file_path + ')')
        print('ERROR: ' + str(e))


def clear_file(file_path, data):
    """
    Open the file and clear the contents. Place data inside if not null.

    :param file_path: The path of the file to write
    :param data: The content to be written.
    :return: None
    """
    try:
        f = open(file_path, "w")
        f.write(data)
        f.close()
    except Exception as e:
        print('ERROR: Unable to open and clear file contents for (File: ' + file_path + ')')
        print('ERROR: ' + str(e))

    return None


def write_file(file_path, data):
    """
    Open the file and append the contents of data

    :param file_path: The path of the file to write
    :param data: The content to be written.
    """
    try:
        f = open(file_path, "a")
        f.write(data)
        f.close()
    except Exception as e:
        print('ERROR: Unable to open and write file contents for (File: ' + file_path + ')')
        print('ERROR: ' + str(e))

    return None
