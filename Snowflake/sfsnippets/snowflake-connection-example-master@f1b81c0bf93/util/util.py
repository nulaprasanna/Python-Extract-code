import configparser
import pyAesCrypt
import io
import pyAesCrypt

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
    Open and collect the file contents at the specified path in binary mode.

    :param file_path: duh.
    :return: String of the file contents. Returns FILE_READ_ERROR if any error
    """
    try:
        with open(file_path, 'rb') as CONTENT:
            content = CONTENT.read()
    except Exception as e:
        print('ERROR: Unable to open and collect file contents for (File: ' + file_path + ')')
        print('ERROR: ' + str(e))
        content = 'FILE_READ_ERROR'

    return content


def write_file(file_path, data):
    """
    Open the file and append the contents of data in binary mode

    :param file_path: The path of the file to write
    :param data: The content to be written.
    """
    try:
        f = open(file_path, "wb")
        f.write(data)
        f.close()
    except Exception as e:
        print('ERROR: Unable to open and write file contents for (File: ' + file_path + ')')
        print('ERROR: ' + str(e))

    return None


def encrypt_text(password, data_to_encrypt):
    """
    Provided a string text, encrypt the data using pyAesCrypt and return the stream

    :param password: The password from FILE_ENCRYPTION_PASSWORD in config.ini
    :param data_to_encrypt: The passphrase or private key
    :return: cipher stream of passphrase or private key
    """
    data_to_encrypt = bytes(data_to_encrypt, 'utf8')
    buffer_size = 64 * 1024
    data_stream = io.BytesIO(data_to_encrypt)
    cipher_stream = io.BytesIO()
    pyAesCrypt.encryptStream(data_stream, cipher_stream, password, buffer_size)

    return cipher_stream


def decrypt_text(password, encrypted_text):
    """
    Decrypt the contents of the encrypted text and return the result stream

    :param password: The password from FILE_ENCRYPTION_PASSWORD in config.ini
    :param encrypted_text: The already encrypted text we want to decrypt
    :return: decrypted text stream
    """
    buffer_size = 64 * 1024
    decrypt_stream = io.BytesIO()
    cipher_stream = io.BytesIO(encrypted_text)
    cipher_text_length = len(cipher_stream.getvalue())
    cipher_stream.seek(0)
    pyAesCrypt.decryptStream(cipher_stream, decrypt_stream, password, buffer_size, cipher_text_length)

    return decrypt_stream
