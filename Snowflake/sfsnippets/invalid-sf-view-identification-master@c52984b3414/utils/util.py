import configparser
from datetime import datetime
from dateutil.tz import tzlocal
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import logging
from email.mime.image import MIMEImage
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import numpy as np

'''
util.py - This collection of methods should be leveraged for any file based interaction.
'''
def collect_property_file_contents(property_file, account_name):
    """
    Collects and makes available all contents of the property file. This should be identified by 'config.ini'

    :param property_file: The relative path to the property file
    :param header: The identifying bucket of property file contents. EG. [CISCODEV.US-EAST-1], [CONFIG]
    :return: The property file object
    """
    def as_dict(config):
        d = dict(config._sections)
        for k in d:
            d[k] = dict(config._defaults, **d[k])
            d[k].pop('__name__', None)
        return d
        
    try:
        config = configparser.ConfigParser()
        config.read(property_file)
        config_dict = as_dict(config)[account_name]
        return config_dict

    except Exception as e:
        logging.info('ERROR: Unable to open and collect property file contents for (property file: ' + property_file + ', account_name: ' + account_name + ')')
        logging.info('ERROR: ' + str(e))
        raise Exception(e)

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
        logging.info('ERROR: Unable to open and collect file contents for (File: ' + file_path + ')')
        logging.info('ERROR: ' + str(e))


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
        logging.info('ERROR: Unable to open and clear file contents for (File: ' + file_path + ')')
        logging.info('ERROR: ' + str(e))

    return None


def append_to_file(file_path, data):
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
        logging.info('ERROR: Unable to open and write file contents for (File: ' + file_path + ')')
        logging.info('ERROR: ' + str(e))

    return None

def send_system_alert(_exception, exc_type, fname, tb_lineno, account, to_list):
    '''
    This function will trigger email alerts for orphan view identification system errors.
    '''
    try:
        exception_type = str(exc_type).replace('<', '').replace('>', '')
        subject = "SnowFlake orphan view identification: System Error (" + \
            exception_type+") for "+account
        html_body = """\
        <html>
        <body>
            <p>Hi Snowflake Admin Team,<br><br>
            <pre>Something went wrong with the snowflake orphan view identification system. Please see information below.</pre>
            <pre>Account: {account}</pre>
            <pre>Exception: {exception_type} </pre>
            <pre>Detail: {_exception}</pre>
            <pre>File Name: {fname}</pre>
            <pre>Line no.: {tb_lineno}</pre>
            </p>
        </body>
        </html>
        """.format(account=account,
                exception_type=exception_type, _exception=_exception,
                fname=fname, tb_lineno=tb_lineno)
        msgText = MIMEText('<br>%s<br>' % (html_body), 'html')
        send_email(to_list, subject, msgText)
    except Exception as e:
        logging.info(
            f"Error: Unable to send email at \
            {datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S')} to {tolist} \
            {_exception} {fname} {account} ")
            
def send_email(tolist, subject, msgbody, msg_images=[]):
    try:
        msg = MIMEMultipart()
        msgfrom = 'no-reply@cisco.com'
        msg['Subject'] = subject
        msg['From'] = msgfrom
        msg['To'] = ','.join(tolist)
        msg.attach(msgbody)
        for msg_img in msg_images:
            msg.attach(msg_img)
        s = smtplib.SMTP("outbound.cisco.com")
        s.sendmail(msgfrom, tolist, msg.as_string())
        s.quit()
        print(
            f"Mail Sent Successfully \
            {datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S')}")
    except smtplib.SMTPRecipientsRefused as smtp_err:
        # we dont want to raise the exception due to emails being refused
        logging.info(
            f"ERROR: {datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S')} Email Scheduler is unable to send email to {tolist} refused to get emails.")
    except Exception as e:
        logging.info(
            f"Error: sending email at \
            {datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S')} to {tolist}")
        raise


def test_jinja(template):

    ####################################
    sample_rows = pd.DataFrame(
        np.array(
            [['CISCO.US-EAST-1','EDW_OPS_ETL_DB_DV3','WI','PLT_TEST_VIEW_1','EDW_OPS_PLT_ETL_ROLE']
                , ['CISCO.US-EAST-1','EDW_OPS_ETL_DB_DV3','WI','PLT_TEST_VIEW_2','EDW_OPS_PLT_ETL_ROLE']
                , ['CISCODEV.US-EAST-1','EDW_OPS_ETL_DB_DV3','WI','PLT_TEST_VIEW_3','EDW_OPS_PLT_ETL_ROLE']
                , ['CISCODEV.US-EAST-1','EDW_OPS_ETL_DB_DV3','WI','PLT_TEST_VIEW_4','EDW_OPS_PLT_ETL_ROLE']]),
        columns=['account', 'database', 'schema', 'view', 'owner'])
    ####################################
    templateLoader = FileSystemLoader(searchpath="../bin/assets/template/")
    templateEnv = Environment(loader=templateLoader)
    templ = templateEnv.get_template(template)

    output = templ.render(sample_rows=sample_rows)
    msgbody = MIMEText(output, 'html')

    BASE_DIR = '../bin/assets/images/'
    list_of_images = [BASE_DIR+'header.png', BASE_DIR+'new_alert.png', BASE_DIR+'sql_query.png', BASE_DIR+'example_oo.png']
    msg_images = []

    for i, filename in enumerate(list_of_images):
        fp = open(filename, 'rb')
        msg_img = MIMEImage(fp.read())
        fp.close()
        msg_img.add_header('Content-ID', '<image'+str(i)+'>')
        msg_images.append(msg_img)
    
    print(output)
    send_email(['invalid-views-alerts@cisco.com'], 'Invalid View Identification', msgbody, msg_images)
