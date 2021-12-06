#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import time
from datetime import datetime, timedelta

from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def send_mail(tolist, subject, msgbody):
    '''
    Send an email to specific list of users 

    : param tolist: list of emails to send the specific email too
    : param subject: subject of the email being sent
    : param msgbody : the message body of the email to be sent
    TO-DO: 
        - Add check for email validity
        - Ability to provide emails in separate files and join 
            them together as needed, so it is possible to maintain
            several independent mailing lists

    '''

    # Global Settings
    # Setting ToList delimiter between different emails
    list_delimiter = ','
    # to_lst = [i for i in tolist.split(list_delimiter)]
    to_lst = [i[1:-1] for i in tolist.split(list_delimiter)]
    msgfrom = 'no-reply@cisco.com'

    msg = MIMEMultipart()
    message = msgbody
    msg['Subject'] = subject
    msg['From'] = msgfrom
    msg['To'] = tolist

    msg.attach(MIMEText(message, 'html'))
    s = smtplib.SMTP('outbound.cisco.com')
    s.sendmail(msgfrom, to_lst, msg.as_string())
    s.quit()
    return "Message sent!"

# The following code has been added for unit testing purposes
# if __name__ == "__main__":
#     tolist = 'alusher@cisco.com,kbatna@cisco.com'
#     subject = 'Test Subject'
#     msgbody = 'test error message'
#     from pdb import set_trace; set_trace()
#     send_mail(tolist, subject, msgbody)
