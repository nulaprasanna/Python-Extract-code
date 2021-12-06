#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
This Token Manager utility updates tokens in each file listed in 
config/hosts.txt
'''

import os
import sys
from datetime import datetime, date

from token_api import get_token_api, post_token_api, rotate_token_api
from config_processing import collect_property_file_contents
from remote_config_processing import remote_config_processing

# Checking Python version
if sys.version.title()[0]!='3':
    print('This program is developed for Python 3')
    sys.exit(1)

try:
    # STEP 1: Read config file
    #     name; ip_address, full_path, key_name
    host_properties = collect_property_file_contents(f'./config/hosts.txt')
    today_date = date.today()

    # STEP 2: Loop through each entry in the file config/hosts.txt
    for i, j in host_properties.items():
        sf_account = i
        print("Processing tokens for: ", sf_account)
        user = j['user']
        host = j['host']
        svc_account = j['svc_account']
        file = j['file']
        key = j['key']
        delimiter = j['delimiter'].strip("'")

        # Read the remote file with token into dict
        output = remote_config_processing(host, user, file, delimiter, 'r', data = None)

        current_token = output[key]

        # Make a call to SF-API service to obtain expiration date for each token
        token_dates = get_token_api(sf_account, svc_account, current_token)

        str_to_date = lambda i : (datetime.strptime(i , '%Y-%m-%d')).date()

        # There are only 2 tokens allowed by design
        if 'Token2' in token_dates:
            if token_dates['Token1'][0] == current_token:
                current_delta = (str_to_date(token_dates['Token1'][1]) - today_date).days
                other_delta = (str_to_date(token_dates['Token2'][1]) - today_date).days
                other_token = token_dates['Token2'][0]
            elif token_dates['Token2'][0] == current_token:
                current_delta = (str_to_date(token_dates['Token2'][1]) - today_date).days
                other_delta = (str_to_date(token_dates['Token1'][1]) - today_date).days
                other_token = token_dates['Token1'][0]

            #  If seven or less days are left before the current token expires
            if other_delta > current_delta and current_delta <= 7:
                if len(output)>0:
                    output[key] = other_token

                    # Write the other token into the conf file
                    remote_config_processing(host, user, file, delimiter, 'w', data = output)
                    print("...SUCCESS: Inserted new token for the account: ", sf_account, "\n")

                    # Rotate the token that is about to expire
                    res = rotate_token_api(sf_account, svc_account, current_token)
                    if res['status_code'] == 200:
                        print('message: ', res['message'])
                    else:
                        print("Cannot rotate the previous token!")
                else:
                    print("The dictionary is empty!")
        else:
            print("...Create and insert new token for ", sf_account, "\n")
            res = post_token_api(sf_account, svc_account, current_token)
            output[key] = res['Token'][0]
            # Writing the new token into the conf file
            remote_config_processing(host, user, file, delimiter, 'w', data = output)

except Exception as e:
    msg = f"Exception occurred while processing token for ",sf_account
    print(msg, e)
    sys.exit(1)
