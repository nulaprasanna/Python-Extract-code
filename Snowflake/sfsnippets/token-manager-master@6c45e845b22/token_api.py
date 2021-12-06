#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import json
from datetime import datetime

def get_token_api(sf_account, svc_account, current_token):
    '''
    This function checks the expiration date for the current tokens in the given account
    '''

    url_string_start = "https://sf-api.cisco.com/snowflake/v1/operations/vault/token"
    url_string = url_string_start + '?' + 'sf_account=' + sf_account + "&" + 'svc_account=' +svc_account

    headers = {
        "user-token": current_token
    }

    body = {
        "sf_account": sf_account,
        "svc_account": svc_account
    }

    conn = requests.get(url_string, headers = headers)

    if conn.status_code == 200:
        parsed_json = json.loads(conn.text)
        token_dict = {}
        ret_object_str = str(parsed_json['returnObject']).replace("'",'"')
        ret_content = json.loads(ret_object_str)
        ret_token1 = ret_content['Token1']['token']
        ret_exp_date_str1 = ret_content['Token1']['token_expiration_date']
        ret_exp_date1 = datetime.strptime(ret_exp_date_str1, '%Y-%m-%dT%H:%M:%S')
        ret_exp_date1_value = str(ret_exp_date1.date())
        token_dict['Token1'] = (ret_token1, str(ret_exp_date1.date()))
        if 'Token2' in ret_content:
            return_token2 = ret_content['Token2']['token']
            return_expiration_date_str2 = ret_content['Token2']['token_expiration_date']
            return_expiration_date2 = datetime.strptime(return_expiration_date_str2, '%Y-%m-%dT%H:%M:%S')
            return_expiration_date2_value = str(return_expiration_date2.date())
            token_dict['Token2'] = (return_token2, return_expiration_date2_value)
        ret_value = token_dict
    else:
        print("Cannot read the current tokens")
        ret_value = -1

    return ret_value


def post_token_api(sf_account, svc_account, current_token):
    '''
    This function creates a new token in the given account
    '''

    url_string_start = "https://sf-api-stg.cisco.com/snowflake/v1/operations/vault/token"

    headers = {
        "Content-Type": "application/json"
    }

    body = {
        "sf_account": sf_account,
        "svc_account": svc_account,
        "user_token": current_token
    }

    conn = requests.post(url_string_start, headers = headers, data = body)


    if conn.status_code == 200:
        parsed_json = json.loads(conn.text)
        token_dict = {}
        combined_string = ''.join(all_lines)
        combined_string = combined_string.replace("\n","").replace(" ","")
        parsed_json = json.loads(combined_string)
        token_value = parsed_json['returnObject'][0]['auth']['client_token']
        token_exp_timestamp_str = parsed_json['returnObject'][1]
        token_exp_timestamp = datetime.strptime(token_exp_timestamp_str, '%Y-%m-%dT%H:%M:%S')
        token_exp_date = str(token_exp_timestamp.date())
        token_dict['Token'] = (token_value, token_exp_date)
        ret_value = token_dict
    else:
        print("Cannot create a new token")
        ret_value = -1
    return ret_value


def rotate_token_api(sf_account, svc_account, current_token):
    '''
    This function creates a new token in the given account
    '''

    url_string_start = "https://sf-api-stg.cisco.com/snowflake/v1/operations/vault/token"

    headers = {
        "Content-Type": "application/json"
    }

    body = {
        "sf_account": sf_account,
        "svc_account": svc_account,
        "user_token": current_token
    }

    conn = requests.put(url_string_start, data = body)

    if conn.status_code == 200:
        parsed_json = json.loads(conn.text)
        token_dict = {}
        token_dict['status_code'] = conn.status_code
        token_dict['message'] = parsed_json["returnObject"]
        ret_value = token_dict
    else:
        print("Cannot create a new token")
        ret_value = -1
    return ret_value
