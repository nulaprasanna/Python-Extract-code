#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os

def warehouse_selector(wh_list_file, db_name):
    '''
     This method identifies the target warehouse based on the database name 
     pattern. If no  target warehouse found, then it returns the default value.
     Otherwise, it returns 'None'.

    '''
    try:
        wh_dict = {}

        with open(wh_list_file, 'r') as wlf:
            for line in wlf:
                if not line.strip().startswith("#") and len(line.strip()) > 0:
                    name, value = line.split(';')
                    name = name.lower().strip()
                    value = value.lower().strip()
                    wh_dict[name] = value.strip('\n')
            if len(wh_dict) == 0:
                print('The warehouse list is empty')
                sys.exit(1)
            target_wh = 'None'

            for key in wh_dict.keys():
                if db_name.startswith(key):
                    target_wh = wh_dict[key]

            if (target_wh == 'None') and wh_dict['default']: 
                target_wh = wh_dict['default']

        target_wh = wh_dict['default'] ## Temporary fix
        return target_wh

    except Exception as e:
        print('ERROR: Unable to open a list of warehouses (file: ' + wh_list_file + ')')
        print('ERROR: ' + str(e))
        sys.exit(1)