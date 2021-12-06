#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
from ConfigObject import ConfigObject
import configparser

def collect_property_file_contents(property_file, account_name=None):
    """
     This method will attempt to open and load the property file to a Python dict 
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
        if account_name:
            config_dict = as_dict(config)[account_name]
        else:
            config_dict = as_dict(config)
        return config_dict
    except Exception as e:
        print('ERROR: Unable to open and collect property file contents for hosts file')
        print('ERROR: ' + str(e))
        exit(1)
