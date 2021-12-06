#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
from os.path import isdir, isfile
import sys
import glob
import argparse
import logging
from datetime import datetime
from ConfigObject import ConfigObject

def collect_property_file_contents(property_file):
    '''
     This method will attempt to open and load the property file to a property file object (onboardingPropertiesObject)
    :param property_file: The file which contains all the properties to be loaded
    :param account_name: The Snowflake Account instance. EG. ciscodev
    :return:

    '''
    try:
        config_dict = {}
        with open(property_file, 'r') as fvar:
            for line in fvar:
                if not line.strip().startswith("#") and len(line.strip()) > 0:
                    name, value = line.split(';')
                    config_dict[name] = value.strip().strip('\n')

        return config_dict
    except Exception as e:
        print('ERROR: Unable to open and collect property file contents for (property file: '
              + property_file + ')')
        print('ERROR: ' + str(e))
        exit(1)