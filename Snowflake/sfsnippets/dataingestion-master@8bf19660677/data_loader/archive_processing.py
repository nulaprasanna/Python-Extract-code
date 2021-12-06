#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
from fnmatch import fnmatch
import os
import logging
from list_builder import getListOfFiles


def directory_maker(full_path):
    initial_dir = os.getcwd()
    try:
        delimiter = '/'
        arch_path = full_path.find('archives/')
        os.chdir(full_path[:arch_path])

        arch_path_lst = full_path[arch_path:].split(delimiter)
        for curr_dir in arch_path_lst:
            if os.path.isdir(curr_dir) != True:
                os.mkdir(curr_dir)
            os.chdir(curr_dir)
        os.chdir(initial_dir)
        ret_code = 0
    except Exception as e:
        print("Cannot create directory ", e)
        ret_code = -1
    return ret_code

def archiver(archive_path, full_dir_path, param_dict, user_logger):
    '''
    This archiving utility is invoked upon successful data ingestion
    It creates a zip file with all data, param and header files as well as user logs
    in the archival directory corresponding to the data ingestion directory.

    '''
    dateTimeObj = datetime.now()
    time_stmp = str(dateTimeObj.year) + str(dateTimeObj.month) + str(dateTimeObj.day) + '_' +\
    str(dateTimeObj.hour) + str(dateTimeObj.minute) + str(dateTimeObj.second)
    try:
        # Checking of the archived directory exists and 
        # trying to automatically create it, if needed
        if os.path.isdir(archive_path) != True:
            print("Post-cleanup: creating archive directory")
            directory_maker(archive_path)

        # Create a zipped archive file with data, param and header file
        archive_name = "zip -D " + archive_path + '/' + param_dict['target_table_name'] + "_" + time_stmp + " " + "*"
        os.chdir(full_dir_path); 
        os.system(archive_name)
        print("Post-cleanup: created archive file...OK")
        all_data_files = getListOfFiles(full_dir_path)
        data_files = [f.replace("\\","/") for f in all_data_files if \
            fnmatch(f, '.*') != True and \
            f.find('.header')<0 and \
            f.find('.param')<0 and \
            f.find('.log')<0 and \
            f.find('.sh')<0 and \
            f.find('.bak')<0 and \
            f.find('.xlsx')<0 and \
            f.find('.xls')<0 and \
            f.find('.xml')<0 and \
            f.find('.gz')<0 and \
            f.find('.zip')<0 and \
            f.find('.json')<0 and \
            f.find('unprocessed_records')<0]
        for data_file in data_files:
            os.remove(data_file)
        return 0
    except:
        return 1
