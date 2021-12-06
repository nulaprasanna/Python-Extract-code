#!/usr/bin/python
# -*- coding: utf-8 -*-

#########################################################################################################################
# This command-line Data Loader utility loads flat files into Snowflake DB                                              #
# The 
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Alex L          13-Nov-2019             Initial Version                                                 #
# 1.1           Alex L          30-Nov-2019             Added summary and modified sample param file                    #
#########################################################################################################################
import os
from os.path import isdir, isfile
import sys
import glob
import argparse
import logging
from datetime import datetime
from ConfigObject import ConfigObject

from send_mail import send_mail
from logger import setup_logger
from archive_processing import archiver
from wh_selector import warehouse_selector
from param_file_processing import param_file_parser
from snowflake_processing import open_sf_connection, close_connection,\
    sf_tgt_table_cnt, put_source_files, process_stg_data,\
    sf_tgt_table_header_dict
from config_processing import collect_property_file_contents

##############################################
# Pre-checks for Python version and config
##############################################
if sys.version.title()[0]!='3':
    sys.exit('This program is developed for Python 3')

##############################################
#                   MAIN
##############################################
if __name__ == "__main__":
    ##########################################################################################
    #       STEP 1 Parsing Arguments
    ##########################################################################################
    parser = argparse.ArgumentParser(description='Data ingestion utility into Snowflake',\
        epilog='Example: python main.py -a ciscodev.us-east-1 -t db_name.schema_name.tbl_name')
    parser.add_argument("-a", "--accountName", required=True, help="Enter account name")
    parser.add_argument("-t", "--tableName", required=True, help="Enter fully qualified table name")
    args = parser.parse_args()
    acct_name =args.accountName
    full_tbl_name =args.tableName

    # processing config.ini
    try:
        config_properties = collect_property_file_contents(f'./config/config.ini')
        ENTRY_POINT = config_properties['ENTRY_POINT']

        # Setting up system log file name
        SYSTEM_LOG_FILE = config_properties['LOG_FILE']

        # Setting up support email list
        SUPPORT_EMAIL_LIST = config_properties['EMAIL_LIST']

        # Setting header file delimiter
        DELIMITER = config_properties['HEADER_FILE_DELIMITER']

        # Checking for System Log file name existence
        if os.path.isfile(SYSTEM_LOG_FILE) != True:
            create_logfile_str = 'touch ' + SYSTEM_LOG_FILE
            os.system(create_logfile_str)
        sys_logger = setup_logger('sys_logger', SYSTEM_LOG_FILE,  level=logging.INFO)
    except Exception as e:
        msg = "Exception occurred while processing config.ini and system log. Please check." + e
        print(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()

    # Parsing DB, schema and table names from command line arguments
    db_name, sch_name, tbl_name = full_tbl_name.lower().split('.')

    # Sanity check for table name
    if db_name.strip() == '' or sch_name.strip() == '' or tbl_name.strip() == '':
        msg = "Exception occurred while processing fully qualified table name. Please check."
        print(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()

    # Adding a slash to the ENTRY POINT, if it is not in the config file
    if ENTRY_POINT[len(ENTRY_POINT)-1] != '/':
        ENTRY_POINT += '/'

    full_dir_path = ENTRY_POINT + acct_name +'/'+ db_name +'/' + sch_name +'/' + tbl_name
    archive_path = ENTRY_POINT + 'archives/' + acct_name +'/'+ db_name +'/' + sch_name +'/' + tbl_name

    # creating param_file and header_file names from arguments
    param_file_name = full_dir_path + '/' + tbl_name + '.param'
    # header_file_name = full_dir_path + '/' + tbl_name + '.header'

    # Setting up name for user log file
    ftime=datetime.now().strftime('%Y%m%d%H%M%S')
    reqid=str(ftime)
    user_logfile_name = full_dir_path + '/' + reqid + '.log'

    # checking for existence of param, header and warehouse files
    ret_status = 0
    if os.path.isfile(param_file_name) != True:
        msg = 'Param file not found...exiting'
        print(msg)
        sys_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()
    # if os.path.isfile(header_file_name) != True:
    #     msg = 'Header file not found...exiting'
    #     print(msg)
    #     sys_logger.error(msg)
    #     send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
    #     sys.exit()

    if len(db_name) > 0:
        wh_list_file = 'config/wh_list.ini'
        if os.path.isfile(wh_list_file) != True:
            msg = 'Warehouse list file not found...exiting'
            print(msg)
            sys_logger.error(msg)
            send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
            sys.exit()
        else:
            wh_name = warehouse_selector(wh_list_file, db_name)
    else:
        msg = 'Database name is missing'
        print(msg)
        sys_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()

    if os.path.isfile(user_logfile_name) != True:
        try:
            create_logfile_str = 'touch ' + user_logfile_name
            os.system(create_logfile_str)
            USER_LOG_FILE = user_logfile_name
        except:
            msg = 'Cannot create user log file...exiting'
            print(msg)
            sys_logger.error(msg)
            send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
            sys.exit()

    # Deleting previous user logs
    curr_path = os.getcwd()
    os.chdir(full_dir_path)
    if os.name == 'nt':
        os.system('del *.log')
    else:
        os.system('rm *.log')
    os.chdir(curr_path)

    # Setting up User Log File
    user_logger = setup_logger('user_logger', USER_LOG_FILE,  level=logging.INFO)

    ##########################################################################################
    #       STEP 2 Checking Snowflake connectivity
    ##########################################################################################
    # Processing Snowflake configuration file
    sf_conn = open_sf_connection(acct_name, reqid)
    if sf_conn == -1:
        msg = "Exception occurred while establishing SF connection....exiting"
        print(msg)
        sys_logger.error(msg)
        user_logger.error(msg)
        sys.exit()
    else:
        msg = "SF connectivity was successfully established"
        print(msg)

    ##########################################################################################
    #       STEP 3 Param file processing and header_dict creation
    ##########################################################################################
    ret_status, param_dict = param_file_parser(param_file_name)
    if ret_status == 0:
        msg = 'Param file check passed...OK'
        print(msg)
        sys_logger.info(msg)
        user_logger.info(msg)
        USER_EMAILS = param_dict['email']
    else:
        msg = 'Param file cannot be processed...exiting'
        print(msg)
        sys_logger.error(msg)
        user_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()

    # Sanity check for param_dict keys
    param_checks = 0
    if 'target_db_name' not in param_dict.keys() or param_dict['target_db_name'] == '':
        print("Please check the .param file entry for ", 'target_db_name')
        param_checks += 1
    elif 'target_schema_name' not in param_dict.keys() or param_dict['target_schema_name'] == '':
        print("Please check the .param file entry for ", 'target_schema_name')
        param_checks += 1
    elif 'target_table_name' not in param_dict.keys() or param_dict['target_table_name'] == '':
        print("Please check the .param file entry for ", 'target_table_name')
        param_checks += 1
    elif 'file_format_path' not in param_dict.keys() or param_dict['file_format_path'] == '':
        print("Please check the .param file entry for ", 'file_format_path')
        param_checks += 1
    elif 'source_format' not in param_dict.keys() or param_dict['source_format'] == '':
        print("Please check the .param file entry for ", 'source_format')
        param_checks += 1
    elif 'merge_type' not in param_dict.keys() or param_dict['merge_type'] == '':
        print("Please check the .param file entry for ", 'merge_type')
        param_checks += 1
    if param_checks > 0:
        print("Inconsistent param file...exiting")
        sys.exit()

    # Sanity check for incremental key required for upsert use case
    if len(param_dict['incremental_key']) == 0 and param_dict['merge_type'] == 'upsert':
        msg = 'The incremental key is empty for UPSERT oprtation...exiting'
        print(msg)
        sys_logger.error(msg)
        user_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()
    # TO-DO:
    #   1) add check for uniqueness of incremental column or merge will fail
    #   2) Rework the check for composite incremental key

    # Checking for target table existence
    full_tbl_name = param_dict['target_db_name'] + '.' + param_dict['target_schema_name'] + '.' + param_dict['target_table_name']

    row_count_before = sf_tgt_table_cnt(sf_conn, full_tbl_name)
    if row_count_before == -1:
        msg = 'Target SF table cannot be found...exiting'
        sys_logger.error(msg)
        user_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()
    else:
        msg = "The target table has been successfully checked"
        print(msg)
        sys_logger.info(msg)
        user_logger.info(msg)
        header_dict = sf_tgt_table_header_dict(sf_conn, full_tbl_name)

    # header_dict = header_file_parser(header_file_name, DELIMITER)
    header_line_count = len(header_dict)

    if header_line_count == 0:
        msg = 'Header dict is either empty or was improperly processed...exiting'
        print(msg)
        sys_logger.error(msg)
        user_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()
    else:
        msg = 'Header dict was successfully built...OK'
        print(msg)
        sys_logger.info(msg)
        user_logger.info(msg)

    ##########################################################################################
    #       STEP 4 Data File Ingestion
    ##########################################################################################
    # Create a list of files in the specified folder and subtract .param, .header and unprocessed
    # Construct a list of all columns
    # Check of the temporary staging area already exists for the table and if not create it
    # In a loop, put each file into the staging area

    # Uploading data files to the staging area
    total_lines = put_source_files(sf_conn, full_dir_path, param_dict, wh_name, sys_logger, user_logger, SUPPORT_EMAIL_LIST)
    if total_lines == -1:
        msg = "Nothing was uploaded into the staging area in Snowflake...exiting"
        print(msg)
        sys_logger.error(msg)
        user_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()
    else:
        msg = "Files put into the SF staging area...OK"
        print(msg)
        sys_logger.info(msg)
        user_logger.info(msg)

    # Construct list of all columns and the COPY statement. Execute the statement
    ret_code = process_stg_data(sf_conn, param_dict, wh_name, header_dict, sys_logger, user_logger, SUPPORT_EMAIL_LIST)
    if ret_code == 0:
        msg = "Copied staging dataset into the target table...OK"
        print(msg)
        sys_logger.info(msg)
        user_logger.info(msg)
    else:
        msg = "Cannot copy staging dataset into the target table...exiting"
        print(msg)
        sys_logger.error(msg)
        user_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()

    row_count_after = sf_tgt_table_cnt(sf_conn, full_tbl_name)
    if row_count_after == -1:
        msg = 'Target SF table cannot be found...exiting'
        sys_logger.error(msg)
        user_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        sys.exit()
    else:
        msg = ""
        msg = msg + ' \n'
        msg = msg + ' \n'
        msg = msg + '                    Ingestion summary: \n'
        msg = msg + '             Target DB: ' + param_dict['target_db_name'].upper() + '\n'
        msg = msg + '             Target Schema: ' + param_dict['target_schema_name'].upper() + '\n'
        msg = msg + '             Target Table: ' + param_dict['target_table_name'].upper() + '\n'
        msg = msg + '---------------------------------------------------------   \n'
        msg = msg + 'Number of lines in the source data set: ' + str(total_lines) + ' \n'
        msg = msg + '---------------------------------------------------------   \n'
        if param_dict['merge_type'] in ('append','upsert'):
            msg = msg + 'Number of records in target table before ingestion: ' + str(row_count_before) + ' \n'
        msg = msg + 'Number of records in target table after ingestion: ' + str(row_count_after) + ' \n'
        msg = msg + ' \n'
        msg = msg + ' \n'
        print(msg)
        sys_logger.info(msg)
        user_logger.info(msg)

    ##########################################################################################
    #       STEP 5 Post-cleanup
    ##########################################################################################
    close_connection(sf_conn)
    # Launching archival utility
    ret_code  = archiver(archive_path, full_dir_path, param_dict, user_logger)
    if ret_code == 0:
        msg = "Post-cleanup: moved archive file to the archive folder for the table " + full_tbl_name
        print(msg)
        sys_logger.info(msg)
        user_logger.info(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
    else:
        msg = "Post-cleanup: cannot move archive file to the archives folder for the table " + full_tbl_name
        print(msg)
        sys_logger.warning(msg)
        user_logger.warning(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
