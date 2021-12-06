#!/usr/bin/python
# -*- coding: utf-8 -*-

import snowflake.connector
import pandas as pd
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import hvac
from ConfigObject import ConfigObject
import subprocess
import os
import sys
import glob
import csv

import logging
from send_mail import send_mail
from fnmatch import fnmatch
from logger import setup_logger
from config_processing import collect_property_file_contents
from list_builder import getListOfFiles


def file_size_checker(file_name, header_flag = 0):
    result = subprocess.run(['wc', '-l', file_name], stdout=subprocess.PIPE)
    size = int(result.stdout.split()[0])
    if header_flag == 0:
        size += 1
    return size


def open_sf_connection(sf_account, req_id):
    '''
    Establish a database connection. Assumes Snowflake.

    :param config_properties: This is the properties obtained from the util pull of config.ini
    :return: The database connection object

    '''
    # Connect to Keeper to collect secrets
    try:
        config_properties = collect_property_file_contents(f'./config/{sf_account.lower()}.conf')
        client = hvac.Client(
            url=config_properties['SFKEEPURL'],
            namespace=config_properties['SFKEEPNAME'],
            token=config_properties['TOKEN'],
            verify=False
        )

        # Secrets are stored within the key entitled 'data'

        keeper_secrets = client.read(
            config_properties['SFKEYPATH'])['data']
        passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
        private_key = keeper_secrets['private_key']

        # PEM key must be byte encoded
        key = bytes(private_key, 'utf-8')

        p_key = serialization.load_pem_private_key(
            key, password=passphrase.encode(), backend=default_backend()
        )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

        conn = snowflake.connector.connect(
            user=config_properties['SFUSER'], account=config_properties['SFACC'],
            warehouse=config_properties['SFWH'], role=config_properties['SFROLE'], private_key=pkb)

        return conn
    except Exception as e:
        return -1

def sf_tgt_table_cnt(conn, tgt_table):
    '''
    Check whether the target table exists

    '''
    cs = conn.cursor()
    sql = "SELECT COUNT(*) FROM "+ tgt_table + ";"
    res = cs.execute(sql)
    rec_count = res.fetchone()[0]
    if rec_count >= 0:
        # added to get table structure
        sql = "DESC TABLE " + tgt_table + ";"
        curOpen = cs.execute(sql)
        sfCols = [row[0].lower() for row in curOpen.description]
        df_data = pd.DataFrame(curOpen.fetchall(), columns=(sfCols))
        # return (rec_count, df_data)
        return rec_count
    else:
        print('Cannot connect to Snowflake...exiting')
        return -1

def sf_tgt_table_header_dict(conn, tgt_table):
    '''
    Calculate the number of columns in the target table

    '''
    cs = conn.cursor()
    sql = "DESC TABLE " + tgt_table + ";"
    curOpen = cs.execute(sql)
    sfCols = [row[0].lower() for row in curOpen.description]
    df_data = pd.DataFrame(curOpen.fetchall(), columns=(sfCols))
    header_dict_pd = pd.Series(df_data.type.values,index=df_data.name).to_dict()

    # Removing constraint columns
    for key in list(header_dict_pd.keys()):
        if key.startswith('EDWSF_'):
            del header_dict_pd[key]

    return header_dict_pd

def put_source_files(conn, full_dir_path, param_dict, wh_name, sys_logger, user_logger, SUPPORT_EMAIL_LIST):
    '''
    Create a list of files in the specified folder and subtract .param, .header and unprocessed
    Construct a list of all columns
    Check of the temporary staging area already exists for the table and if not create it
    In a loop, put each file into the staging area

    '''
    try:
        cs = conn.cursor()
        sql = "use warehouse " + wh_name +";"
        res = cs.execute(sql)
  
        sql = "use " + param_dict['target_db_name'] +";"
        res = cs.execute(sql)

        sql = "use schema STG;"
        res = cs.execute(sql)

        print("Creating a staging area in Snowflake")
        sql = 'create or replace stage stg_'+ param_dict['target_table_name'] +\
            ' file_format = (format_name = ' +"'" + param_dict['file_format_path'] +\
            param_dict['source_format']+"');"
        res = cs.execute(sql)
        print("SF staging area created...OK")

        # Creating a list of data files
        # that excludes param, header and log files
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
        if len(data_files) == 0:
            msg = "No data files found in the directory " + full_dir_path
            print(msg)
            sys_logger.error(msg)
            user_logger.error(msg)
            send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
            ret_code = -1
        else:
            print("Putting files into Snowflake staging area")
            total_lines = 0
            if param_dict['source_format'].find('with_header') > 0:
                header_flag = 1
            else:
                header_flag = 0
            for data_file in data_files:
                total_lines += file_size_checker(data_file, header_flag)
                sql = 'put file://' + data_file +\
                    " @stg_"+ param_dict['target_table_name'] + " parallel=10;"
                res = cs.execute(sql)
            if total_lines == 0:
                msg = "No records were uploaded to SF staging area, check if the source files(s) are empty"
                print(msg)
                sys_logger.error(msg)
                user_logger.error(msg)
                send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
                ret_code = -1
            else:
                return total_lines
    except:
        ret_code = -1
    return ret_code

def copy_data_csv(tgt_tbl_name, header_st, src_tbl_name, arg_st, tbl_file_format):
    sql = "copy into " + tgt_tbl_name + " (" + header_st +\
        ") from (select " + arg_st + " from "+ src_tbl_name +\
        " t) file_format = (format_name = " +\
        "'" + tbl_file_format + "');"
    return sql

def copy_data_parquet(target_table, source_table, param_dict, header_dict):
    sql = ''
    header_columns = [hc.lower() for hc in list(header_dict.keys())]
    header_datatypes = [hc.lower().strip(',') for hc in list(header_dict.values())]
    target_columns = list(zip(header_columns, header_datatypes))
    tgt_column_dict = ['$1:'+i[0]+'::'+i[1] for i in target_columns]
    tgt_column_str = ', '.join(tgt_column_dict)
    sql = "copy into " + target_table + " from (select " +\
        tgt_column_str+" from " + source_table + ");"
    return sql

def create_temp_table(cs,param_dict, header_dict, header_st, arg_st, tbl_file_format, sys_logger, user_logger, SUPPORT_EMAIL_LIST):
    '''
    This procedure creates temp table in STG schema
    and populates it with uploaded file

    '''
    # Creating empty temp table in the staging schema
    try:
        header_columns = [hc.lower() for hc in list(header_dict.keys())]
        header_datatypes = [hc.lower().strip(',') for hc in list(header_dict.values())]
        target_columns = list(zip(header_columns, header_datatypes))
        tgt_column_dict = [i[0]+' '+i[1] + ' NULL' for i in target_columns]
        tgt_column_str = ', '.join(tgt_column_dict)

        sql = 'use ' + param_dict['target_db_name'] + ';'
        res = cs.execute(sql)
        sql = 'use schema stg;'
        res = cs.execute(sql)
        sql = "create or replace table " + "tmp_" + param_dict['target_table_name'] + " (" + tgt_column_str + ");"

        res = cs.execute(sql)

        # Copying staged data files into STG tmp table
        if param_dict['source_format'].find('parquet') >= 0:
            target_table = param_dict['target_db_name'] + '.'+\
                'stg.tmp_' + param_dict['target_table_name']
            source_table = "@stg.stg_" + param_dict['target_table_name']
            sql = copy_data_parquet(target_table, source_table, param_dict, header_dict)
            res = cs.execute(sql)
        # elif param_dict['source_format'].find('csv') >= 0:
        else:
            sql = copy_data_csv('stg.tmp_' + param_dict['target_table_name'],\
                header_st,\
                '@stg.stg_' + param_dict['target_table_name'],\
                arg_st,\
                tbl_file_format)
            res = cs.execute(sql)

        # Checking for the number of records in the temp table
        sql = 'select count(*) from stg.tmp_' + param_dict['target_table_name'] + ';'
        res = cs.execute(sql)
        res_out = res.fetchone()[0]
        if res_out == 0:
            print('No records were copied to the temp table...exiting')
            temp_ret_code = -1
        else:
            temp_ret_code = 0
    except Exception as e:
        msg = "Could not create temp table: "
        print(msg, e)
        sys_logger.error(msg)
        user_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        temp_ret_code = -1
    return temp_ret_code

def process_stg_data(conn, param_dict, wh_name, header_dict, sys_logger, user_logger, SUPPORT_EMAIL_LIST):
    '''
    This procedure appends/overwites/upserts staged dataset 
    into the prod table

    '''
    try:
        cs = conn.cursor()
        header_columns = [hc.lower() for hc in list(header_dict.keys())]
        header_line_count = len(header_columns)
        header_column_str = ', '.join(header_columns)
        arg_lst = ["t.$" + str(i+1) for i in range(0,header_line_count)]
        arg_lst2 = ["$" + str(i+1) for i in range(0,header_line_count)]
        arg_st = ', '.join(arg_lst)
        arg_st2 = ', '.join(arg_lst2)
        tbl_file_format = param_dict['file_format_path'] + param_dict['source_format']

        # Creating string for UPSERT condition with incremental key
        # that consists of more than one column
        if param_dict['merge_type'] == 'upsert':
            if len(param_dict['incremental_key']) > 1:
                upsert_condition_dict = ['t1.$'+ str(int(header_columns.index(i)+1)) +' = t2.$'+ str(int(header_columns.index(i)+1)) for i in param_dict['incremental_key']]
                upsert_condition = ' and '.join(upsert_condition_dict)
            else:
                upsert_condition_ind = int(header_columns.index(param_dict['incremental_key'][0])) + 1
                upsert_condition = 't1.$'+ str(upsert_condition_ind ) + ' = t2.$'+ str(upsert_condition_ind )

        # One more header_columns2 for upsert
        header_columns2 = ["t1.$" + str(i+1) for i in range(0,header_line_count)]

        # Creating a string for merge statement
        merge_lst = []
        for i in range(0,len(header_columns2)):
            merge_lst.append(header_columns2[i] + " = t2." + arg_lst2[i])
        merge_str = ', '.join(merge_lst)

        # Creating empty temp table in the staging schema
        sql = 'use ' + param_dict['target_db_name'] + ';'
        res = cs.execute(sql)

        if param_dict['merge_type'] == 'append':
            # Appending data from the staging area to the prod. table
            if param_dict['source_format'].find('parquet') >= 0:
                # Creating and populating temp table with uploaded file
                temp_ret_code = create_temp_table(cs,param_dict, \
                                                header_dict, \
                                                header_st, \
                                                arg_st, \
                                                tbl_file_format, \
                                                sys_logger, \
                                                user_logger, \
                                                SUPPORT_EMAIL_LIST)
                if temp_ret_code == -1:
                    sys.exit()
                # Appending data to the prod table from the temp table
                sql = "insert into " + \
                    param_dict['target_schema_name'] + '.' +\
                    param_dict['target_table_name'] + " (" + header_st +\
                    ") select " + header_st + " from stg.tmp_"+\
                    param_dict['target_table_name'] + ";"
            else:
                sql = copy_data_csv(param_dict['target_schema_name'] + '.' +\
                                    param_dict['target_table_name'],\
                                    arg_st2,\
                                    '@stg.stg_' + param_dict['target_table_name'],\
                                    arg_st,\
                                    tbl_file_format)
            res = cs.execute(sql)
            msg = "Appended dataset into the table...OK"
            print(msg)
            ret_code = 0

        if param_dict['merge_type'] == 'overwrite':
            # Creating and populating temp table with uploaded file
            temp_ret_code = create_temp_table(cs,param_dict, \
                                            header_dict, \
                                            header_column_str, \
                                            arg_st, \
                                            tbl_file_format, \
                                            sys_logger, \
                                            user_logger, \
                                            SUPPORT_EMAIL_LIST)
            if temp_ret_code == 0:
                # Performing insert overwrite into the prod table
                sql = "insert overwrite into " + \
                        param_dict['target_schema_name'] + '.' +\
                        param_dict['target_table_name'] + " (" + arg_st2 +\
                        ") select " + arg_st + " from stg.tmp_"+\
                        param_dict['target_table_name'] + " t;"

                res = cs.execute(sql)
                msg = "Inserted staging dataset into the prod table...OK"
                print(msg)
                ret_code = 0
            else:
                ret_code = -1

        if param_dict['merge_type'] == 'upsert':
            # Creating and populating temp table with uploaded file
            temp_ret_code = create_temp_table(cs,param_dict, \
                                            header_dict, \
                                            header_column_str, \
                                            arg_st, \
                                            tbl_file_format, \
                                            sys_logger, \
                                            user_logger, \
                                            SUPPORT_EMAIL_LIST)
            if temp_ret_code == 0:
                # Performing Upsert into the Prod table
                sql = "merge into " + param_dict['target_schema_name'] + "." +\
                    param_dict['target_table_name'] + " t1 using stg.tmp_" +\
                    param_dict['target_table_name'] + " t2 on " +\
                    upsert_condition + " when matched then update set "+\
                    merge_str + " when not matched then insert (" +\
                    arg_st2 + ") values (" + arg_st2 + ");"
                res = cs.execute(sql)
                msg = "Upserted staging dataset into the prod table...OK"
                print(msg)
                ret_code = 0
            else:
                ret_code = -1
        if ret_code == 0:
            conn.commit()
        
    except Exception as e:
        msg = "Could not ingest data into prod table: "
        print(msg, e)
        sys_logger.error(msg)
        user_logger.error(msg)
        send_mail(SUPPORT_EMAIL_LIST, 'Data Loader Notification', msg)
        ret_code = -1
    return ret_code

def close_connection(conn):
    '''
    Close a SF connection
    :param conn: this is the conn object when creating the connection

    '''
    conn.commit()
    conn.close()
