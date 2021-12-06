#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os

def param_file_parser(param_file_name):
    '''
    This function process *.param files and returns them
    as dictionary to the main routine, or -1 in case of failure

    '''
    if os.path.isfile(param_file_name) != True:
        print('Param file not found')
        ret_status = 1
    else:
        try:
            f_param = open(param_file_name,'r')
            param_lines = f_param.readlines()
            param_dict = {}
            for p_line in param_lines:
                if p_line[0] != '#' and len(p_line.strip()) != 0:
                    parsed_line = p_line.lower().split(';')
                    param_dict[parsed_line[0].strip()] = parsed_line[1].strip().strip('\n')

            # checking for composite INCREMENTAL_KEY
            if param_dict['incremental_key'].find(',') > 0:
                compound_inc_key_dict = param_dict['incremental_key'].split(',')
                param_dict['incremental_key'] = [i.strip().strip('\n') for i in compound_inc_key_dict]
            else:
                inc_key = param_dict['incremental_key']
                param_dict['incremental_key'] = []
                param_dict['incremental_key'].append(inc_key)
            ret_status = 0

            # checking whether File Format Path contains dot at the end
            # if not then it will be added
            ff_path = param_dict['file_format_path']
            if ff_path[len(ff_path)-1] !='.':
                param_dict['file_format_path'] = ff_path + '.'

        except Exception as e:
            print("The following caused exception", e)
            ret_status = 1

    return [ret_status,param_dict]