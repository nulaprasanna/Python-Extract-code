#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import paramiko


def remote_config_processing(machinename, username, filename, delimiter, mode, data = None):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(machinename, username=username)
    sftp = ssh.open_sftp()
    try:
        f = sftp.open(filename, mode)
        if mode == 'w':
            if len(data) > 0:
                total_str = ''
                for k, v in data.items():
                    total_str += str(k) + ';'+ str(v).strip() + '\n'
                out = f.write(total_str)
            else:
                print("No data - cannot write to the file")

        elif mode == 'r':
            result = f.read()
            result_lst = result.decode('utf-8').split('\n')
            pair = lambda i : i.split(delimiter)
            out = {pair(i)[0]:pair(i)[1].replace('\n','').strip() for i in result_lst if len(i)>0}
        f.close()
        ssh.close()

    except IOError:
        print("This operation caused an error")
        out = -1
    return out
