#!/apps/python/install/bin/python
import argparse
import snowflake.connector
import cx_Oracle
import teradatasql
import os, re, string, subprocess, sys, csv
from datetime import datetime
from datetime import timedelta
import time
import hvac
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from ConfigObject import ConfigObject
import faulthandler; faulthandler.enable()
import math
from metadata_handler import *
import pdb,random,string
import threading,calendar
parser = argparse.ArgumentParser(description='Script used to perform put operation',
    epilog='Example: python DIY_perform_put_operation.py -e env -u user -w wh -r role -p put_cmd -i reqid -t temp_dir -b bucket_num')
parser.add_argument('-p','--cmd',required=True,
        help='Put command to execute')
parser.add_argument('-i','--reqid',required=True,
        help='reqid')
parser.add_argument('-t','--tempdir',required=True,
        help='Temporary directory to use for put operation')
parser.add_argument('-b','--bucketnum',required=True,
        help='Bucket number')
parser.add_argument('-e','--env',required=True,
                help='Snowflake environemnt')
parser.add_argument('-u','--usern',required=True,
        help='Snowflake user to connect')
parser.add_argument('-w','--wh',required=True,
        help='Snowflake warehouse to use')
parser.add_argument('-r','--role',required=True,
        help='Snowflake role')
args=parser.parse_args()
reqid=args.reqid
put_cmd=args.cmd
conn=-1
temp_dir=args.tempdir
bucket_num=args.bucketnum
sf_env=args.env
sf_user=args.usern
sf_wh=args.wh
sf_role=args.role
TEMPDIR=f"{temp_dir}/TMP_{bucket_num}"
out=os.popen(f'mkdir -p {TEMPDIR}').read()
os.environ['TEMPDIR']=TEMPDIR
os.environ['TMPDIR']=TEMPDIR
os.environ['TMP']=TEMPDIR
sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","PERFORM_PUT")
conn3=open_sf_connection(sf_env,sf_user,sf_wh,sf_role,query_tag=sf_query_tag)
conn=-1
rc=execute_qry(conn3, put_cmd, reqid, conn)
conn3.close()
print(str(rc))
