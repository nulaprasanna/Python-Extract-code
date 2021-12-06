#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_orcl_aud_fix.py                                                                                                   #
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Nov-2020                Initial Version                                                 #
#########################################################################################################################
import argparse,pdb
import os, re, string, subprocess, sys
from datetime import datetime
from datetime import timedelta
import time
import faulthandler; faulthandler.enable()
import math
from metadata_handler import *
from operator import itemgetter, attrgetter
from ConfigObject import ConfigObject
import cx_Oracle

## Main program starts here
parser = argparse.ArgumentParser(description='Script to Peform Audit fix for Oracle type JCT',
    epilog='Example: python DIY_orcl_aud_fix.py -e env_type -j job_stream_id')
parser.add_argument('-e','--env',
	help='Envrionment type (TS*/DV*/PRD) of Job that is already ingested')
parser.add_argument('-j','--jobstreamid',
	help='Existing job stream id which you want to ingest')
args=parser.parse_args()
envType=args.env
jct=args.jobstreamid
repo_conn=""
src_conn=""
try:
	configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
	BASE_DIR=configFile.Config.BASE_DIR
	ftime=datetime.now().strftime('%Y%m%d%H%M%S')
	reqid=str(ftime)
	rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
	LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
	os.mkdir(LOGS_DIR)
	log_file=LOGS_DIR + 'diy_orcl_aud_fix.log'
	err_file=OGS_DIR + 'diy_orcl_aud_fix.err'
	step_name='Opening Repository Connection'
	log_data(log_file,step_name,1)
	repo_conn = open_oracle_connection(envType,None,True)
	step_name='Validating JCT'
	log_data(log_file,step_name,1)
	jct_obj=validate_jct(repo_conn,jct,reqid,envType)
	if type(jct_obj) is str:
		raise Exception(f"Exception {jct_obj} encountered while querying JCT details")
	step_name='Opening Source DB Connection'
	log_data(log_file,step_name,1)
	db_type=jct_obj["db_type"]
	if db_type == 'ORACLE':
		src_conn=cx_Oracle.connect(jct_obj['SOURCE_LOGIN'], jct_obj['SOURCE_LOGIN_PASSWORD'], jct_obj['SOURCE_SERVICE_NAME'])
	step_name='Opening Snowflake Connection'
	log_data(log_file,step_name,1)
	currOsUser=c_getuser()
	if configFile.Config.PRDSUFF in envType:
		sfEnv = 'cisco.us-east-1'
	elif configFile.Config.STGSUFF in envType:
		sfEnv = 'ciscostage.us-east-1'
	elif configFile.Config.DEVSUFF in envType:
		sfEnv = 'ciscodev.us-east-1'
	sf_dict=fetch_sf_conn_values(jct_obj['tdc'],envType,repo_conn,reqid,jct_dict["brjct"])
	if type(sf_dict) is str:
		raise Exception(f"Exception {sf_dict} encountered while querying SF connection details")
	sf_query_tag=f"Module=AUDIT_FIX;User={currOsUser};Table={sf_dict['target_db_name']}.{sf_dict['target_schema']}.{sf_dict['target_table_name']}"
	sf_conn=open_sf_connection(sfEnv,sf_dict['CONNECTION_USER'],sf_dict['sf_warehouse'],sf_dict['sf_role'],sf_dict['target_db_name'],sf_dict['target_schema'],query_tag=sf_query_tag)

except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	return(f"Exception - {e} occurred at line {linenu} step {step_name} while performing audit fix")