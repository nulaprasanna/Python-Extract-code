#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_orcl_aud_fix.py                                                                                                   #
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Nov-2020                Initial Version                                                 #
# 1.1           Manick          Nov-2020                Integrate date column to drill audit                            #
# 1.2           Manick          Nov-2020                Include capability to pass override column                      #
# 1.3           Manick          Dec-2020                Include logic for TD                                            #
# 1.4           Manick          Feb-2021                Include logic for Hana                                          #
# 1.5           Manick          Mar-2021                Push aud fix to EDW_AUD_FIX job group while fix in progress     #
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
from audit_meta import *

## Main program starts here
parser = argparse.ArgumentParser(description='Script to Peform Audit fix for Oracle type JCT',
    epilog='Example: python DIY_orcl_aud_fix.py -e env_type -j job_stream_id -a email_address -n column_name')
parser.add_argument('-e','--env',required=True,
	help='Envrionment type (TS*/DV*/PRD) of Job that is already ingested')
parser.add_argument('-j','--jobstreamid',required=True,
	help='Existing job stream id which you want to ingest')
parser.add_argument('-a','--emailAdd',required=True,
	help='Email address for notifications')
parser.add_argument('-n','--colName',
	help='Drill by specific column if desired')
args=parser.parse_args()
envType=args.env
jct=args.jobstreamid
emailAdd=args.emailAdd
colName=args.colName
repo_conn=""
src_conn=""
sf_conn=""
try:
	configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
	BASE_DIR=configFile.Config.BASE_DIR
	ftime=datetime.now().strftime('%Y%m%d%H%M%S')
	reqid=str(ftime)
	print(f"reqid is {reqid}")
	rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
	LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
	os.mkdir(LOGS_DIR)
	log_file=LOGS_DIR + 'diy_orcl_aud_fix.log'
	err_file=LOGS_DIR + 'diy_orcl_aud_fix.err'
	step_name='Opening Repository Connection'
	log_data(log_file,step_name,1)
	repo_conn = open_oracle_connection(envType,None,True)
	step_name='Validating JCT'
	log_data(log_file,step_name,1)
	jct_obj=validate_jct(repo_conn,jct,reqid,envType)
	if type(jct_obj) is str:
		raise Exception(jct_obj)
	step_name='Opening Source DB Connection'
	log_data(log_file,step_name,1)
	db_type=jct_obj["db_type"]
	if db_type == 'ORACLE':
		src_conn=cx_Oracle.connect(jct_obj['SOURCE_LOGIN'], jct_obj['SOURCE_LOGIN_PASSWORD'], jct_obj['SOURCE_SERVICE_NAME'])
	elif db_type == 'TERADATA':
		src_conn=teradatasql.connect(host=jct_obj['SOURCE_HOST'],user=jct_obj['SOURCE_LOGIN'], password=jct_obj['SOURCE_LOGIN_PASSWORD'])
	elif db_type == 'HANA':
		hostName=jct_obj['SOURCE_HOST'].split(';')[0].split(':')[0]
		if 'SOURCE_ADDITIONAL_PARAMS' in jct_obj:
			src_conn = open_hana_connection(hostName,jct_obj['SOURCE_PORT'],jct_obj['SOURCE_LOGIN'],jct_obj['SOURCE_LOGIN_PASSWORD'],jct_obj['SOURCE_ADDITIONAL_PARAMS'])
		else:
			src_conn = open_hana_connection(hostName,jct_obj['SOURCE_PORT'],jct_obj['SOURCE_LOGIN'],jct_obj['SOURCE_LOGIN_PASSWORD'])
	else:
		raise Exception(f"Unsupported database type {db_type} for audit")
	if type(src_conn) is str:
		raise Exception("Could not establish connection to source db")
	step_name='Opening Snowflake Connection'
	log_data(log_file,step_name,1)
	currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
	if currOsUser == "":
		try:
			currOsUser=os.getlogin()
		except Exception as e:
			currOsUser=c_getuser()
			pass
	if configFile.Config.PRDSUFF in envType:
		sfEnv = 'cisco.us-east-1'
	elif configFile.Config.STGSUFF in envType:
		sfEnv = 'ciscostage.us-east-1'
	elif configFile.Config.DEVSUFF in envType:
		sfEnv = 'ciscodev.us-east-1'
	sf_dict=fetch_sf_conn_values(jct_obj['tdc'],envType,repo_conn,reqid,jct_obj["brjct"],jct)
	if type(sf_dict) is str:
		raise Exception(sf_dict)
	sf_query_tag=f"Module=AUDIT_FIX;User={currOsUser};Table={sf_dict['target_db_name']}.{sf_dict['target_schema']}.{sf_dict['target_table_name']}"
	sf_conn=open_sf_connection(sfEnv,sf_dict['CONNECTION_USER'],sf_dict['sf_warehouse'],sf_dict['sf_role'],sf_dict['target_db_name'],sf_dict['target_schema'],query_tag=sf_query_tag)
	if colName is not None and len(colName) > 0:
		step_name="Validating overridden column"
		result=validate_column(envType,repo_conn,reqid,jct,colName)
		if 'Success' not in result:
			raise Exception(result)
		else:
			categ=result.split("~")[1]
	step_name="Checking audit status"
	log_data(log_file,step_name,1)
	target_fqn=f"{sf_dict['target_db_name']}.{sf_dict['target_schema']}.{sf_dict['target_table_name']}"
	aud_count=fetch_aud_count(repo_conn,target_fqn,reqid,jct)
	if type(aud_count) is str:
		raise Exception(f"{aud_count}")
	if aud_count < 0:
		raise Exception(f"SF has more data than Source. Check it out")
	log_data(log_file,f"JCT {jct} has a delta of {aud_count} rows.. Hence attempting to fix the same..",1)
	if db_type == 'ORACLE':
		step_name="Checking job schedule status before triggering manual catch up load"
		log_data(log_file,step_name,1)
		job_sched_stat=check_sched_stat(repo_conn,jct,reqid)
		if job_sched_stat != 'Pass':
			raise Exception(job_sched_stat)
	step_name="Resetting JOb group to EDW_AUD_FIX"
	log_data(log_file,step_name,1)
	upd_aud_job_group(repo_conn,jct,'START',reqid)
	step_name="Running one time manual to bring latest record before fixing audit"
	log_data(log_file,step_name,1)
	out=trig_ingestion_fw(repo_conn,jct,reqid,envType,emailAdd,jct_obj["brjct"])
	if out != 'Success':
		raise Exception(out)
	step_name="Analyzing source and sf data to find filter to be used to bring missing data"
	log_data(log_file,step_name,1)
	if colName is not None and len(colName) > 0:
		mod_delta=prep_jct_4_delta_load(repo_conn,jct,reqid,log_file,err_file,envType,sf_conn,src_conn,aud_count,target_fqn,f"{categ}~{colName}",db_type)
		if "Unable to find any key" in mod_delta:
			step_name="Resetting JOb group from EDW_AUD_FIX"
			log_data(log_file,step_name,1)
			upd_aud_job_group(repo_conn,jct,'END',reqid)
			raise Exception(mod_delta)
		elif "Success" not in mod_delta:
			raise Exception(mod_delta)
		else:
			filter=mod_delta.split('~')[1]
	else:
		mod_delta=prep_jct_4_delta_load(repo_conn,jct,reqid,log_file,err_file,envType,sf_conn,src_conn,aud_count,target_fqn,'NUMBER',db_type)
		if "Success" not in mod_delta:
			log_data(log_file,f"Attempting to fix audit by number column resulted in {mod_delta}. Hence attempting to fix by date column",1)
			mod_delta=prep_jct_4_delta_load(repo_conn,jct,reqid,log_file,err_file,envType,sf_conn,src_conn,aud_count,target_fqn,'DATE',db_type)
			if "Success" not in mod_delta:
				raise Exception(mod_delta)
			else:
				filter=mod_delta.split('~')[1]
		else:
			filter=mod_delta.split('~')[1]
	#print(vfilter)
	step_name="Updating JCT to perform filtered data load"
	log_data(log_file,step_name,1)
	out=upd_jct_4_with_filter(repo_conn,jct,reqid,filter,jct_obj["brjct"],envType,emailAdd,target_fqn,log_file)
	if out == 'Success':
		log_data(log_file,f"Succesfull completion of audit fix for jct {jct}",1)
		step_name="Resetting JOb group from EDW_AUD_FIX"
		log_data(log_file,step_name,1)
		upd_aud_job_group(repo_conn,jct,'END',reqid)
	else:
		raise Exception(out)

except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	if type(repo_conn) == cx_Oracle.Connection:
		excep_msg=str(e).replace("'","''")
		sql=f"update diy_aud_auto_fix_filter set status='{excep_msg}',end_time=sysdate where reqid={reqid} and job_stream_id='{jct}' and status is null"
		execute_qry(repo_conn, sql, reqid, repo_conn)
		execute_qry(repo_conn, "commit", reqid, repo_conn)
	log_data(err_file,f"Exception - {e} occurred at line {linenu} step {step_name} while performing audit fix",1)

finally:
	if type(repo_conn) == cx_Oracle.Connection:
		repo_conn.close()
	if type(src_conn) == cx_Oracle.Connection:
		src_conn.close()
	if type(sf_conn) == snowflake.connector.connection.SnowflakeConnection:
		sf_conn.close()
