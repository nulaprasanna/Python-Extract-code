#!/apps/python/install/bin/python
#########################################################################################################################
# unload_hana_nrt_tbl.py                                                                                                #
# Script to extract data from Hana table                                                                                #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          May-2020                Initial version                                                 #
# 1.1           Manick          May-2020                Open and close oracle connection within thread                  #
# 1.2           Manick          May-2020                Use 4 servers in round robin for downlaod                       #
# 1.3           Manick          May-2020                pass batch number as filter                                     #
# 1.4           Manick          May-2020                Thread is giving problem so invoke as seperate script           #
# 1.5           Manick          May-2020                Include create stage table step                                 #
# 1.6           Manick          May-2020                Raise exception if some batches left in I status for cleanup    #
# 1.7           Manick          Sep-2020                Enable SF query tag                                             #
#########################################################################################################################

import sys
import csv
import os
import math
from ConfigObject import ConfigObject
from metadata_handler import *
import pdb,random,string
import threading,calendar,subprocess
from datetime import datetime
thread_errors = [] #global list

def unload_hana_data(batch_number,run_id):
	global log_file,err_file,step_name
	try:
		Err=''
		Out=''
		run_cmd=f"/apps/edwsfdata/python/scripts/unload_hana_nrt_tbl_worker.py -b '{batch_number}' -i '{run_id}'"
		log_data(log_file,run_cmd,0)
		cmnd=subprocess.Popen(run_cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		pid=cmnd.pid
		step_name=f"Invoking hana unload worker for batch {batch_number} with pid {pid}"
		log_data(log_file,step_name,0)
		(output, error) = cmnd.communicate()
		Err=error.decode('ascii')
		Out=output.decode('ascii')
		#out=os.popen(f'rm -rf {TEMPDIR}').read()
		if (Err is not None and len(Err)>0) or 'Exception occurred at line' in str(Out):
			err_msg=f"Exception encountered while downloading data for batch {batch_number}. Err is {Err} and Out is {Out}"
			raise Exception(f"{err_msg}. Exeception is {Err}")
	except Exception as e:
		log_data(err_file,f"Failure at step {step_name}. Please find below exception",1)
		log_data(err_file,str(e),1)
		thread_errors.append([batch_number,str(e)])
		raise

def recreate_stg_tbl(conn3,tgt_db_name,tgt_schema,tgt_table):
	global conn,reqid,step_name,log_file,envType,vsrcSchema,tableName,dbName,sql,srcBaseschema,srcBasetable
	who_cols="EDWSF_CREATE_DTM varchar(16777216),EDWSF_CREATE_USER varchar(16777216),EDWSF_UPDATE_DTM varchar(16777216),EDWSF_UPDATE_USER varchar(16777216),EDWSF_BATCH_ID varchar(16777216)"
	step_name=f"Fetching column details from repos for table {dbName}.{vsrcSchema}.{tableName}"
	log_data(log_file,step_name,0)
	sql=f"select column_name from EDS_DATA_CATALOG.EDW_TABLE_COLUMN where table_name='{srcBasetable}' and db_instance_name='{dbName}'"
	sql+=f" and db_schema_name='{srcBaseschema}' and environment_name='{envType}' and active_flag='A' ORDER BY column_sequence"
	col_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(col_obj) == 0:
		raise Exception(f"Could not query column metadata for {dbName}.{vsrcSchema}.{tableName}")
	if tgt_schema != "STG":
		raise Exception(f"Invalid target schema {tgt_schema}. It has to be STG")
	step_name=f"creating table {tgt_db_name}.{tgt_schema}.{tgt_table}"
	log_data(log_file,step_name,0)
	cr_table_stmt=f'create or replace table {tgt_db_name}.{tgt_schema}.{tgt_table} ('
	for index,row in col_obj.iterrows():
		cr_table_stmt+=f"\"{row['column_name']}\" varchar(16777216),"
	cr_table_stmt += who_cols
	#cr_table_stmt=cr_table_stmt[:-1] ## strip off last comma
	cr_table_stmt += ")"
	execute_qry(conn3,cr_table_stmt,reqid,conn)

## Main program starts here
configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
BASE_DIR=configFile.Config.BASE_DIR
talendbase=BASE_DIR + '/talend'
tesbase=talendbase + '/TES'
envType=os.environ['RUN_ENVIRONMENT']
connectionName=os.environ['SOURCE_DB_CONNECTION']
dbName=os.environ['SOURCE_DB_NAME']
tableName = os.environ['SOURCE_TABLE_NAME']
vPath=os.environ['SRC2STG_FF_PATH']
vFile=os.environ['CURRENT_SRC2STG_FF_NAME']
vDelim=os.environ['SRC2STG_FF_DELIMITER']
vsrcQuery=os.environ['SOURCE_QUERY']
vsrcSchema=os.environ['SOURCE_SCHEMA']
vWhere=os.environ['WHERE_CLAUSE']
vBatch=os.environ['BATCH_ID']
vInccol=os.environ['INCREMENTAL_COLUMN_NAME']
vExttype=os.environ['EXTRACT_TYPE']
vFromdtm=os.environ['FROM_EXTRACT_DTM']
vTodtm=os.environ['TO_EXTRACT_DTM']
vFromid=os.environ['FROM_EXTRACT_ID']
vToid=os.environ['TO_EXTRACT_ID']
reqid=os.environ['REQID']
jct=os.environ['JOB_STREAM_ID']
currtime=datetime.now().strftime('%Y%m%d%H%M%S')
concurrent_nrt_batch_count=int(configFile.Config.concurrent_nrt_batch_count)
env=envType
try:
	tableName=tableName.replace('"','') ## remove double quotes
	vsrcSchema=vsrcSchema.replace('"','') ## remove double quotes
	step_name="Connecting to Repos DB"
	conn = open_oracle_connection(envType,None,True)
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	srcdbParams={}
	cursor1=conn.cursor()
	step_name=f"Creating BTP log and err file"
	sql=f"select logs_dir from diy_master where reqid={reqid}"
	logs_dir_obj=execute_df_qry(conn, sql, reqid, conn)
	logs_dir=logs_dir_obj["logs_dir"][0]
	if not os.path.exists(os.path.dirname(logs_dir)):
		raise Exception("Unable to locate logs_dir. Please check")
	log_file=f"{logs_dir}/{reqid}_nrtload_{vBatch}.log"
	err_file=f"{logs_dir}/{reqid}_nrtload_{vBatch}.err"
	log_data(log_file,step_name,0)
	step_name="Fetching source DB params"
	log_data(log_file,step_name,0)
	sql="select parameter_name,to_char(parameter_value) from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "' and parameter_type ='" + connectionName + "'"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	if len(results) == 0:
		msg="Could not find connection details for connection name - " + connectionName.strip('\n') + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion"
		raise Excepiton (msg)
	for obj in results:
		param=obj[0]
		value=obj[1]
		srcdbParams[param]=value
	#print(srcdbParams)
	step_name="Fetching source db password"
	log_data(log_file,step_name,0)
	sql2="SELECT UTL_I18N.RAW_TO_CHAR('"+ srcdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') from dual"
	cursor1.execute(sql2)
	results=cursor1.fetchall()
	for passobj in results:
		srcPassword=passobj[0]
	
	# output each table content to a separate CSV file
	step_name="Querying DIY BTP table"
	log_data(log_file,step_name,0)
	step_name=f"Querying BTP for reqid {reqid} and batch_id {vBatch}"
	log_data(log_file,step_name,0)
	sql=f"select batch_number,src_rows from DIY_HANA_NRT_BATCH_LIST where reqid={reqid} and batch_id={vBatch} and run_status in ('I') order by src_rows desc"
	btp_obj=execute_df_qry(conn,sql,reqid,conn)
	if type(btp_obj) is int or len(btp_obj) != 0:
		log_data(log_file,f"Looks like previous unload failed and few batches in Limbo. Cleanup and retry.. ",0)
		REVSRC=sql
		raise Exception("Looks like previous unload failed and few batches in Limbo. Cleanup and retry.. ")
	sql=f"select batch_number,src_rows from DIY_HANA_NRT_BATCH_LIST where reqid={reqid} and batch_id={vBatch} and run_status in ('P','R') order by src_rows desc"
	btp_obj=execute_df_qry(conn,sql,reqid,conn)
	if type(btp_obj) is int or len(btp_obj) == 0:
		log_data(log_file,f"Dont find any eligible batch to process.. ",0)
		sys.exit(0)
	step_name="Establishing remote db connection"
	hostName=srcdbParams['SOURCE_HOST'].split(';')[0].split(':')[0]
	if 'SOURCE_ADDITIONAL_PARAMS' in srcdbParams:
		conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword,srcdbParams['SOURCE_ADDITIONAL_PARAMS'])
	else:
		conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword)
	if type(conn2) is int:
		raise Exception ("Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'])
	cursor2=conn2.cursor()
	step_name=f"Fetching base table and base schema"
	log_data(log_file,step_name,0)
	sql=f"select base_schema_name,base_object_name from SYS.object_dependencies where dependent_object_name = '{tableName}'"
	sql+=f" and dependent_schema_name='{vsrcSchema}' and base_object_type='TABLE' and base_schema_name not like '%SYS%'"
	sql+=f" AND   BASE_OBJECT_NAME <> 'BATCHES_TO_PUBLISH'"
	cursor2.execute(sql)
	results=cursor2.fetchall()
	if len(results) != 1:
		raise Exception (f"Could not retrieve base table/schema for view on schema {vsrcSchema} - table {tableName}")
	else:
		for results_obj in results:
			srcBaseschema=results_obj[0]
			srcBasetable=results_obj[1]
	conn2.close()
	os.environ['BASE_SCHEMA']=srcBaseschema
	os.environ['BASE_TABLE']=srcBasetable
	step_name="Establishing Sf connection"
	log_data(log_file,step_name,0)
	sql=f"select sf_role,sf_warehouse,target_db_name,target_schema,target_table_name from edw_job_streams where job_stream_id='{jct}'"
	sf_conn_obj=execute_df_qry(conn, sql, reqid, conn)
	sf_user='EDW_DATALAKE_SVC'
	sf_wh=sf_conn_obj["sf_warehouse"][0]
	sf_role=sf_conn_obj["sf_role"][0]
	tgt_db_name=sf_conn_obj["target_db_name"][0]
	tgt_schema=sf_conn_obj["target_schema"][0]
	tgt_table=sf_conn_obj["target_table_name"][0]
	sfDict={"sfwh":sf_wh,"sfrole":sf_role,"dbname":tgt_db_name,"schemaname":tgt_schema}
	step_name="Now establishing connection to SF"
	log_data(log_file,step_name,0)
	if configFile.Config.PRDSUFF in env:
		sfEnv = 'cisco.us-east-1'
	elif configFile.Config.STGSUFF in env:
		sfEnv = 'ciscostage.us-east-1'
	elif configFile.Config.DEVSUFF in env:
		sfEnv = 'ciscodev.us-east-1'
	os.environ['SF_ENV']=sfEnv
	sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","UNLOAD_HANA_NRT_TBL")
	conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,query_tag=sf_query_tag)
	if type(conn3) is int:
		raise Exception ("Unable to connect to Snowflake DB")
	step_name="Recreating stage table"
	log_data(log_file,step_name,0)
	run_cnt=0
	sql=f"select count(*) cnt from DIY_HANA_NRT_BATCH_LIST where reqid={reqid} and batch_id= {vBatch} and run_status in ('A','C')"
	batch_run_obj=execute_df_qry(conn, sql, reqid, conn)
	run_cnt=batch_run_obj["cnt"][0]
	if run_cnt == 0:
		step_name=f"Recreating stage table"
		log_data(log_file,f"At step {step_name}",0)
		recreate_stg_tbl(conn3,tgt_db_name,tgt_schema,tgt_table)
	step_name="Forming thread array"
	log_data(log_file,step_name,0)
	thread_array=[]
	run_id=0
	vsrcQuery=vsrcQuery + str(vBatch) + " as EDWSF_BATCH_ID "
	for index,row in btp_obj.iterrows():
		batch_number=row["batch_number"]
		run_id += 1
		thread_array.append(threading.Thread(target=unload_hana_data, args=(batch_number,run_id)))
	loop_cnt=1
	tot_work_count=len(thread_array)
	cum_loop_cnt=0
	step_name="Launching workers"
	log_data(log_file,step_name,0)
	ins_while_flag=False
	for worker in thread_array:
		t=worker.start()
		time.sleep(1) ## sleep 1 second between each thread invocation
		main_thread = threading.currentThread()
		while loop_cnt >= concurrent_nrt_batch_count:
			loop_cnt=threading.active_count()-1
			ins_while_flag=True
			time.sleep(2) ## Sleep 2 seconds before checking curreng active threads
		cum_loop_cnt+=1
		if cum_loop_cnt >= tot_work_count:
			log_data(log_file,"Done with launching all workers. Waiting for final set to complete",0)
			for t in threading.enumerate():
				if t is main_thread:
					continue
				log_data(log_file,f"Waiting for {t.getName()} to complete",0)
				t.join()
		if not ins_while_flag:
			loop_cnt+=1
			ins_while_flag=False
		loop_cnt+=1
	## End of launching all threads
	if len(thread_errors) > 0: #check if there are any errors
		for e in thread_errors:
			err_msg=f"{e[1]} occurred in thread: {e[0]}"
			log_data(err_file,err_msg,1)
		raise Exception(f"Some of threads failed while unloading. Check {err_file} for details")
	else:
		log_data(log_file,f"Succesfully unloaded all batches for reqid {reqid}",1)
	time.sleep(120) ## sleep 120 seconds before checking for pending batch upload
	sql=f"select attribute1 cnt from DIY_HANA_NRT_BATCH_LIST where reqid={reqid} and batch_id= {vBatch} and run_status='I'"
	pend_obj=execute_df_qry(conn,sql,reqid,conn)
	pend_batch_cnt=len(pend_obj)
	sleep_cnt=0
	while (pend_batch_cnt > 0) and (sleep_cnt < 30):
		log_data(log_file,f"{pend_batch_cnt} batch upload are still in WIP.. Hence waiting for them to finish ",0)
		log_data(log_file,f"Running pids are  \n {pend_obj}",0)
		sleep_cnt+=1
		time.sleep(120)
		pend_obj=execute_df_qry(conn,sql,reqid,conn)
		pend_batch_cnt=len(pend_obj)
	if sleep_cnt >=30:
		if pend_batch_cnt > 0:
			log_data(err_file,"Even after waiting for 1 hour batch upload still pending. Hence giving up",1)
			raise Exception("Even after waiting for 1 hour batch upload still pending. Hence giving up")
	if isinstance(conn3,snowflake.connector.connection.SnowflakeConnection):
		conn3.close()
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Exception - {e} occurred at line {linenu} step {step_name} while downloading data")
	print("Error occurred while running to source query - " + REVSRC)
	sys.exit(7)



