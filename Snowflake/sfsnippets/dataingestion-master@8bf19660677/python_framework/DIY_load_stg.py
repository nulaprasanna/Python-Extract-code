#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_load_stg.py                                                                                                       #
# Script to extract data from source and upload to stage layer                                                          #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Mar-2020                Initial Version                                                 #
# 1.1           Manick          Apr-2020                Convert SOURCE_QUERY to CLOB type and log error in DIY_Master   #
# 1.2           Manick          Apr-2020                Include HANA download script                                    #
# 1.3           Manick          Apr-2020                If all child streams are complete (merge fail) upd JCT to C     #
# 1.4           Manick          May-2020                Set REQID for unload TD table array to work                     #
# 1.5           Manick          May-2020                Enaha HANA-NRT                                                  #
# 1.6           Manick          May-2020                Parallelize split and control threads for zip for HANANNRT      #
# 1.7           Manick          May-2020                Pass source row count for HANANRT uplaod process                #
# 1.8           Manick          May-2020                Resize WH before calling split load and update DIY_Params       #
# 1.9           Manick          Jun-2020                Enable Snowflake                                                #
# 2.0           Manick          Jun-2020                Change hard coded username of EDW_DATALAKE_SVC                  #
# 2.1           Manick          Aug-2020                Always use XSmall wh for upload operation                       #
# 2.2           Manick          Aug-2020                Introduce sf upload while download for Oracle in progress       #
# 2.3           Manick          Sep-2020                Introduce sf upload while download for TD in progress           #
# 2.4           Manick          Sep-2020                Introduce sf upload while dowload for Hana in progress          #
# 2.5           Manick          Jan-2021                Explicit SF connection close methods                            #
# 2.6           Manick          Jun-2021                New version with additional comments for easy readability       #
#########################################################################################################################
import argparse,pdb
import snowflake.connector
import cx_Oracle
import os, re, sys
from datetime import datetime
from datetime import timedelta
import time
from ConfigObject import ConfigObject
from metadata_handler import *
import subprocess,pdb
import threading,calendar,psutil

# Method to update run status in EDW_JOB_STREAMS table

def upd_jct_status(run_status):
	global conn,jct,reqid,step_name,log_file,step_name,sql
	step_name=f"Updating edw_job_stream status to {run_status}"
	log_data(log_file,step_name,0)
	sql=f"update edw_job_streams set run_status='{run_status}' where job_stream_id='{jct}'"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)

## Method to insert or update record into checkpoint table for restartability

def ins_checkpoint(phase,val):
	global conn,jct,reqid,step_name,log_file,sql
	if phase == 'Insert':
		step_name=f"Inserting diy_src2stg_checkpoint"
		log_data(log_file,step_name,0)
		sql=f"delete from diy_src2stg_checkpoint where reqid={reqid} and job_stream_id='{jct}'"
		execute_qry(conn, sql, reqid, conn)
		sql=f"Insert into diy_src2stg_checkpoint (reqid,job_stream_id,current_phase,timestamp) values "
		sql+=f"({reqid},'{jct}','{val}',sysdate)"
		execute_qry(conn, sql, reqid, conn)
	elif phase == 'Update':
		step_name=f"Updating diy_src2stg_checkpoint"
		log_data(log_file,step_name,0)
		sql=f"update diy_src2stg_checkpoint set current_phase='{val}',timestamp=sysdate where reqid={reqid} and job_stream_id='{jct}'"
		execute_qry(conn, sql, reqid, conn)
	execute_qry(conn, "commit", reqid, conn)

# Method to insert or update job stream run table

def ins_jct_run_status(phase):
	global conn,jct,reqid,step_name,log_file,sql
	if phase == 'Insert':
		step_name=f"Inserting edw_job_stream_runs"
		log_data(log_file,step_name,0)
		sql=f"INSERT INTO EDW_JOB_STREAM_RUNS (job_stream_id, job_stream_start_time, from_extract_dtm,to_extract_dtm) "
		sql+=f" SELECT job_stream_id,sysdate,from_extract_dtm,to_extract_dtm FROM EDW_JOB_STREAMS WHERE JOB_STREAM_ID = '{jct}'"
	elif phase == 'Update':
		step_name=f"updating edw_job_stream_runs"
		log_data(log_file,step_name,0)
		sql=f"UPDATE EDW_JOB_STREAM_RUNS SET job_stream_end_time = sysdate "
		sql+=f" WHERE (job_stream_id,job_stream_start_time) in (SELECT job_stream_id,MAX(job_stream_start_time) FROM edw_job_stream_runs "
		sql+=f" WHERE job_stream_id = '{jct}' GROUP BY job_stream_id)"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)

## Method to write into params file as well insert into params table.

def pull_param(jct):
	global conn,reqid,step_name,log_file,env,src_base_schema,src_base_tbl,job_group_id,batch_id,config_file,sql
	step_name=f"Framing query for extracting Job parameters for {jct}"
	log_data(log_file,step_name,0)
	sql="SELECT "
	sql +=f" 'REQID;' || {reqid} || CHR(10)"
	sql +=f"||'JOB_STREAM_ID;'||JOB_STREAM_ID||CHR(10)"
	sql +=f"||'JOB_GROUP_ID;'||JOB_GROUP_ID||CHR(10)"
	sql +=f"||'JOB_STREAM_SEQ_NUM;'||JOB_STREAM_SEQ_NUM||CHR(10)"
	sql +=f"||'ACTIVE_IND;'||ACTIVE_IND||CHR(10)"
	sql +=f"||'RUN_STATUS;'||RUN_STATUS||CHR(10)"
	sql +=f"||'WORKFLOW_TYPE;'||WORKFLOW_TYPE||CHR(10)"
	sql +=f"||'SOURCE_DB_CONNECTION;'||SOURCE_DB_CONNECTION||CHR(10)"
	sql +=f"||'SOURCE_DB_NAME;'||SOURCE_DB_NAME||CHR(10)"
	sql +=f"||'SOURCE_SCHEMA;\"'||SOURCE_SCHEMA||'\"'||CHR(10)"
	sql +=f"||'SOURCE_TABLE_NAME;\"'||SOURCE_TABLE_NAME||'\"'||CHR(10)"
	sql +=f"||'EXTRACT_TYPE;'||EXTRACT_TYPE||CHR(10)"
	sql +=f"||'MAX_VALUE_FROM;'||MAX_VALUE_FROM||CHR(10)"
	sql +=f"||'INCREMENTAL_COLUMN_NAME;'||INCREMENTAL_COLUMN_NAME||CHR(10)"
	sql +=f"||'FROM_EXTRACT_DTM;'||TO_CHAR(NVL(FROM_EXTRACT_DTM,SYSDATE),'YYYY/MM/DD HH24:MI:SS')||CHR(10)"
	sql +=f"||'TO_EXTRACT_DTM;'||TO_CHAR(NVL(TO_EXTRACT_DTM,SYSDATE),'YYYY/MM/DD HH24:MI:SS')||CHR(10)"
	sql +=f"||'FROM_EXTRACT_ID;'||NVL(FROM_EXTRACT_ID,1)||CHR(10)"
	sql +=f"||'TO_EXTRACT_ID;'||NVL(TO_EXTRACT_ID,1)||CHR(10)"
	sql +=f"||'PREVIOUS_FROM_EXTRACT_DTM;'||TO_CHAR(PREVIOUS_FROM_EXTRACT_DTM,'YYYY/MM/DD HH24:MI:SS')||CHR(10)"
	sql +=f"||'PREVIOUS_TO_EXTRACT_DTM;'||TO_CHAR(PREVIOUS_TO_EXTRACT_DTM,'YYYY/MM/DD HH24:MI:SS')||CHR(10)"
	sql +=f"||'PREVIOUS_FROM_EXTRACT_ID;'||PREVIOUS_FROM_EXTRACT_ID||CHR(10)"
	sql +=f"||'PREVIOUS_TO_EXTRACT_ID;'||PREVIOUS_TO_EXTRACT_ID||CHR(10)"
	sql +=f"||'TARGET_DB_CONNECTION;'||TARGET_DB_CONNECTION||CHR(10)"
	sql +=f"||'TARGET_DB_NAME;'||TARGET_DB_NAME||CHR(10)"
	sql +=f"||'TARGET_SCHEMA;'||TARGET_SCHEMA||CHR(10)"
	sql +=f"||'TARGET_TABLE_NAME;'||TARGET_TABLE_NAME||CHR(10)"
	sql +=f"||'SF_WAREHOUSE;'||SF_WAREHOUSE||CHR(10)"
	sql +=f"||'SF_ROLE;'||SF_ROLE||CHR(10)"
	sql +=f"||'SRC2STG_FF_PATH;/users/'||sys_context('USERENV','OS_USER')||'/{env}/'||JOB_GROUP_ID||'/data/inbox/'||CHR(10)"
	sql +=f"||'CURRENT_SRC2STG_FF_NAME;'||JOB_STREAM_ID||'_'||'{batch_id}'||CHR(10)"
	sql +=f"||'SRC2STG_FF_DELIMITER;'||SRC2STG_FF_DELIMITER||CHR(10)"
	sql +=f"||'MERGE_TYPE;'||MERGE_TYPE||CHR(10)"
	sql +=f"||'JOB_STREAM_ID_LINK;'||JOB_STREAM_ID_LINK||CHR(10)"
	sql +=f"||'CONFIG_PATH;/users/'||sys_context('USERENV','OS_USER')||'/{env}/'||JOB_GROUP_ID||'/config/'||CHR(10)"
	sql +=f"||'BATCH_ID;' || '{batch_id}'||CHR(10)"
	sql +=f"||'SYSTEM_CALL;'||SYSTEM_CALL||CHR(10)"
	sql +=f"||'SOURCE_QUERY;'||'SELECT '||EDS_DATA_CATALOG.FN_GET_COLUMN_LIST_NEW('{env}',SOURCE_DB_NAME,'{src_base_schema}','{src_base_tbl}')||CHR(10)"
	sql +=f"||'WHERE_CLAUSE;'||CASE WHEN TRIM(WHERE_CLAUSE) IS NOT NULL THEN ' WHERE '||WHERE_CLAUSE||' ' WHEN (TRIM(WHERE_CLAUSE) IS NULL AND EXTRACT_TYPE IN ('DATE','ID')) THEN ' WHERE ' ELSE ' ' END||CHR(10)"
	sql +=f"||'CONNECTION_TYPE;'||(SELECT distinct decode(DB_TYPE,'HANANRT','HANA',DB_TYPE) FROM EDS_DATA_CATALOG.EDW_DB_INSTANCE WHERE DB_INSTANCE_NAME = SOURCE_DB_NAME AND ENVIRONMENT_NAME='{env}')||CHR(10)"
	sql +=f"||'ENVIRONMENT_NAME;'||'{env}'||CHR(10) as params"
	sql +=f" FROM EDW_JOB_STREAMS"
	sql +=f" WHERE JOB_STREAM_ID = '{jct}'"
	step_name=f"Executing query for extracting Job parameters for {jct}"
	log_data(log_file,step_name,0)
	param_obj=execute_df_qry(conn, sql, reqid, conn)
	job_params=str(param_obj["params"][0])
	step_name=f"Writing params to config file"
	log_data(log_file,step_name,0)
	param_file=open(config_file,'w')
	param_file.write(job_params)
	param_file.write(f'INBOX_DATA_FILE_NAME;/apps/edwsfdata/{env}/{job_group_id}/data/inbox/{jct}_{batch_id}')
	param_file.close()
	step_name=f"inserting params to table"
	log_data(log_file,step_name,0)
	sql=f"delete from diy_job_params where reqid={reqid} and job_stream_id='{jct}'"
	execute_qry(conn,sql,reqid,conn)
	cursor=conn.cursor()
	sql=f"insert into diy_job_params(reqid,job_stream_id,job_group_id,JOB_STREAM_SEQ_NUM,ACTIVE_IND,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
	sql+=f",source_table_name,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id"
	sql+=f",previous_from_extract_dtm,previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name"
	sql+=f",target_schema,target_table_name,sf_warehouse,sf_role,SRC2STG_FF_PATH,CURRENT_SRC2STG_FF_NAME,SRC2STG_FF_DELIMITER,MERGE_TYPE,JOB_STREAM_ID_LINK"
	sql+=f",CONFIG_PATH,BATCH_ID,SYSTEM_CALL,SOURCE_QUERY,WHERE_CLAUSE,CONNECTION_TYPE,ENVIRONMENT_NAME,INBOX_DATA_FILE_NAME)"
	sql+=f" values "
	sql+=f" ( "
	param_file=open(config_file,'r')
	for line in param_file:
		par=line.split(';')[0]
		val=line.split(';')[1].rstrip('\n') ## extract only value from parameter file to insert 
		#if par in ('SOURCE_QUERY','WHERE_CLAUSE'):
		if par == 'WHERE_CLAUSE':
			val=val.replace("'","''") ## replace single quote with two single to insert data into database
		if par == 'SOURCE_QUERY':
			bind_val=val
		if par in ('FROM_EXTRACT_DTM','TO_EXTRACT_DTM','PREVIOUS_FROM_EXTRACT_DTM','PREVIOUS_TO_EXTRACT_DTM'):
			sql+=f"to_date('{val}','YYYY/MM/DD HH24:MI:SS'),"
		elif par == 'SOURCE_QUERY':
			sql+=':query,'
		else:
			sql+=f"'{val}',"
	param_file.close()
	sql=sql[:-1]
	sql+=")"
	#execute_qry(conn,sql,reqid,conn)
	cursor.prepare(sql)
	cursor.setinputsizes(query=cx_Oracle.CLOB)
	cursor.execute(sql,query=bind_val)
	cursor.execute('commit')

## Methold to upload HANA NRT table into Snowflake. This upload essentially happens from DIY_hana_nrt_interim_load.py
## Just incase if still some files are found as downloaded but not updated into SF then this method is used.

def upload_for_hananrt(flat_file,batch_number,src_rows):
	global reqid,step_name,log_file,err_file,sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,tgt_table,env,jct,sf_query_tag
	scr_2_upload=f"/apps/edwsfdata/python/scripts/DIY_split_load.py -e {env} -r {reqid} -j {jct} -f {flat_file} -s {src_rows}"
	cmnd=subprocess.Popen([scr_2_upload] ,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	pid=cmnd.pid
	step_name=f"Stage upload script launched for batch_number {batch_number} with pid {pid}"
	log_data(log_file,step_name,0)
	(output, error) = cmnd.communicate()
	Err=error.decode('ascii')
	Out=output.decode('ascii')
	if (Err is not None and len(Err)>0):
		log_data(err_file,"Exception encountered while uploading flat file to stage. Please find below the exception \n" + Err,0)
		print(Err)
		raise Exception(f"Upload to stage failed \n {Err}")
	elif  ('Error occurred' in Out) or ('Inside exception' in Out):
		log_data(err_file,"Exception encountered while uploading flat file to stage. Please find below the exception \n" + Out,0)
		print(Out)
		raise Exception(f"Upload to stage failed \n {Out}")
	step_name=f"Connecting to snowflake for batch_number {batch_number}"
	#log_data(log_file,step_name,0)
	conn3=''
	conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,query_tag=sf_query_tag)
	step_name=f"Connecting to repository for batch_number {batch_number}"
	#log_data(log_file,step_name,0)
	conn = open_oracle_connection(env,None,True)
	if type(conn) is int:
		raise Exception (f"Unable to connect to repository DB for batch {batch_number}")
	sql=f"select count(1) cnt from {tgt_db_name}.{tgt_schema}.{tgt_table} where batch_number={batch_number}"
	#log_data(log_file,sql,0)
	cnt_obj=execute_df_qry(conn3,sql,reqid,conn)
	interim_stg_cnt=cnt_obj["cnt"][0]
	step_name=f"Uploaded {interim_stg_cnt} rows for {batch_number}"
	log_data(log_file,step_name,0)
	sql=f"update DIY_HANA_NRT_BATCH_LIST set tgt_rows={interim_stg_cnt},last_update_time=sysdate,run_status='A' "
	sql+=f" where reqid={reqid} and run_status = 'U' and batch_number={batch_number}"
	step_name=f"Updating DIY_HANA_NRT_BATCH_LIST with sql {sql}"
	#log_data(log_file,step_name,0)
	execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,"commit",reqid,conn)
	step_name=f"closing connections for batch number {batch_number}"
	log_data(log_file,step_name,0)
	if isinstance(conn3,snowflake.connector.connection.SnowflakeConnection):
		conn3.close()
	conn.close()

## Everytime stage table in Snowflake gets dropped and recreated
## This method is used to create stage table based on data catalog column along with standard who columns
## All columns will be created as varchar in stage layer

def recreate_stg_tbl(conn3,tgt_db_name,tgt_schema,tgt_table):
	global conn,reqid,step_name,log_file,env,src_base_schema,src_base_tbl,source_db_name,err_file,col_name,sql
	who_cols="EDWSF_CREATE_DTM varchar(16777216),EDWSF_CREATE_USER varchar(16777216),EDWSF_UPDATE_DTM varchar(16777216),EDWSF_UPDATE_USER varchar(16777216),EDWSF_BATCH_ID varchar(16777216)"
	step_name=f"Fetching column details from repos for table {source_db_name}.{src_base_schema}.{src_base_tbl}"
	src_base_tbl=src_base_tbl.replace('"','')
	source_db_name=source_db_name.replace('"','')
	src_base_schema=src_base_schema.replace('"','')
	log_data(log_file,step_name,0)
	sql=f"select column_name from EDS_DATA_CATALOG.EDW_TABLE_COLUMN where table_name='{src_base_tbl}' and db_instance_name='{source_db_name}'"
	sql+=f" and db_schema_name='{src_base_schema}' and environment_name='{env}' and active_flag='A' ORDER BY column_sequence"
	col_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(col_obj) == 0:
		raise Exception(f"Could not query column metadata for {source_db_name}.{src_base_schema}.{src_base_tbl}")
	if tgt_schema != "STG":
		raise Exception(f"Invalid target schema {tgt_schema}. It has to be STG")
	#step_name=f"dropping table {tgt_db_name}.{tgt_schema}.{tgt_table}"
	#log_data(log_file,step_name,0)
	#sql=f"drop table {tgt_db_name}.{tgt_schema}.{tgt_table}"
	#execute_qry(conn3,sql,reqid,conn)
	step_name=f"creating table {tgt_db_name}.{tgt_schema}.{tgt_table}"
	log_data(log_file,step_name,0)
	cr_table_stmt=f'create or replace table {tgt_db_name}.{tgt_schema}.{tgt_table} ('
	for index,row in col_obj.iterrows():
		cr_table_stmt+=f"\"{row['column_name']}\" varchar(16777216),"
	cr_table_stmt += who_cols
	#cr_table_stmt=cr_table_stmt[:-1] ## strip off last comma
	cr_table_stmt += ")"
	execute_qry(conn3,cr_table_stmt,reqid,conn)

## This method will convert ORACLE's CLOB data type to string data type

def LobtoStr(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)

## This method will trigger respective platform script which will read the data from source and write to stage layer via CSV files

def ext_data_2_ff(jct):
	global conn,reqid,step_name,log_file,env,src_base_schema,src_base_tbl,job_group_id,batch_id,config_file,db_type,err_file,tblsize,datafile,file_path,logs_dir,sql
	step_name=f"setting environment for {jct}"
	log_data(log_file,step_name,0)
	cursor1=conn.cursor()
	cursor1.outputtypehandler=LobtoStr
	sql=f"select source_db_connection,source_db_name,source_table_name,target_table_name,SRC2STG_FF_PATH sfp,CURRENT_SRC2STG_FF_NAME csfn,SRC2STG_FF_DELIMITER sfd"
	sql+=f",SOURCE_QUERY query,source_schema,WHERE_CLAUSE wc,batch_id,nvl(incremental_column_name,' ') icn,extract_type,to_char(from_extract_dtm,'YYYY/MM/DD HH24:MI:SS') from_extract_dtm"
	sql+=f",to_char(to_extract_dtm,'YYYY/MM/DD HH24:MI:SS') to_extract_dtm,from_extract_id fei,to_extract_id tei from diy_job_params where job_stream_id='{jct}' and reqid={reqid}"
	#jct_param_obj=execute_df_qry(conn, sql, reqid, conn)
	cursor1.execute(sql)
	jct_obj=cursor1.fetchall()
	if len(jct_obj) != 1:
		raise Exception(f"Unable to query jobstream {jct} parameter")
	for jct_param_obj in jct_obj:
		os.environ['RUN_ENVIRONMENT']=env
		os.environ['SOURCE_DB_CONNECTION']=jct_param_obj[0]
		os.environ['SOURCE_DB_NAME']=jct_param_obj[1]
		os.environ['SOURCE_TABLE_NAME']=jct_param_obj[2]
		os.environ['TARGET_TABLE_NAME']=jct_param_obj[3]
		file_path=jct_param_obj[4]
		os.environ['SRC2STG_FF_PATH']=file_path
		file_name=jct_param_obj[5]
		os.environ['CURRENT_SRC2STG_FF_NAME']=file_name
		os.environ['SRC2STG_FF_DELIMITER']=jct_param_obj[6]
		os.environ['SOURCE_QUERY']=jct_param_obj[7]
		os.environ['SOURCE_SCHEMA']=jct_param_obj[8]
		os.environ['WHERE_CLAUSE']=jct_param_obj[9]
		os.environ['BATCH_ID']=str(jct_param_obj[10])
		if jct_param_obj[11] is not None:
			os.environ['INCREMENTAL_COLUMN_NAME']=jct_param_obj[11]
		os.environ['EXTRACT_TYPE']=jct_param_obj[12]
		if jct_param_obj[13] is not None:
			os.environ['FROM_EXTRACT_DTM']=str(jct_param_obj[13])
		if jct_param_obj[14] is not None:
			os.environ['TO_EXTRACT_DTM']=str(jct_param_obj[14])
		if jct_param_obj[15] is not None:
			os.environ['FROM_EXTRACT_ID']=str(jct_param_obj[15])
		if jct_param_obj[16] is not None:
			os.environ['TO_EXTRACT_ID']=str(jct_param_obj[16])
		os.environ['TBLSIZE']=str(tblsize)
		os.environ['JOB_GROUP_ID']=job_group_id
		os.environ['JOB_STREAM_ID']=jct
		os.environ['LOGS_DIR']=logs_dir
		os.environ['REQID']=str(reqid)
		datafile=f"{file_path}/{file_name}"
	if db_type == "ORACLE":
		step_name=f"Invoking Oracle unloader program for JCT {jct}"
		log_data(log_file,step_name,0)
		scr_2_run="/apps/edwsfdata/python/scripts/unload_oracle_tbl.py"
	elif db_type == "TERADATA":
		step_name=f"Invoking TD unloader program for JCT {jct}"
		log_data(log_file,step_name,0)
		scr_2_run="/apps/edwsfdata/python/scripts/unload_td_tbl.py"
	elif db_type == "HANA":
		step_name=f"Invoking Hana unloader program for JCT {jct}"
		log_data(log_file,step_name,0)
		scr_2_run="/apps/edwsfdata/python/scripts/unload_hana_tbl.py"
	elif db_type == "HANANRT":
		step_name=f"Invoking Hana NRT unloader program for JCT {jct}"
		log_data(log_file,step_name,0)
		sql=f"update DIY_HANA_NRT_BATCH_LIST set batch_id={batch_id} where reqid={reqid} and run_status != 'C'"
		execute_qry(conn,sql,reqid,conn)
		execute_qry(conn,"commit",reqid,conn)
		scr_2_run="/apps/edwsfdata/python/scripts/unload_hana_nrt_tbl.py"
	elif db_type == "SNOWFLAKE":
		step_name=f"Invoking Snowflake unloader program for JCT {jct}"
		log_data(log_file,step_name,0)
		scr_2_run="/apps/edwsfdata/python/scripts/unload_sf_tbl.py"
	else:
		raise Exception(f"Unsupported db type {db_type}")
	cmnd=subprocess.Popen(scr_2_run ,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	pid=cmnd.pid
	step_name=f"Unload script launched with pid {pid}"
	log_data(log_file,step_name,0)
	(output, error) = cmnd.communicate()
	ret_code=cmnd.returncode
	Err=error.decode('ascii')
	Out=output.decode('ascii')
	if ret_code != 0:
		log_data(err_file,f"Exception encountered while extracting data to flatfile from db. \n {Out} \n {Err} \n Please check logs for exception",0)
		print(Out)
		raise Exception(f"Unload table failed \n")
	if (Err is not None and len(Err)>0):
		log_data(err_file,"Exception encountered while extracting data to flatfile from db. Please find below the exception \n" + Err,0)
		print(Err)
		raise Exception(f"Unload table failed \n {Err}")
	elif  ('Error occurred' in Out) or ('Exception encountered' in Out):
		log_data(err_file,"Exception encountered while extracting data to flatfile from db. Please find below the exception \n" + Out,0)
		print(Out)
		raise Exception(f"Unload table failed \n {Out}")
	else:
		log_data(log_file,"Completed extracting data to flatfile \n" + Out,0)
		if db_type in "SNOWFLAKE":
			ins_checkpoint('Update','Complete')
		elif db_type in ("ORACLE","TERADATA","HANA"):
			out_dict=Out.split('\n')
			for line in out_dict:
				if 'Snowflake load triggered with pid' in line:
					log_data(log_file,line,0)
					sfload_pid=int(line.split(' ')[-1])
			log_data(log_file,f"Now waiting for Snowflake load running with pid {sfload_pid} to complete",0)
			while psutil.pid_exists(sfload_pid):
				time.sleep(15) ## Wake up every 15 seconds to complete if sf load has completed.
			ins_checkpoint('Update','Complete')
		else:
			ins_checkpoint('Update','Upload_stg_Data')

## Below method is triggered only for HANA NRT. For other types the code is obsolete since logic got changed during later releases
## For HANA NRT data upload batch by batch happens to Snowflake from flatfile using below procedure and framework does the same using 10 parallel threads.

def upl_ff_2_stg(jct):
	global conn,reqid,step_name,log_file,env,src_base_schema,src_base_tbl,job_group_id,batch_id,config_file,db_type,err_file,sfEnv,datafile,file_path
	global sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,sql,file_name,tgt_table,sf_query_tag
	step_name="Now uploading FF to SF Stg"
	log_data(log_file,step_name,0)
	sql=f"select sf_role,sf_warehouse,target_db_name,target_schema,target_table_name,job_group_id,target_db_connection from diy_job_params where reqid={reqid} and job_stream_id='{jct}'"
	sf_conn_obj=execute_df_qry(conn, sql, reqid, conn)
	#sf_user='EDW_DATALAKE_SVC'
	sf_wh=sf_conn_obj["sf_warehouse"][0]
	sf_role=sf_conn_obj["sf_role"][0]
	tgt_db_name=sf_conn_obj["target_db_name"][0]
	tgt_schema=sf_conn_obj["target_schema"][0]
	tgt_table=sf_conn_obj["target_table_name"][0]
	tgt_db_conn=sf_conn_obj["target_db_connection"][0]
	sql=f"select to_char(parameter_value) pv from eds_data_catalog.edw_parameter where parameter_type='{tgt_db_conn}'"
	sql+=f" and environment_name='{env}' and parameter_name='CONNECTION_USER'"
	sf_user_obj=execute_df_qry(conn, sql, reqid, conn)
	sf_user=sf_user_obj["pv"][0]
	sfDict={"sfwh":sf_wh,"sfrole":sf_role,"dbname":tgt_db_name,"schemaname":tgt_schema}
	step_name="Now establishing connection to SF"
	log_data(log_file,step_name,0)
	conn3=''
	conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,query_tag=sf_query_tag)
	if type(conn3) is int:
		raise Exception ("Unable to connect to Snowflake DB")
	if 'COMMON_WH' in sf_wh:
		wh_2_use=sf_wh
	else:
		wh_2_use='EDW_I_REF_COMMON_WH'
	sql=f"show warehouses like '{wh_2_use}'"
	wh_obj=execute_df_qry(conn3,sql, reqid, conn)
	if len(wh_obj) > 0:
		log_data(log_file,f"Switching warehouse to {wh_2_use} for upload operation",0)
		sql=f"update diy_job_params set sf_warehouse='{wh_2_use}' where reqid={reqid} and job_stream_id='{jct}'"
		execute_qry(conn,sql,reqid,conn)
		execute_qry(conn,"commit",reqid,conn)
		sf_wh=wh_2_use
	if os.path.exists(datafile):
		if os.stat(datafile).st_size == 0:
			log_data(log_file,"Zero byte file detected",0)
			return
	run_cnt=0
	if db_type == "HANANRT":
		sql=f"select count(*) cnt from DIY_HANA_NRT_BATCH_LIST where reqid={reqid} and batch_id= {batch_id} and run_status in ('A','C')"
		batch_run_obj=execute_df_qry(conn, sql, reqid, conn)
		run_cnt=batch_run_obj["cnt"][0]
	if run_cnt == 0:
		step_name=f"Recreating stage table"
		log_data(log_file,f"At step {step_name}",0)
		recreate_stg_tbl(conn3,tgt_db_name,tgt_schema,tgt_table)
	step_name=f"Invoking script for stage upload"
	log_data(log_file,f"At step {step_name}",0)
	
	if db_type == "HANANRT":
		sql=f"select batch_number,flat_file,src_rows from DIY_HANA_NRT_BATCH_LIST where reqid={reqid} and batch_id= {batch_id} and run_status='U' order by src_rows desc"
		batch_obj=execute_df_qry(conn, sql, reqid, conn)
		curr_pid=os.getpid()
		thread_array=[]
		for index,row in batch_obj.iterrows():
			batch_number=row["batch_number"]
			flat_file=row["flat_file"]
			src_rows=row["src_rows"]
			thread_array.append(threading.Thread(target=upload_for_hananrt, args=(flat_file,batch_number,src_rows)))
		loop_cnt=1
		tot_work_count=len(thread_array)
		cum_loop_cnt=0
		step_name="Launching workers"
		log_data(log_file,step_name,0)
		ins_while_flag=False
		for worker in thread_array:
			t=worker.start()
			time.sleep(0.5) ## sleep .5 seconds before each thread submission
			main_thread = threading.currentThread()
			while loop_cnt >= 10:
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
		log_data(log_file,"Completed Uploading to stage \n",0)
	## Below part is obsolete and upload happens while downloading the data
	else:
		scr_2_upload=f"/apps/edwsfdata/python/scripts/DIY_split_load.py -e {env} -r {reqid} -j {jct}"
		cmnd=subprocess.Popen([scr_2_upload] ,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		pid=cmnd.pid
		step_name=f"Stage upload script launched with pid {pid}"
		log_data(log_file,step_name,0)
		(output, error) = cmnd.communicate()
		Err=error.decode('ascii')
		Out=output.decode('ascii')
		if (Err is not None and len(Err)>0):
			log_data(err_file,"Exception encountered while uploading flat file to stage. Please find below the exception \n" + Err,0)
			print(Err)
			raise Exception(f"Upload to stage failed \n {Err}")
		elif  ('Error occurred' in Out) or ('Inside exception' in Out):
			log_data(err_file,"Exception encountered while uploading flat file to stage. Please find below the exception \n" + Out,0)
			print(Out)
			raise Exception(f"Upload to stage failed \n {Out}")
		else:
			log_data(log_file,"Completed Uploading to stage \n" + Out,0)
	conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,query_tag=sf_query_tag)
	step_name="validating count of stage table"
	log_data(log_file,f"At step {step_name}",0)
	sql=f"select count(1) cnt from {tgt_db_name}.{tgt_schema}.{tgt_table}"
	cnt_obj=execute_df_qry(conn3,sql,reqid,conn)
	stg_cnt=cnt_obj["cnt"][0]
	log_data(log_file,f"Successfully loaded {stg_cnt} rows into {tgt_db_name}.{tgt_schema}.{tgt_table}",0)
	ins_checkpoint('Update','Archive_Files')
	if isinstance(conn3,snowflake.connector.connection.SnowflakeConnection):
		conn3.close()

## Below procedure is used to archive processed csv files from data to archive mount

def arch_files(jct):
	global datafile,file_path,step_name,configFile,job_group_id,env,logs_dir,sql,file_name
	step_name="Now compressing and archiving data file"
	log_data(log_file,step_name,0)
	if env.lower() == 'prd':
		datafile_dest=configFile.Config.datafile_dest_prd
	elif 'DV' in env.upper():
		datafile_dest=configFile.Config.datafile_dest_dev
	elif 'TS' in env.upper():
		datafile_dest=configFile.Config.datafile_dest_stg
	else:
		raise Exception(f"Invalid data file archive destination")
	processes = set()
	datafile_dest=datafile_dest.replace('jobgroup',job_group_id)
	datafile_dest=datafile_dest.replace('env',env.upper())
	if db_type == "HANANRT":
		curr_pid=os.getpid()
		osPrcmd=f"ps -ef | grep {curr_pid}  | grep -v defunct | grep -v grep | wc -l"
		sql=f"select batch_number,flat_file from DIY_HANA_NRT_BATCH_LIST where reqid={reqid} and batch_id= {batch_id} and run_status='A'"
		batch_obj=execute_df_qry(conn, sql, reqid, conn)
		out=os.popen(f'mkdir -p {datafile_dest}').read()
		for index,row in batch_obj.iterrows():
			batch_number=row["batch_number"]
			flat_file=row["flat_file"]
			compress_log=open(f"{logs_dir}/compress.log",'a')
			oscmd=f"gzip {flat_file} && mv {flat_file}.gz {datafile_dest}"
			proc=subprocess.Popen([oscmd],stdout=compress_log, stderr=compress_log, shell=True)
			pid=proc.pid
			pout=processes.add(proc)
			step_name=f"Compress file for batch_number {batch_number} submitted with pid {pid}"
			log_data(log_file,step_name,0)
			sql=f"update DIY_HANA_NRT_BATCH_LIST set run_status='C',end_time=sysdate "
			sql+=f" where reqid={reqid} and run_status = 'A' and batch_id= {batch_id} and batch_number={batch_number}"
			execute_qry(conn,sql,reqid,conn)
			execute_qry(conn,"commit",reqid,conn)
			while (int(int(os.popen(osPrcmd).read())-1)) >= 10:
				time.sleep(15)  ## sleep 15 seconds if more than 10 jobs running.
	else:
		compress_log=open(f"{logs_dir}/compress.log",'w')
		out=os.popen(f'mkdir -p {datafile_dest}').read()
		oscmd=f"gzip {datafile} && mv {datafile}.gz {datafile_dest}"
		proc=subprocess.Popen([oscmd],stdout=compress_log, stderr=compress_log, shell=True)
		pid=proc.pid
		pout=processes.add(proc)
		step_name=f"Compress file script submitted with pid {pid}"
		log_data(log_file,step_name,0)

## Main Program start here
pgmVersion='1.0'
parser = argparse.ArgumentParser(description='Script used to push data to SF stg',
    epilog='Example: python DIY_load_stg.py -j jct -e environment -r reqid -s tblsize -b baseSchema -t baseTable')
parser.add_argument('-j', '--jct',required=True,
    help='SRC2STG JCT which needs to be executed ')
parser.add_argument('-e','--env',required=True,
        help='Environment for which src2stg needs to be executed (DV3/TS3/PRD)')
parser.add_argument('-r','--reqid',required=True,
        help='Reqid from DIY tables')
parser.add_argument('-s','--tblsize',required=True,
        help='Size of source table in KB')
parser.add_argument('-b','--baseSchema',required=True,
        help='Base Schema')
parser.add_argument('-t','--baseTable',required=True,
        help='Base Table')

args=parser.parse_args()
jct=args.jct
env=args.env
reqid=args.reqid
tblsize=args.tblsize
batch_id=datetime.now().strftime('%Y%m%d%H%M%S')
src_base_schema=args.baseSchema
src_base_tbl=args.baseTable
try:
	set_env()
	sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","LOAD_STG")
	configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
	conn = open_oracle_connection(env,None,True)
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	sql=f"select logs_dir from diy_master where reqid={reqid}"
	logs_dir_obj=execute_df_qry(conn, sql, reqid, conn)
	logs_dir=logs_dir_obj["logs_dir"][0]
	if not os.path.exists(os.path.dirname(logs_dir)):
		raise Exception("Unable to locate logs_dir. Please check")
except  Exception as e:
	print(f"Below exception occurred while connecting to repos db")
	print(str(e))
	sys.exit(1)
try:
	configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
	if configFile.Config.PRDSUFF in env:
		sfEnv = 'cisco.us-east-1'
	elif configFile.Config.STGSUFF in env:
		sfEnv = 'ciscostage.us-east-1'
	elif configFile.Config.DEVSUFF in env:
		sfEnv = 'ciscodev.us-east-1'
	step_name="Opening log and error File"
	log_file=f"{logs_dir}/{jct}_stgload_{batch_id}.log"
	err_file=f"{logs_dir}/{jct}_stgload_{batch_id}.err"
	step_name="Querying JCT to get metadata"
	log_data(log_file,step_name,0)
	sql=f"select run_status,active_ind,job_group_id,job_stream_description desc1,source_db_connection sdc,source_db_name sdn from edw_job_streams where job_stream_id='{jct}'"
	jct_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(jct_obj) == 0:
		raise Exception(f"Unable to query jobstream {jct}")
	run_status=jct_obj["run_status"][0]
	active_ind=jct_obj["active_ind"][0]
	job_group_id=jct_obj["job_group_id"][0]
	base_obj_str=jct_obj["desc1"][0]
	source_db_conn=jct_obj["sdc"][0]
	source_db_name=jct_obj["sdn"][0]
	step_name="Validating JCT status"
	log_data(log_file,step_name,0)
	if run_status not in ('P','R'):
		raise Exception(f"Job stream id {jct} is neither in P nor in R status.. Hence unable to proceed further")
	if active_ind != 'Y':
		raise Exception(f"Job steram id {jct} is not in Active status ({active_ind})..Hence unable to proceed further")
	if len(base_obj_str) == 0:
		raise Exception(f"Unable to find base object from JCT {jct}")
	#src_base_schema=base_obj_str.split(',')[0].split('=')[1]
	#src_base_tbl=base_obj_str.split(',')[1].split('=')[1]
	step_name='Fetching source db type'
	log_data(log_file,f"At step {step_name}",0)
	sql=f"select distinct parameter_category pc from eds_data_catalog.edw_parameter where environment_name='{env}' and parameter_type='{source_db_conn}'"
	db_type_obj=execute_df_qry(conn, sql, reqid, conn)
	db_type=db_type_obj["pc"][0]
	#config_file=f"/apps/edwsfdata/{env}/{job_group_id}/config/{jct}_{batch_id}.param"
	config_file=f"{logs_dir}/{jct}_{batch_id}.param"
	Init_flag=True
	if run_status == "P":
		ins_checkpoint('Insert','Init')
		pull_param(jct)
		ins_jct_run_status('Insert')
		upd_jct_status('R')
		ins_checkpoint('Update','Unload_DB_Data')
		Init_flag=False
	sql=f"select batch_id from diy_job_params where reqid={reqid} and job_stream_id='{jct}' "
	batch_obj=execute_df_qry(conn,sql,reqid,conn)
	if len(batch_obj) == 0:
		raise Exception(f"Unable to query job parameter table for reqid {reqid}")
	batch_id=batch_obj["batch_id"][0]
	sql=f"select current_phase cp from diy_src2stg_checkpoint where reqid={reqid} and job_stream_id='{jct}'"
	cp_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(cp_obj) == 0:
		raise Exception(f"Unable to query diy_src2stg_checkpoint for reqid {reqid} and jct {jct} ")
	curr_status=cp_obj["cp"][0]
	init_curr_status=curr_status
	if curr_status == 'Init':
		pull_param(jct)
		ins_jct_run_status('Insert')
		upd_jct_status('R')
		ins_checkpoint('Update','Unload_DB_Data')
		curr_status ='Unload_DB_Data'
	if curr_status == "Unload_DB_Data":
		if Init_flag:
			pull_param(jct)
		ext_data_2_ff(jct)
		if db_type in ("SNOWFLAKE" ,"ORACLE","TERADATA","HANA"):
			curr_stats = "Complete"
			upd_jct_status('C')
			ins_jct_run_status('Update')
		else:
			curr_status = "Upload_stg_Data"
	sql=f"select SRC2STG_FF_PATH sfp,CURRENT_SRC2STG_FF_NAME csfn from diy_job_params where job_stream_id='{jct}' and reqid={reqid}"
	jct_param_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(jct_param_obj) == 0:
		raise Exception(f"Unable to query jobstream {jct} parameter")
	file_path=jct_param_obj["sfp"][0]
	os.environ['SRC2STG_FF_PATH']=file_path
	file_name=jct_param_obj["csfn"][0]
	os.environ['CURRENT_SRC2STG_FF_NAME']=file_name
	datafile=f"{file_path}/{file_name}"
	if curr_status == "Upload_stg_Data":
		upl_ff_2_stg(jct)
		curr_status = "Archive_Files"
	if curr_status == "Archive_Files":
		arch_files(jct)
		ins_jct_run_status('Update')
		upd_jct_status('C')
		ins_checkpoint('Update','Complete')
	if init_curr_status == 'Complete':
		upd_jct_status('C')
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Error occurred while Loading stage data line number {linenu} at step - {step_name}")
	print(f"Last sql executed was {sql}")
	print (str(e))
	msg_2_update=str(e).replace("'","''")
	sql=f"update diy_master set err_msg='{msg_2_update}' where reqid={reqid}"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)
	sys.exit(7)	