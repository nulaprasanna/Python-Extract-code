#!/apps/python/install/bin/python
#########################################################################################################################
# unload_sf_tbl.py                                                                                                      #
# Script to extract data from Hana table                                                                                #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          May-2020                Initial version                                                 #
# 1.1           Manick          Jun-2020                Use Snowflake connector from metadata handler                   #
# 1.2           Manick          Jul-2020                Pick WH from JCT for usage                                      #
# 1.3           Manick          Aug-2020                Issue with stg_row_cnt download comparision                     #
# 1.4           Manick          Sep-2020                Enable SF query banding                                         #
# 1.5           Manick          Nov-2020                Use nvl function while running count from stage to consider null#
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
import subprocess
try:
	set_env()
	configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
	BASE_DIR=configFile.Config.BASE_DIR
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
	step_name="Connecting to Repos DB"
	conn = open_oracle_connection(envType,None,True)
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	srcdbParams={}
	cursor1=conn.cursor()
	step_name=f"Creating log and err file"
	sql=f"select logs_dir from diy_master where reqid={reqid}"
	logs_dir_obj=execute_df_qry(conn, sql, reqid, conn)
	logs_dir=logs_dir_obj["logs_dir"][0]
	if not os.path.exists(os.path.dirname(logs_dir)):
		raise Exception("Unable to locate logs_dir. Please check")
	log_file=f"{logs_dir}/{reqid}_sfunload_{vBatch}.log"
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
	#step_name="Fetching source db password"
	#log_data(log_file,step_name,0)
	#sql2="SELECT UTL_I18N.RAW_TO_CHAR('"+ srcdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') from dual"
	#cursor1.execute(sql2)
	#results=cursor1.fetchall()
	#for passobj in results:
	#	srcPassword=passobj[0]
	step_name="Prepping table for download"
	log_data(log_file,step_name,0)
	if vExttype == 'ALL_DATA':
		tbl_2_download=f"{dbName}.{vsrcSchema}.{tableName}"
	elif vExttype == 'STREAM':
		tbl_2_download=f"{vInccol}"
	else:
		raise Exception("Invalid exract type. Valid types are ALL_DATA or STREAM")
	tableName=tableName.replace('"','')
	step_name="Establising source db connection"
	log_data(log_file,step_name,0)
	#conn2=snowflake.connector.connect(user=srcdbParams['SOURCE_LOGIN'],password=srcPassword,account=srcdbParams['ACCOUNT'],warehouse=srcdbParams['WAREHOUSE'])
	sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","UNLOAD_SF_TBL")
	conn2=open_sf_connection(srcdbParams['ACCOUNT'],srcdbParams['SOURCE_LOGIN'],query_tag=sf_query_tag)
	cursor2=conn2.cursor()
	sql=f"select sf_warehouse from edw_job_streams where job_stream_id='{jct}'"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	for obj in results:
		sf_wh=obj[0]
	cursor2.execute(f"use warehouse {sf_wh}")
	init_cnt_sql=f"select count(1) cnt from {tbl_2_download}"
	vsrcQuery=vsrcQuery[:-1] ## strip off last comma
	if vExttype == 'STREAM':
		source_query=vsrcQuery + f' ,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID FROM {tbl_2_download}'
	else:
		source_query=vsrcQuery + f' FROM {tbl_2_download}'
	if vWhere is not None and len(vWhere.replace(' ','')) > 0:
		source_query += f' where {vWhere}'
		init_cnt_sql += f' where {vWhere}'
	init_cnt_obj=execute_df_qry(conn2, init_cnt_sql, reqid, conn)
	init_cnt=init_cnt_obj["cnt"][0]
	log_data(log_file,source_query,0)
	step_name="Creating stage for download"
	log_data(log_file,step_name,0)
	sql=f"create or replace stage DEMO_DB.PUBLIC.{tableName}_{vBatch}_{currtime} file_format = EDW_UTIL_DB.FILE_FORMATS.CSV_PIPE_WITHOUT_HEADER_FOR_DOWNLOAD"
	execute_qry(conn2,sql,reqid,conn)
	step_name="Copying data to stage"
	log_data(log_file,step_name,0)
	cursor2.execute("begin transaction")
	sql=f"copy into @DEMO_DB.PUBLIC.{tableName}_{vBatch}_{currtime}/data_{tableName}_{vBatch}_ from ({source_query})  max_file_size=50000000"
	down_obj=execute_df_qry(conn2, sql, reqid, conn)
	rows_unloaded=down_obj["rows_unloaded"][0]
	input_bytes=down_obj["input_bytes"][0]
	output_bytes=down_obj["output_bytes"][0]
	if rows_unloaded != init_cnt :
		cursor2.execute("rollback")
		raise Exception(f"Expected to donwload {init_cnt} rows. However only {rows_unloaded} got downloaded")
	log_data(log_file,f"Succesfully downloaded {rows_unloaded} rows to internal stage",0)
	step_name="Validating data copied to stage"
	log_data(log_file,step_name,0)
	sql=f"list @DEMO_DB.PUBLIC.{tableName}_{vBatch}_{currtime}"
	list_obj=execute_df_qry(conn2, sql, reqid, conn)
	num_files=len(list_obj)
	log_data(log_file,f"Table {tableName} produced {num_files} files",0)
	sql=f"select count(nvl($1,'')) cnt from @DEMO_DB.PUBLIC.{tableName}_{vBatch}_{currtime}"
	stg_row_cnt_obj=execute_df_qry(conn2, sql, reqid, conn)
	stg_row_cnt=stg_row_cnt_obj["cnt"][0]
	if stg_row_cnt < init_cnt :
		cursor2.execute("rollback")
		raise Exception(f"Expected to donwload {init_cnt} rows. However only {stg_row_cnt} got downloaded")
	step_name="Downloading data to flatfile"
	log_data(log_file,step_name,0)
	sql=f"get @DEMO_DB.PUBLIC.{tableName}_{vBatch}_{currtime} file://{vPath} parallel=24"
	get_obj=execute_df_qry(conn2, sql, reqid, conn)
	files_downloaded=len(get_obj)
	step_name="Validating downloaded data"
	log_data(log_file,step_name,0)
	osPrcmd=f"ls -lrt {vPath}/data_{tableName}_{vBatch}_* | wc -l"
	file_cnt=int(os.popen(osPrcmd).read())
	if (num_files > files_downloaded):
		raise Exception(f"File download mis match. Expected {num_files}. Actual {files_downloaded}")
	if (num_files > file_cnt):
		raise Exception(f"File count mis match. Expected {num_files}. Actual {file_cnt}")
	sql=f"update diy_master set tgt_rows={stg_row_cnt} where reqid={reqid}"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)
	sql=f"update edw_job_streams set src2stg_ff_path='{vPath}',current_src2stg_ff_name='data_{tableName}_{vBatch}',batch_id='{vBatch}' where job_stream_id='{jct}'"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)
	conn.close()
	log_data(log_file,f"Now committing to flush stream",0)
	cursor2.execute("commit")
	cursor2.close()
	conn2.close()
	log_data(log_file,f"Succesfully extracted data for {tbl_2_download}",1)
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Exception - {e} occurred at line {linenu} step {step_name} while downloading data")
	sys.exit(7)
