#!/apps/python/install/bin/python
#########################################################################################################################
# unload_td_tbl.py                                                                                                      #
# Script to extract data from TD to falt file                                                                           #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Mar-2020                Initial Version                                                 #
# 1.1           Manick          Apr-2020                to_date to to_timestamp for incremental                         #
# 1.2           Manick          May-2020                same column title without column alias is failing duing tbuild  #
# 1.3           Manick          May-2020                Fix for filtered data incrementals                              #
# 1.4           Manick          May-2020                Set REQID for unload TD table array to work and query banding   #
# 1.5           Manick          Apr-2020                Return output to file                                           #
# 1.6           Manick          Aug-2020                Remove hardcoded TD db name from template                       #
# 1.7           Manick          Sep-2020                Enable LOCK ROW FOR ACCESS clause to avoid blocks               #
# 1.8           Manick          Sep-2020                Create 500MB file and upload while download                     #
# 1.9           Manick          Nov-2020                Replace 4 quotes with 2 quotes due to change in DIY_src2stg     #
# 1.10          Manick          Nov-2020                Check error file size for any interim load errors               #
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
from pathlib import Path
try:
	set_env()
	step_name="Setting parameter"
	envType=os.environ['RUN_ENVIRONMENT']
	v_TBL_SIZE=int(os.environ['TBLSIZE'])
	connectionName=os.environ['SOURCE_DB_CONNECTION']
	dbName=os.environ['SOURCE_DB_NAME']
	tableName = os.environ['SOURCE_TABLE_NAME'].replace('"','') ## metadata query fails with schema name inside double quotes
	target_tableName = os.environ['TARGET_TABLE_NAME']
	vPath=os.environ['SRC2STG_FF_PATH']
	vFile=os.environ['CURRENT_SRC2STG_FF_NAME']
	vDelim=os.environ['SRC2STG_FF_DELIMITER']
	vsrcQuery=os.environ['SOURCE_QUERY']
	vsrcSchema=os.environ['SOURCE_SCHEMA'].replace('"','') ## metadata query fails with schema name inside double quotes
	vWhere=os.environ['WHERE_CLAUSE']
	vBatch=int(os.environ['BATCH_ID'])
	vInccol=os.environ['INCREMENTAL_COLUMN_NAME']
	vExttype=os.environ['EXTRACT_TYPE']
	vFromdtm=os.environ['FROM_EXTRACT_DTM']
	vTodtm=os.environ['TO_EXTRACT_DTM']
	vFromid=int(os.environ['FROM_EXTRACT_ID'])
	vToid=int(os.environ['TO_EXTRACT_ID'])
	job_group_id=os.environ['JOB_GROUP_ID']
	jct=os.environ['JOB_STREAM_ID']
	logs_dir=os.environ['LOGS_DIR']
	reqid=os.environ['REQID']
	currtime=datetime.now().strftime('%Y%m%d%H%M%S')
	currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
	if currOsUser == "":
		try:
			currOsUser=os.getlogin()
		except Exception as e:
			currOsUser=c_getuser()
			pass
	step_name="determing template to use"
	if v_TBL_SIZE < 1048576:
		TEMPLATE="Tbuild_template_Micro.txt"
	elif v_TBL_SIZE < 104857600:
		TEMPLATE="Tbuild_template_Mini.txt"
	else:
		TEMPLATE="Tbuild_template_Medium.txt"
	step_name="Replacing single quote on where clause"
	SCR_DIR="/apps/edwsfdata/python/scripts"
	REV_WHERE_CLAUSE=vWhere.replace("'","''").replace("''''","''") ## replace single quote with two single quotes
	step_name="Establishing repos connection"
	conn = open_oracle_connection(envType,None,True)
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	cursor1=conn.cursor()
	step_name="Executing query to pull vachar columns"
	## Check for any VARCHAR columns to apply rtrim
	sql=f"select column_name from EDS_DATA_CATALOG.EDW_TABLE_COLUMN"
	sql+=f" where table_name='{tableName}' and db_instance_name='{dbName}' and db_schema_name=replace('{vsrcSchema}','VWDB','DB')"
	sql+=f" and environment_name='{envType}' and data_type in ('VARCHAR','CHAR') and active_flag='A'"
	cursor1.execute(sql)
	varcharres=cursor1.fetchall()
	step_name="Applying transformation for varchar columns"
	if len(varcharres) > 0:
		for varcharobj in varcharres:
			colName=varcharobj[0]
			vsrcQuery=vsrcQuery.replace(f'"{colName}"',f'rtrim("{colName}") as "{colName}"')
	step_name="Executing query to pull CLOB columns"
	## Check for any CLOB columns to restrict first 64K records due to TPT limitation
	sql=f"select column_name from EDS_DATA_CATALOG.EDW_TABLE_COLUMN"
	sql+=f" where table_name='{tableName}' and db_instance_name='{dbName}' and db_schema_name=replace('{vsrcSchema}','VWDB','DB')"
	sql+=f" and environment_name='{envType}' and data_type in ('CLOB') and data_length>64000 and active_flag='A'"
	cursor1.execute(sql)
	clobres=cursor1.fetchall()
	step_name="Applying transformation for CLOB columns"
	if len(clobres) > 0:
		for clobresobj in clobres:
			colName=clobresobj[0]
			vsrcQuery=vsrcQuery.replace(f'"{colName}"',f'cast(substr("{colName}",1,64000) as varchar(64000)) as "{colName}"')
	step_name="Fetching filter array if exists"
	sql=f"select nvl(attribute7,'NULL') arr from diy_master where reqid={reqid}"
	cursor1.execute(sql)
	arrres=cursor1.fetchall()
	if len(arrres) > 0:
		for arrobj in arrres:
			dt_array=arrobj[0]
	else:
		raise Exception (f"Unable to query DIY_MASTER with reqid {reqid}")
	if dt_array != 'NULL':
		dt_flag=True
		dt_array=dt_array.replace("'","''")  ## replace single quote with two single quotes
	else:
		dt_flag=False
	step_name="Appending batchid to query"
	vsrcQuery+=f"{vBatch} as EDWSF_BATCH_ID "
	step_name="Appending where clause"
	vsrcQuery=vsrcQuery.replace("'","''") ## replace single quote with two single quotes
	if vWhere == " WHERE ":
		## this will be true if it is not split and load and inc col needs to be added
		if vExttype == "DATE":
			if dt_flag:
				REV_WHERE_CLAUSE=f"{vWhere} {vInccol} in {dt_array}"
			else:
				REV_WHERE_CLAUSE=f"{vWhere} {vInccol} > to_timestamp(''{vTodtm}'',''YYYY/MM/DD HH24:MI:SS'')"
		elif vExttype == "ID":
			REV_WHERE_CLAUSE=f"{vWhere} {vInccol} > {vToid}"
		vsrcQuery=f'{vsrcQuery} FROM "{vsrcSchema}"."{tableName}" {REV_WHERE_CLAUSE}'
	elif len(vWhere) > 7:
		## this is split and load or filtered data
		if vExttype == "DATE":
			if vInccol not in vWhere: 
				## if single thread incremental but filtered data
				if dt_flag:
					REV_WHERE_CLAUSE=f"{REV_WHERE_CLAUSE} AND {vInccol} in {dt_array}"
				else:
					REV_WHERE_CLAUSE=f"{REV_WHERE_CLAUSE} AND {vInccol} > to_timestamp(''{vTodtm}'',''YYYY/MM/DD HH24:MI:SS'')"
		elif vExttype == "ID":
			if vInccol not in vWhere: 
				REV_WHERE_CLAUSE=f"{REV_WHERE_CLAUSE} AND {vInccol} > {vToid}"
		vsrcQuery=f'{vsrcQuery} FROM "{vsrcSchema}"."{tableName}" {REV_WHERE_CLAUSE}'
	else:
		vsrcQuery=f'{vsrcQuery} FROM "{vsrcSchema}"."{tableName}"'
	step_name="Extracting source username and password"
	sql=f"select source_db_connection sdc from edw_job_streams where job_stream_id='{jct}'"
	sdc_obj=execute_df_qry(conn,sql,reqid,conn)
	sdc=sdc_obj["sdc"][0]
	sql=f"select to_char(parameter_value) un from eds_data_catalog.edw_parameter where upper(environment_name)='{envType}'"
	sql+=f" and parameter_type ='{sdc}' and parameter_name='SOURCE_LOGIN'"
	un_obj=execute_df_qry(conn,sql,reqid,conn)
	source_user=un_obj["un"][0]
	sql=f"select UTL_I18N.RAW_TO_CHAR(to_char(parameter_value),'AL32UTF8') pass from eds_data_catalog.edw_parameter where upper(environment_name)='{envType}'"
	sql+=f" and parameter_type ='{sdc}' and parameter_name='SOURCE_LOGIN_PASSWORD'"
	pass_obj=execute_df_qry(conn,sql,reqid,conn)
	source_pass=pass_obj["pass"][0]
	step_name="creating job param file"
	template_file=open(f"{SCR_DIR}/{TEMPLATE}",'r')
	par_file_name=f"{logs_dir}/jobvars_{jct}_{vBatch}.param"
	param_file=open(par_file_name,'w')
	for line in template_file:
		line=line.replace('TBLNAME',tableName)
		line=line.replace('SOURCEDB',dbName.lower())
		line=line.replace('TUSER',source_user)
		line=line.replace('TPASSWORD',source_pass)
		line=line.replace('TFNAME',f"{vPath}/{vFile}")
		line=line.replace('TDELIMITER',f"{vDelim}")
		line=line.replace('QBAND',f"Module=Unload;LoadType={vExttype};user={currOsUser};")
		param_file.write(line)
	template_file.close()
	if "LOCK ROW FOR ACCESS " not in vsrcQuery:
		vsrcQuery = f"LOCK ROW FOR ACCESS {vsrcQuery}"
	param_file.write(f",SelectStmt     = '{vsrcQuery}'")
	param_file.write(f"\n\n,DCCFileSizeMax='500M'")
	param_file.close()
	step_name="Invoking tbuild to extract data"
	file_cnt=1
	outf=open(f'{logs_dir}/tbuild_{target_tableName}_{vBatch}.log','a')
	errf=open(f'{logs_dir}/tbuild_{target_tableName}_{vBatch}.err','a')
	sfoutf=open(f"{logs_dir}/td_split_load_{currtime}.log",'w')
	sferrf=open(f"{logs_dir}/td_split_load_{currtime}.err",'w')
	touch_file=f"{logs_dir}/{jct}_{currtime}"
	Path(touch_file).touch()
	cmnd=subprocess.Popen(f"tbuild -f {SCR_DIR}/jobdef.txt -v {par_file_name} -j {target_tableName}_{vBatch}" ,stdout=outf, stderr=errf, shell=True)
	oscmd="/apps/edwsfdata/python/scripts/DIY_orcl_interim_load.py"
	time.sleep(5) ## sleep 5 seconds before invoking load script
	proc=subprocess.Popen([oscmd,'-e',envType,'-r',str(reqid),'-j',jct,'-t',currtime],stdout=sfoutf, stderr=sferrf)
	pid=proc.pid
	print(f"Snowflake load triggered with pid {pid}")
	(output, error) = cmnd.communicate()
	ret_code=cmnd.returncode
	if ret_code != 0:
		raise Exception(f"Exception encountered while executing TPT. Please refer {logs_dir}/tbuild_{target_tableName}_{vBatch}.err for details\n")
	os.remove(touch_file)
	proc.communicate()
	retcode=proc.returncode
	int_err_file=f"{logs_dir}/td_split_load_{currtime}.err"
	fsize=os.stat(int_err_file).st_size
	if retcode != 0 or fsize > 0:
		raise Exception(f"Oracle interim load failed with return code {retcode}")
	cursor1.close()
	conn.close()
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Error occurred at line number {linenu} while invoking TPT  at step {step_name} - {vsrcQuery}")
	print (str(e))
	sys.exit(7)
