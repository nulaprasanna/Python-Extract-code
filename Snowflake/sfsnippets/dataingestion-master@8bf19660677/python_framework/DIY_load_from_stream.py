#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_load_from_stream                                                                                                  #
# Script to perform load data out of Snowflake to TD                                                                    #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Jun-2020                Initial Version                                                 #
# 1.1           Manick          Jun-2020                Use Snowflake connector from metadata handler                   #
# 1.2           Manick          Jul-2020                Convert count to bigint                                         #
# 1.3           Manick          Jul-2020                Use -h 100m for processing large volume of data                 #
# 1.4           Manick          Aug-2020                Remove hard coded TD System to use both in prod and dev/test    #
# 1.5           Manick          Sep-2020                Enable query tag                                                #
# 1.6           Manick          Jun-2021                New version with additional comments for easy readability       #
#########################################################################################################################
import argparse,pdb
import snowflake.connector
import cx_Oracle
import teradatasql
import os, re, sys
from datetime import datetime
from datetime import timedelta
import time
from ConfigObject import ConfigObject
from metadata_handler import *

## This method is used to convert Oracle's CLOB type to String type

def LobtoStr(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)

## This method

def arch_files():
	global log_file,step_name,envType,env,configFile,job_group_id,file_list_file,logs_dir
	step_name="Now compressing and archiving data file"
	log_data(log_file,step_name,0)
	if envType.lower() == 'prd':
		datafile_dest=configFile.Config.datafile_dest_prd
	elif 'DV' in envType.upper():
		datafile_dest=configFile.Config.datafile_dest_dev
	elif 'TS' in envType.upper():
		datafile_dest=configFile.Config.datafile_dest_stg
	else:
		raise Exception(f"Invalid data file archive destination")
	processes = set()
	datafile_dest=datafile_dest.replace('jobgroup',job_group_id)
	datafile_dest=datafile_dest.replace('env',env.upper())
	curr_pid=os.getpid()
	fh=open(file_list_file,'r')
	for line in fh:
		file_name=line.strip('\n')
		compress_log=open(f"{logs_dir}/compress.log",'a')
		oscmd=f"mv {file_name} {datafile_dest}"
		proc=subprocess.Popen([oscmd],stdout=compress_log, stderr=compress_log, shell=True)
		pid=proc.pid
		pout=processes.add(proc)
		step_name=f"Compress file submitted with pid {pid}"
		log_data(log_file,step_name,0)
		#while (int(int(os.popen(oscmd).read())-1)) >= 10:
		#	time.sleep(15)  ## sleep 15 seconds if more than 10 jobs running.

def upd_jct_status(run_status):
	global conn,jct,reqid,step_name,log_file,step_name
	step_name=f"Updating edw_job_stream status to {run_status}"
	log_data(log_file,step_name,0)
	sql=f"update edw_job_streams set run_status='{run_status}' where job_stream_id='{jct}'"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)

def ins_jct_run_status(phase):
	global conn,jct,reqid,step_name,log_file
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
def perform_td_load():
	global conn,reqid,step_name,log_file,err_file,merge_type,logs_dir,tgtdbParams,tgtPassword,conn3,target_schema,target_table,source_schema,source_table,target_db,jct,envType,sql,source_schema_name
	global src2stg_ff_path,current_src2stg_ff_name,batch_id,file_list_file,stg_jct,source_db_name,source_table_name,target_db_name,target_schema_name,target_table_name,source_db_conn
	SCR_DIR="/apps/edwsfdata/python/scripts"
	E_FILE=current_src2stg_ff_name
	E_PATH=src2stg_ff_path
	currTime=datetime.now().strftime('%Y%m%d%H%M%S')
	tgtuser=tgtdbParams['SOURCE_LOGIN']
	osPrcmd=f"ls -lrt {E_PATH}/{E_FILE}_* | wc -l"
	file_cnt=int(os.popen(osPrcmd).read())
	if file_cnt <=0:
		raise Exception("Issue while checking files from staging area - {E_PATH}/{E_FILE}_*")
	TEMPLATE=f"{SCR_DIR}/Tbuild_template_upload.txt"
	jobparam=f"{SCR_DIR}/jobdef_upload.txt"
	if file_cnt < 20:
		tsize="Micro"
	elif file_cnt < 200:
		tsize="Mini"
	else:
		tsize="Medium"
	currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
	if currOsUser == "":
		try:
			currOsUser=os.getlogin()
		except Exception as e:
			currOsUser=c_getuser()
			pass
	qband=f"Module=Upload;user={currOsUser}"
	oscmd=f"ls -lrt {E_PATH}/{E_FILE}_* | awk '" + "{print $9}'"
	file_list=os.popen(oscmd).read()
	file_list_file=f"{logs_dir}/file_list.txt"
	file_list_hand=open(file_list_file,'w')
	file_list_hand.write(file_list)
	file_list_hand.close()

	local_template=f"{logs_dir}/jobvars_{jct}_{currTime}.param"
	local_template_file=open(local_template,'w')
	template_file=open(TEMPLATE,'r')
	for line in template_file:
		line=line.replace('TUSER',tgtuser)
		line=line.replace('TGTSYSTEM',target_db.lower())
		line=line.replace('TPASSWORD',tgtPassword)
		if merge_type == 'OVERWRITE':
			line=line.replace('TGTDB',target_schema)
			line=line.replace('TGTTABLE',target_table)
		else:
			line=line.replace('TGTDB',target_schema_name)
			line=line.replace('TGTTABLE',target_table_name)
		line=line.replace('SIZE',tsize)
		line=line.replace('QBAND',qband)
		line=line.replace('FILELIST',file_list_file)
		local_template_file.write(line)
	local_template_file.close()
	template_file.close()
	if merge_type == 'OVERWRITE':
		step_name=f"Entering into overwrite section"
		log_data(log_file,step_name,0)
		step_name=f"Validate existence of target table"
		log_data(log_file,step_name,0)
		sql=f"select count(1) cnt from dbc.tablesv where tablename='{target_table}' and databasename='{target_schema}'"
		tgtobj=execute_df_qry(conn3, sql, reqid, conn)
		tgtobj_cnt=tgtobj["cnt"][0]
		if tgtobj_cnt != 1:
			raise Exception(f"Target table {target_schema}.{target_table} does not exist. Please check and retry.")
		sql=f"select cast(count(1) as bigint) cnt from {target_schema}.{target_table}"
		init_cnt_obj=execute_df_qry(conn3,sql,reqid,conn)
		init_cnt=init_cnt_obj["cnt"][0]
		step_name=f"Deleting data from target table {target_schema}.{target_table} since it is full load"
		log_data(log_file,step_name,0)
		sql=f"delete from {target_schema}.{target_table}"
		execute_qry(conn3,sql,reqid,conn)
		step_name=f"Triggering TPT to load data into target table"
		log_data(log_file,step_name,0)
		outf=open(f'{logs_dir}/tbuild.log','a')
		errf=open(f'{logs_dir}/tbuild.err','a')
		cmnd=subprocess.Popen(f"tbuild -f {jobparam} -v {local_template} -j {target_table}_{currTime} -h 100m" ,stdout=outf, stderr=errf, shell=True)
		(output, error) = cmnd.communicate()
		ret_code=cmnd.returncode
		if ret_code != 0:
			raise Exception(f"Exception encountered while executing TPT.")
		outf.close()
		errf.close()
		outf=open(f'{logs_dir}/tbuild.log','r')
		for line in outf:
			if 'Rows Inserted' in line:
				ins_row_cnt=line.split(':')[2].replace(" ","").strip('\n')
		outf.close()
		log_data(log_file,f"Succesfully inserted {ins_row_cnt} rows into {target_schema}.{target_table}",1)
		sql=f"select cast(count(1) as bigint) cnt from {target_schema}.{target_table}"
		fin_cnt_obj=execute_df_qry(conn3,sql,reqid,conn)
		final_cnt=fin_cnt_obj["cnt"][0]
		sql=f"update diy_master set src_rows={init_cnt},tgt_rows={final_cnt} where reqid={reqid}"
		execute_qry(conn,sql,reqid,conn)
		execute_qry(conn,"commit",reqid,conn)
	else:
		step_name=f"Entering into merge section"
		log_data(log_file,step_name,0)
		step_name=f"Validate existence of stage table"
		log_data(log_file,step_name,0)
		sql=f"select count(1) cnt from dbc.tablesv where tablename='{target_table_name}' and databasename='{target_schema_name}'"
		tgtobj=execute_df_qry(conn3, sql, reqid, conn)
		tgtobj_cnt=tgtobj["cnt"][0]
		if tgtobj_cnt != 1:
			raise Exception(f"Target Stage table {target_schema_name}.{target_table_name} does not exist. Please check and retry.")
		step_name=f"Validate existence of target table"
		log_data(log_file,step_name,0)
		sql=f"select cast(count(1) as bigint) cnt from dbc.tablesv where tablename='{target_table}' and databasename='{target_schema}'"
		tgtobj=execute_df_qry(conn3, sql, reqid, conn)
		tgtobj_cnt=tgtobj["cnt"][0]
		if tgtobj_cnt != 1:
			raise Exception(f"Target table {target_schema}.{target_table} does not exist. Please check and retry.")
		step_name=f"Deleting data from stage table {target_schema_name}.{target_table_name}"
		log_data(log_file,step_name,0)
		sql=f"delete from {target_schema_name}.{target_table_name}"
		execute_qry(conn3,sql,reqid,conn)
		step_name=f"Triggering TPT to load data into stage table"
		log_data(log_file,step_name,0)
		outf=open(f'{logs_dir}/tbuild.log','a')
		errf=open(f'{logs_dir}/tbuild.err','a')
		cmnd=subprocess.Popen(f"tbuild -f {jobparam} -v {local_template} -j {target_schema_name}_{currTime} -h 100m" ,stdout=outf, stderr=errf, shell=True)
		(output, error) = cmnd.communicate()
		ret_code=cmnd.returncode
		if ret_code != 0:
			raise Exception(f"Exception encountered while executing TPT.")
		outf.close()
		errf.close()
		outf=open(f'{logs_dir}/tbuild.log','r')
		for line in outf:
			if 'Rows Inserted' in line:
				ins_row_cnt=line.split(':')[2].replace(" ","").strip('\n')
		outf.close()
		log_data(log_file,f"Succesfully inserted {ins_row_cnt} rows into {target_schema_name}.{target_table_name}",1)
		sql=f"select cast(count(1) as bigint) cnt from {target_schema_name}.{target_table_name}"
		fin_cnt_obj=execute_df_qry(conn3,sql,reqid,conn)
		final_cnt=fin_cnt_obj["cnt"][0]
		log_data(log_file,f"{target_schema_name}.{target_table_name} has {final_cnt} rows",0)
		sql=f"select cast(count(1) as bigint) cnt from {target_schema}.{target_table}"
		begin_cnt_obj=execute_df_qry(conn3,sql,reqid,conn)
		begin_cnt=begin_cnt_obj["cnt"][0]
		step_name="Pulling unique key for merging"
		log_data(log_file,step_name,0)
		sql=f"select b.column_name from EDS_DATA_CATALOG.EDW_UNIQUE_KEY a,EDS_DATA_CATALOG.EDW_UNIQUE_KEY_COLUMN b where a.table_name='{target_table}' and a.environment_name='{env}'"
		sql=sql + f" and a.db_instance_name='{target_db}' and a.db_schema_name='{target_schema}' and a.active_flag='A' and a.unique_key_name=b.unique_key_name "
		sql=sql + " and a.environment_name=b.environment_name and a.db_instance_name=b.db_instance_name and a.table_name=b.table_name and a.db_schema_name=b.db_schema_name"
		uk_obj=execute_df_qry(conn, sql, reqid, conn)
		if type(uk_obj) is int or len(uk_obj) == 0:
			raise Exception(f"Unable to query unique key for {target_db}.{target_schema}.{target_table}")
		uk_col_list=''
		for index,row in uk_obj.iterrows():
			uk_col_list+=f"{row[0]},"
		if uk_col_list == '':
			raise Exception("Issue encountered while pulling unique key. Please check and retry")
		uk_col_list=uk_col_list[:-1] ## remove last comma
		sql=f"select cast(count(1) as bigint) cnt,metadata$action ma from {target_schema_name}.{target_table_name} group by 2 order by 2"
		act_obj=execute_df_qry(conn3, sql, reqid, conn)
		for index,row in act_obj.iterrows():
			action=row["ma"]
			cnt=row["cnt"]
			log_data(log_file,f"{action} - {cnt}",0)
		step_name="Now counting actions"
		log_data(log_file,step_name,0)
		upd_cnt=0
		ins_cnt=0
		del_cnt=0
		sql=f"select cast(count(1) as bigint) cnt from {target_schema_name}.{target_table_name} where metadata$action ='DELETE' and metadata$isupdate='TRUE'"
		upd_cnt_obj=execute_df_qry(conn3, sql, reqid, conn)
		upd_cnt=upd_cnt_obj["cnt"][0]
		sql=f"select cast(count(1) as bigint) cnt from {target_schema_name}.{target_table_name}  where metadata$action ='DELETE' and metadata$isupdate='FALSE'"
		del_cnt_obj=execute_df_qry(conn3, sql, reqid, conn)
		del_cnt=del_cnt_obj["cnt"][0]
		sql=f"select cast(count(1) as bigint) cnt from {target_schema_name}.{target_table_name}  where metadata$action ='INSERT' and metadata$isupdate='FALSE'"
		ins_cnt_obj=execute_df_qry(conn3, sql, reqid, conn)
		ins_cnt=ins_cnt_obj["cnt"][0]
		log_data(log_file,f"Records inserted - {ins_cnt} \nRecords Updated - {upd_cnt} \nRecords Deleted - {del_cnt} \n",0)
		step_name="Now framing insert query"
		log_data(log_file,step_name,0)
		cursor1=conn.cursor()
		cursor1.outputtypehandler=LobtoStr
		sql=f"select EDS_DATA_CATALOG.FN_GET_COLUMN_LIST_NEW('{envType}','{source_db_name}','{source_schema_name}','{source_table_name}') from dual"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		for obj in results:
			sel_query=obj[0]
		ins_query=f"Insert into {target_schema}.{target_table} select"
		ins_query += f" {sel_query}"
		ins_query=ins_query[:-1] ## strip off last comma
		ins_query += f" from {target_schema_name}.{target_table_name} where metadata$action ='INSERT'"
		log_data(log_file,ins_query,0)
		step_name="Now performing deletion"
		log_data(log_file,step_name,0)
		sql=f"delete from {target_schema}.{target_table} where ({uk_col_list}) in (select {uk_col_list} from {target_schema_name}.{target_table_name} "
		sql += f" where metadata$action ='DELETE')"
		execute_qry(conn3,sql,reqid,conn)
		step_name="Now performing Insertion"
		execute_qry(conn3,ins_query,reqid,conn)
		sql=f"select cast(count(1) as bigint) cnt from {target_schema}.{target_table}"
		end_cnt_obj=execute_df_qry(conn3,sql,reqid,conn)
		end_cnt=end_cnt_obj["cnt"][0]
		rows_merged=end_cnt-begin_cnt
		log_data(log_file,f"Succesfully merged {rows_merged} rows",1)
		sql=f"update diy_master set src_rows={begin_cnt},tgt_rows={end_cnt} where reqid={reqid}"
		execute_qry(conn,sql,reqid,conn)
		execute_qry(conn,"commit",reqid,conn)
	log_data(log_file,"Establishing SF connection to drop stage",0)
	srcdbParams={}
	sql=f"select parameter_name pn,to_char(parameter_value) pv from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='{envType}'"
	sql+=f" and parameter_type ='{source_db_conn}'"
	results=execute_df_qry(conn, sql, reqid, conn)
	for index,obj in results.iterrows():
		param=obj["pn"]
		value=obj["pv"]
		srcdbParams[param]=value
	#print(srcdbParams)
	#step_name="Fetching source db password"
	#log_data(log_file,step_name,0)
	#sql="SELECT UTL_I18N.RAW_TO_CHAR('"+ srcdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') pass from dual"
	#src_pass_obj=execute_df_qry(conn, sql, reqid, conn)
	#srcPassword=src_pass_obj["pass"][0]
	#conn2=snowflake.connector.connect(user=srcdbParams['SOURCE_LOGIN'],password=srcPassword,account=srcdbParams['ACCOUNT'],warehouse=srcdbParams['WAREHOUSE'])
	sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","LOAD_FROM_STREAM")
	conn2=open_sf_connection(srcdbParams['ACCOUNT'],srcdbParams['SOURCE_LOGIN'],query_tag=sf_query_tag)
	sql=f"select current_role() rol"
	curr_rol_obj=execute_df_qry(conn2, sql, reqid, conn)
	current_role=curr_rol_obj["rol"][0]
	sql=f"show stages like '{source_table_name}_{batch_id}%' in schema DEMO_DB.PUBLIC"
	stg_qry_obj=execute_df_qry(conn2, sql, reqid, conn)
	for index,row in stg_qry_obj.iterrows():
		stg_owner=row["owner"]
		stg_name=row["name"]
		if stg_owner == current_role:
			log_data(log_file,f"Now dropping stage {stg_name}",0)
			sql=f"drop stage demo_db.public.{stg_name}"
			execute_qry(conn2, sql, reqid, conn)
	conn2.close()
## Main program starts here
pgmVersion='1.0'
parser = argparse.ArgumentParser(description='Script used to load data from stereams to TD',
    epilog='Example: python DIY_load_from_stream.py -j jct -e environment -r reqid')
parser.add_argument('-j', '--jct',required=True,
    help='STG2BR JCT which needs to be executed ')
parser.add_argument('-e','--env',required=True,
        help='Environment for which data needs to be loaded from streams (DV3/TS3/PRD)')
parser.add_argument('-r','--reqid',
        help='Reqid from DIY tables')
args=parser.parse_args()
jct=args.jct
env=args.env
reqid=args.reqid
envType=env
tgtdbParams={}
try:
	set_env()
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
	step_name="Opening log and error File"
	log_file=f"{logs_dir}/{jct}_load_from_stream.log"
	err_file=f"{logs_dir}/{jct}_load_from_stream.err"
	step_name="Querying JCT to get metadata"
	log_data(log_file,step_name,0)
	sql=f"select workflow_type,target_db_connection,source_db_name,source_schema,source_table_name,target_table_name"
	sql+=f",target_db_name,target_schema,merge_type,job_stream_id_link,run_status,active_ind,job_group_id"
	sql+=f" from edw_job_streams where job_stream_id='{jct}'"
	jct_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(jct_obj) == 0:
		raise Exception(f"Unable to query jobstream {jct}")
	workflow_type=jct_obj["workflow_type"][0]
	target_db_conn=jct_obj["target_db_connection"][0]
	source_db=jct_obj["source_db_name"][0]
	source_schema=jct_obj["source_schema"][0]
	source_table=jct_obj["source_table_name"][0]
	stgtbl=f"{source_db}.{source_schema}.{source_table}"
	target_table=jct_obj["target_table_name"][0]
	target_db=jct_obj["target_db_name"][0]
	target_schema=jct_obj["target_schema"][0]
	merge_type=jct_obj["merge_type"][0]
	stg_jct=jct_obj["job_stream_id_link"][0]
	run_status=jct_obj["run_status"][0]
	active_ind=jct_obj["active_ind"][0]
	job_group_id=jct_obj["job_group_id"][0]
	step_name="Validating JCT status"
	log_data(log_file,step_name,0)
	if run_status not in ('P','R'):
		raise Exception(f"Job stream id {jct} is neither in P nor in R status.. Hence unable to proceed further")
	if active_ind != 'Y':
		raise Exception(f"Job steram id {jct} is not in Active status ({active_ind})..Hence unable to proceed further")
	step_name="validating mergetype"
	log_data(log_file,step_name,0)
	if merge_type not in ('OVERWRITE','MERGE_ONE2ONE','APPEND'):
		raise Exception(f"Invalid merge type {merge_type} for jct {jct}..Hence unable to proceed further")
	step_name="Queryig base table and base schema"
	log_data(log_file,step_name,0)
	base_obj_str=""
	sql=f"select job_stream_description desc1,source_db_connection,source_table_name,source_schema,source_db_name,src2stg_ff_path,current_src2stg_ff_name,batch_id, "
	sql+=f" target_schema,target_db_name,target_table_name from edw_job_streams where job_stream_id='{stg_jct}'"
	base_obj=execute_df_qry(conn, sql, reqid, conn)
	base_obj_str=base_obj["desc1"][0]
	source_schema_name=base_obj["source_schema"][0]
	source_db_conn=base_obj["source_db_connection"][0]
	source_table_name=base_obj["source_table_name"][0]
	source_db_name=base_obj["source_db_name"][0]
	src2stg_ff_path=base_obj["src2stg_ff_path"][0]
	current_src2stg_ff_name=base_obj["current_src2stg_ff_name"][0]
	batch_id=base_obj["batch_id"][0]
	target_table_name=base_obj["target_table_name"][0]
	target_schema_name=base_obj["target_schema"][0]
	target_db_name=base_obj["target_db_name"][0]
	step_name="Establishing target connection"
	targetdbType=""
	sql=f"select distinct parameter_category from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='{envType}' and parameter_type ='{target_db_conn}'"
	results=execute_df_qry(conn, sql, reqid, conn)
	targetdbType=results["parameter_category"][0]
	sql=f"select parameter_name,to_char(parameter_value) from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='{envType}'"
	sql+=f" and upper(parameter_category)='{targetdbType}' and parameter_type ='{target_db_conn}'"
	results=execute_df_qry(conn, sql, reqid, conn)
	if len(results) == 0:
		raise Exception("Could not query target db connection details")
	for index,row in results.iterrows():
		param=row[0]
		value=row[1]
		tgtdbParams[param]=value
	sql="SELECT UTL_I18N.RAW_TO_CHAR('"+ tgtdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') pass from dual"
	results=execute_df_qry(conn, sql, reqid, conn)
	tgtPassword=results["pass"][0]
	if targetdbType == 'TERADATA':
		conn3=teradatasql.connect(host=tgtdbParams['SOURCE_HOST'],user=tgtdbParams['SOURCE_LOGIN'], password=tgtPassword)
	else:
		raise Exception("Unsupported Target db Type. Currently Teradata is only supported db type")
	step_name="Updating JCT run status"
	log_data(log_file,step_name,0)
	upd_jct_status('R')
	step_name="Inserting into job stream runs"
	log_data(log_file,step_name,0)
	ins_jct_run_status('Insert')
	step_name=f"Performing TD load"
	log_data(log_file,step_name,0)
	perform_td_load()
	step_name="Updating job stream runs"
	log_data(log_file,step_name,0)
	ins_jct_run_status('Update')
	step_name="Updating job JCT run status"
	log_data(log_file,step_name,0)
	upd_jct_status('C')
	step_name="Archiving data files"
	log_data(log_file,step_name,0)
	arch_files()
	conn.close()
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	log_data(err_file,f"Failure in line number {linenu} at step {step_name}. Please find below exception",1)
	log_data(err_file,str(e),1)
	if sql is not None:
		log_data(err_file,f"Last sql executed was :- {sql}",1)
	print(f"Below exception occurred in line number {linenu}  at step {step_name} while running stg2br/ss jct")
	print(str(e))
	sys.exit(1)