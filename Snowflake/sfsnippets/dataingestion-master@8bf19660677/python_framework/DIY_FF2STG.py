#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_FF_2_STG.py                                                                                                       #
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Apr-2019                Initial Version                                                 #
#########################################################################################################################
import argparse,pdb
import cx_Oracle
import os, re, string, subprocess, sys
from datetime import datetime
from datetime import timedelta
import time,fnmatch,csv
import threading,shutil
from metadata_handler import *
from ConfigObject import ConfigObject

## function defintion starts here

def upd_jct_status(run_status):
	global conn,jct,reqid,step_name,log_file,step_name,sql
	step_name=f"Updating edw_job_stream status to {run_status}"
	log_data(log_file,step_name,0)
	sql=f"update edw_job_streams set run_status='{run_status}' where job_stream_id='{jct}'"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)

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

def updDIYmstr(reqid,updcol,colval,conn,err_file,log_file,emailAdd):
	if colval =='curr_date':
		sql=f"update DIY_master set {updcol}=sysdate,last_update_time = sysdate where reqid={reqid}"
	else:
		colval=colval.replace("'","''")
		sql=f"update DIY_master set {updcol}='{colval}',last_update_time = sysdate where reqid={reqid}"
	execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,'commit',reqid,conn)
	
def insDIYlines(reqid,prephase,currphase,conn,err_file,emailAdd):
	sql="insert into DIY_lines(reqid,step_name,step_start_time) values "
	sql+=f"({reqid},'{currphase}',sysdate)" 
	execute_qry(conn,sql,reqid,conn)
	sql=f"update DIY_lines set step_end_time=sysdate where reqid={reqid} and step_name='{prephase}'"
	execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,'commit',reqid,conn)

def recreate_stg_tbl(targetDb,targetSchema,targetTable,jobStreamid):
	global sfEnv,conn,reqid,envType,sample_file,sf_role,sf_wh,sf_user,conn3
	sql=f"Select target_db_connection tdc,sf_warehouse wh,sf_role sr from edw_job_streams where job_stream_id='{jobStreamid}'"
	sfobj=execute_df_qry(conn,sql,reqid,conn)
	who_cols="EDWSF_CREATE_DTM varchar(16777216),EDWSF_CREATE_USER varchar(16777216),EDWSF_UPDATE_DTM varchar(16777216),EDWSF_UPDATE_USER varchar(16777216),EDWSF_BATCH_ID varchar(16777216)"
	tdc=sfobj["tdc"][0]
	sf_wh=sfobj["wh"][0]
	sf_role=sfobj["sr"][0]
	sql=f"select to_char(parameter_value) pv from eds_data_catalog.edw_parameter where parameter_type='{tdc}' and environment_name='{envType}' and parameter_name='CONNECTION_USER'"
	pv_obj=execute_df_qry(conn,sql,reqid,conn)
	sf_user=pv_obj["pv"][0]
	sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","FF2STG")
	conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,query_tag=sf_query_tag)
	i=1
	with open(sample_file,newline=None) as csvfile:
		file = csv.reader(csvfile, delimiter='|', quotechar='"')
		for row in file:
			if i == 1:
				header_line=row
			else:
				break
			i+=1
	sql=f'create or replace table {targetDb}.{targetSchema}.{targetTable} ('
	for column_name in header_line:
		sql+=f"{column_name} varchar(16777216),"
	#sql += who_cols
	sql=sql[:-1] ## strip of last comma
	sql+=")"
	execute_qry(conn3,sql,reqid,conn)
	
def upload_stg_tbl(files_2_process):
	curr_pid=os.getpid()
	j=0
	prcmd="ps -ef | grep -v defunct | grep -v grep | awk '{print $3}' | grep " + str(curr_pid) + "  | wc -l"
	for chunkfile in files_2_process:
		j+=1
		cnt=int(os.popen(prcmd).read())
		if cnt < 10:
			thread_array=threading.Thread(target=zip_and_upload, args=(chunkfile,j))
			thread_array.start()
			time.sleep(0.5) ## sleep 0.5 second
			main_thread = threading.currentThread()
		else:
			log_data(log_file,f"More than 10 put operations submitted. Waiting for few to complete",0)
			while (int(os.popen(prcmd).read())) >= 10:
				time.sleep(60)
			thread_array=threading.Thread(target=zip_and_upload, args=(chunkfile,j))
			thread_array.start()
			main_thread = threading.currentThread()

	for t in threading.enumerate():
		if main_thread is not None and t is main_thread:
			continue
		log_data(log_file,f"Waiting for {t.getName()} to complete",0)
		t.join()

def zip_and_upload(chunkfile,bucket_num):
	try:
		global log_file,step_name,targetTable,configFile,sf_role,sf_wh,reqid,filePath,targetDb,targetSchema,envType,err_file,sf_user,jobGroup,filePath
		step_name=f'begining thread {bucket_num}'
		env=envType
		log_data(log_file,step_name,0)
		sql_list={}
		out=os.popen(f'gzip -f {filePath}/{chunkfile}').read()
		if configFile.Config.PRDSUFF in env:
			sfEnv = configFile.Config.PRDSFACC
			datafile_dest=configFile.Config.datafile_dest_prd
		elif configFile.Config.STGSUFF in env:
			sfEnv = configFile.Config.STGSFACC
			datafile_dest=configFile.Config.datafile_dest_stg
		elif configFile.Config.DEVSUFF in env:
			sfEnv = configFile.Config.DVSFACC
			datafile_dest=configFile.Config.datafile_dest_dev
		datafile_dest=datafile_dest.replace('jobgroup',jobGroup)
		datafile_dest=datafile_dest.replace('env',env.upper())
		#sf_user=configFile.Config.sfUser
		messg=''
		bid=datetime.now().strftime('%Y%m%d%H%M%S%f')
		TEMPDIR=f"{filePath}/{bid}"
		os.mkdir(TEMPDIR)
		schema_name=f"{targetDb}.{targetSchema}"
		put_cmd=f"PUT 'file://{filePath}/{chunkfile}.gz' '@%{targetTable}' SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE AUTO_COMPRESS=FALSE PARALLEL=24" 
		step_name=f"Invoking put command for bucket {bucket_num}"
		log_data(log_file,step_name,0)
		run_cmd=f"/apps/edwsfdata/python/scripts/DIY_perform_put_with_s.py -e '{sfEnv}' -u '{sf_user}' -w '{sf_wh}' -r '{sf_role}' -p '{put_cmd}' -i {reqid} -t '{TEMPDIR}' -b {bucket_num} -s {schema_name} -n {targetTable}"
		#print(f"Invoking put command for bucket {bucket_num}")
		cmnd=subprocess.Popen(run_cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		pid=cmnd.pid
		step_name=f"Invoking perform_put for bucket {bucket_num} with pid {pid}"
		log_data(log_file,step_name,0)
		(output, error) = cmnd.communicate()
		Err=error.decode('ascii')
		Out=output.decode('ascii')
		#out=os.popen(f'rm -rf {TEMPDIR}').read()
		if (Err is not None and len(Err)>0) or (Out.strip('\n') != '1'):
			err_msg=f"Exception encountered while loading {chunkfile}.gz to SF Stage. Err is {Err} and Out is {Out}"
			raise Exception(f"{err_msg}. Exeception is {Err}")
		#time.sleep(15)## sleep 15 seconds before checking put data
		step_name=f"Checking staged data for bucket {bucket_num}"
		log_data(log_file,step_name,0)
		attempts=0
		succ=True
		step_name=f"opening sf connection for bucket {bucket_num}"
		log_data(log_file,step_name,0)
		sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","FF2STG")
		conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,query_tag=sf_query_tag)
		conn=-1
		step_name=f"validating staged data for bucket {bucket_num}"
		log_data(log_file,step_name,0)
		while succ:
			try:
				sql_list[bucket_num]=f"list '@%{targetTable}/{chunkfile}.gz'"
				sf_stg_cnt_obj=execute_df_qry(conn3, f'use schema {schema_name}', reqid, conn)
				sf_stg_cnt_obj=execute_df_qry(conn3, sql_list[bucket_num], reqid, conn)
				#print(sf_stg_cnt_obj)
				sf_stg_cnt=sf_stg_cnt_obj["name"][0]
				#print(sf_stg_cnt)
				succ=False
			except Exception as e:
				attempts+=1
				messg=f"Attempting to query stage data for bucket {bucket_num} on attempt {attempts} failed with exception - {e}"
				time.sleep(10)
				if attempts == 3:
					succ=False
					raise Exception (f"Checking staged data failed even after 3 attempts.. Hence giving up for bucket {bucket_num}")
				pass
		step_name=f"performing copy for bucket {bucket_num}"
		log_data(log_file,step_name,0)
		sql=f"copy into {schema_name}.{targetTable} from '@%{targetTable}/{chunkfile}.gz'"
		sql+=f" ON_ERROR = 'CONTINUE' PURGE = TRUE FILE_FORMAT=EDW_UTIL_DB.FILE_FORMATS.CSV_PIPE_WITH_HEADER_NO_UTF8"
		#execute_qry(conn3,sql,reqid,conn)
		copy_res_obj=execute_df_qry(conn3,sql,reqid,conn)
		if type(copy_res_obj) is int:
			raise Exception(f"Copy to stage failed. Please check sql {sql} and retry")
		rows_parsed=copy_res_obj["rows_parsed"][0]
		rows_loaded=copy_res_obj["rows_loaded"][0]
		errors=copy_res_obj["errors_seen"][0]
		row_details=f"{bucket_num}~{rows_parsed}~{rows_loaded}~{errors}"
		if errors>0:
			out=os.popen(f'gunzip -f {filePath}/{chunkfile}').read() 
			log_data(err_file,f"Stage file (split {bucket_num}) had {rows_parsed} rows out of which {rows_loaded} rows loaded and {errors} rows failed",1)
			raise Exception(f"Some of the rows for bucket {bucket_num} failed to load. Please check")
		log_data(log_file,row_details,0)
		#os.remove(chunkfile + '.gz')
		shutil.move(f"{filePath}/{chunkfile}.gz",datafile_dest)
		try:
			os.rmdir(TEMPDIR)
		except Exception as e:
			pass
		
	except  Exception as e:
		print(f"Inside exception at step_name {step_name} for bucket {bucket_num}")
		print(str(e))
		print(messg)


def zeroRecIngestion(jobStream,reqid,conn,err_file,log_file,emailAdd,envType,LOGS_DIR):
	log_data(log_file,"Now marking SRC2STG and STG2BR workflows as completed since no record to process",1)
	sql=f"select count(*) cnt from edw_job_streams where job_stream_id='{jobStream}'"
	jct_obj=execute_df_qry(conn,sql,reqid,conn)
	jctCnt=jct_obj["cnt"][0]
	if jctCnt == 0:
		log_data(log_file,f"Job stream - {jobStream} doesnt exist on database.. Hence nothing to mark complete",1)
	else:
		sql=f"Update edw_job_streams set run_status='C' where job_stream_id='{jobStream}'"
		execute_df_qry(conn,sql,reqid,conn)
		log_data(log_file,"Now pulling corresponding STG2BR to mark complete",0)
		sql=f"select target_db_name tdn,target_schema ts,target_db_connection tdc,target_table_name ttn,EXTRACT_TYPE et from  EDW_JOB_STREAMS where job_stream_id='{jobStream}'"
		resultsObj=execute_df_qry(conn,sql,reqid,conn)
		targetDb=resultsObj["tbn"][0]
		targetSchema=resultsObj["ts"][0]
		targetConn=resultsObj["tdc"][0]
		targetTbl=resultsObj["ttn"][0]
		load_type=resultsObj["et"][0]
		#sql=f"select job_stream_id from edw_job_streams where source_db_connection='{targetConn}' and source_db_name='{targetDb}' and source_schema='{targetSchema}' and source_table_name='{targetTbl}' and active_ind='Y'"
		sql=f"select job_stream_id jsi,target_db_name tdn,target_schema ts,target_table_name ttn from edw_job_streams where job_stream_id_link='{jobStream}' and active_ind='Y'"
		resultsObj=execute_df_qry(conn,sql,reqid,conn)
		brJobStream=resultsObj["jsi"][0]
		brdbName=resultsObj["tdn"][0]
		brdbSchema=resultsObj["ts"][0]
		brtbl=resultsObj["ttn"][0]
		if length(resultsObj)>0:
			log_data(log_file,f"Found corresponding BR Jobstream as {brJobStream}. Hence marking it as completed as well",1)
			sql=f"Update edw_job_streams set run_status='C' where job_stream_id='{brJobStream}'"
			execute_qry(conn,sql,reqid,conn)
			execute_qry(conn,'commit',reqid,conn)
		updDIYmstr(reqid,'end_time','curr_date',conn,err_file,log_file,emailAdd)
		updDIYmstr(reqid,'current_phase','Success',conn,err_file,log_file,emailAdd)
		insDIYlines(reqid,'Invoking_Data_Load','Completed_Ingestion',conn,err_file,emailAdd)

#
## Main program starts here
#
pgmVersion='1.0'
ejcparams={}
configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
BASE_DIR=configFile.Config.BASE_DIR
ftime=datetime.now().strftime('%Y%m%d%H%M%S')
rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
jctCnt=0
parser = argparse.ArgumentParser(description='Script to Peform data ingestion into Snowflake',
    epilog='Example: python DIY_FF_2_STG.py -e env_type -j job_stream_id -a email -r reqid -l logs_dir')
parser.add_argument('-e','--env',required=True,
	help='Envrionment type (TS*/DV*/PRD) of Job that is already ingested')
parser.add_argument('-j','--jobstreamid',required=True,
	help='Existing job stream id which you want to ingest')
parser.add_argument('-a','--email',required=True,
	help='Comma seperated email list to which notification needs to be sent out on ingestion status')
parser.add_argument('-r','--reqid',
	help='Reqid if invoked from DIY_src2stg.py')
parser.add_argument('-l','--logs_dir',
	help='Logs directory if invoked from DIY_src2stg.py ')
args=parser.parse_args()
envType=args.env
jobStreamid=args.jobstreamid
emailAdd=args.email
reqid=args.reqid
LOGS_DIR=args.logs_dir
sf_user='EDW_DATALAKE_SVC'
sfWh=None

try:
	step_name='Init'
	currHostName=os.uname()[1]
	currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
	if currOsUser == "":
		try:
			currOsUser=os.getlogin()
		except Exception as e:
			currOsUser=c_getuser()
			pass
	if LOGS_DIR is not None:
		if not os.path.exists(os.path.dirname(LOGS_DIR)):
			LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
			os.mkdir(LOGS_DIR)
	else:
		rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
		LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
		os.mkdir(LOGS_DIR)
	if reqid is None:
		reqid=str(datetime.now().strftime('%Y%m%d%H%M%S%f'))
	log_file=f"{LOGS_DIR}/FFupload.log"
	err_file=f"{LOGS_DIR}/FFupload.err"
	if configFile.Config.PRDSUFF in envType:
		sfEnv = 'cisco.us-east-1'
	elif configFile.Config.STGSUFF in envType:
		sfEnv = 'ciscostage.us-east-1'
	elif configFile.Config.DEVSUFF in envType:
		sfEnv = 'ciscodev.us-east-1'
	step_name='Establishing repos connection'
	log_data(log_file,f"At step {step_name}",0)
	conn = open_oracle_connection(envType,None,True)
	cursor1=conn.cursor()
	if type(conn) is int:
		raise Exception("Unable to connect to repos DB")
	step_name='Jct Validation'
	log_data(log_file,f"At step {step_name}",0)
	sql=f"select workflow_type,active_ind,run_status,job_group_id,source_db_name,source_schema,source_table_name,target_schema,target_table_name,"
	sql+=f"source_db_connection sdc,src2stg_ff_path sfp,target_db_name from edw_job_streams where job_stream_id='{jobStreamid}'"
	jctobj=execute_df_qry(conn,sql,reqid,conn)
	if len(jctobj) == 0:
		raise Exception(f"Job stream {jobStreamid} could not be located in {envType}")
	wf_type=jctobj["workflow_type"][0]
	act_ind=jctobj["active_ind"][0]
	run_status=jctobj["run_status"][0]
	jobGroup=jctobj["job_group_id"][0]
	sourceDb=jctobj["source_db_name"][0]
	sourceSchema=jctobj["source_schema"][0]
	sourceTable=jctobj["source_table_name"][0]
	targetSchema=jctobj["target_schema"][0]
	targetTable=jctobj["target_table_name"][0]
	sdc=jctobj["sdc"][0]
	filePath=jctobj["sfp"][0]
	targetDb=jctobj["target_db_name"][0]
	jct=jobStreamid
	sql=f"select distinct parameter_category pc from eds_data_catalog.edw_parameter where parameter_type='{sdc}' and environment_name='{envType}'"
	st_obj=execute_df_qry(conn,sql,reqid,conn)
	sourcedbType=st_obj["pc"][0]
	if wf_type != 'FF2STG':
		raise Exception(f"Invalid Workflow Type ({wf_type}) for jct {jobStreamid}. DIY_FF2_STG can be invoked only for FF2STG workfow type")
	if act_ind != 'Y':
		raise Exception(f"Invalid Active Indicator ({act_ind}) for jct {jobStreamid}")
	Init_flag=True
	if run_status == "P":
		ins_checkpoint('Insert','Init')
		ins_jct_run_status('Insert')
		ins_checkpoint('Update','Create_Stage_Table')
		Init_flag=False
		step_name="DIY_Master insertion"
		log_data(log_file,f"At step {step_name}",0)
		sql="insert into DIY_master(reqid,Ingestion_type,logs_dir,env_type,job_group,source_db_name,source_db_type,source_schema,source_table,target_schema,"
		sql+="target_table,current_phase,start_time,attribute1,attribute2,attribute3,attribute4,attribute5,load_type,retryflag) values "
		sql+=f"({reqid},'{wf_type}','{LOGS_DIR}','{envType}','{jobGroup}','{sourceDb}','{sourcedbType}','{sourceSchema}',"
		sql+=f"'{sourceTable}','{targetSchema}','{targetTable}','INIT',sysdate,"
		sql+=f"'{currOsUser}','{emailAdd}','{jobStreamid}','{currHostName}','{pgmVersion}','STREAM','F')"
		execute_qry(conn,sql,reqid,conn)
		execute_qry(conn,"commit",reqid,conn)
		upd_jct_status('R')
		insDIYlines(reqid,'NONE','INIT',conn,err_file,emailAdd)
	else:
		sql=f"select max(reqid) reqid from diy_master where attribute3='{jct}'"
		reqid_obj=execute_df_qry(conn,sql,reqid,conn)
		reqid=reqid_obj["reqid"][0]
		if len(reqid_obj) == 0:
			 raise Exception ("JCT in Running status but could not find entry in DIY table")
		sql=f"update diy_master set logs_dir='{LOGS_DIR}',job_group='{jobGroup}',target_schema='{targetSchema}',target_table='{targetTable}',"
		sql+=f"retryflag='T',attribute1='{currOsUser}',err_msg=NULL,attribute2='{emailAdd}',attribute4='{currHostName}',attribute5='{pgmVersion}'"
		sql+=f" where reqid={reqid}"
		execute_qry(conn,sql,reqid,conn)
		execute_qry(conn,"commit",reqid,conn)
	sql=f"select current_phase cp from diy_src2stg_checkpoint where reqid={reqid} and job_stream_id='{jct}'"
	cp_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(cp_obj) == 0:
		raise Exception(f"Unable to query diy_src2stg_checkpoint for reqid {reqid} and jct {jct} ")
	curr_status=cp_obj["cp"][0]
	init_curr_status=curr_status
	if curr_status == "Create_Stage_Table":
		step_name="Checking for pending files to process"
		log_data(log_file,f"At step {step_name}",0)
		updDIYmstr(reqid,'current_phase','Invoking_Data_Load',conn,err_file,log_file,emailAdd)
		insDIYlines(reqid,'INIT','Invoking_Data_Load',conn,err_file,emailAdd)
		files_2_process=fnmatch.filter(os.listdir(filePath),'*.csv')
		no_of_files=len(files_2_process)
		if no_of_files == 0:
			zeroRecIngestion(jobStreamid,reqid,conn,err_file,log_file,emailAdd,envType,LOGS_DIR)
			ins_jct_run_status('Update')
			upd_jct_status('C')
			ins_checkpoint('Update','Complete')
			sys.exit(0)
		if no_of_files == 1:
			linecnt_cmd="wc -l " + filePath + "/*.csv | awk '{print $1}'"
		else:
			linecnt_cmd="wc -l " + filePath + "/*.csv | grep total | awk '{print $1}'"
		src_cnt=int(os.popen(linecnt_cmd).read())-int(no_of_files)
		updDIYmstr(reqid,'src_rows',str(src_cnt),conn,err_file,log_file,emailAdd)
		updDIYmstr(reqid,'split_count',str(no_of_files),conn,err_file,log_file,emailAdd)
		step_name="Recreating stage table"
		log_data(log_file,f"At step {step_name}",0)
		updDIYmstr(reqid,'current_phase','Creating_Stage_Table',conn,err_file,log_file,emailAdd)
		insDIYlines(reqid,'Invoking_Data_Load','Creating_Stage_Table',conn,err_file,emailAdd)
		sample_file=f"{filePath}/{files_2_process[0]}"
		recreate_stg_tbl(targetDb,targetSchema,targetTable,jobStreamid)
		ins_checkpoint('Update','Upload_CSV_Data')
		curr_status='Upload_CSV_Data'
	if curr_status == "Upload_CSV_Data":
		files_2_process=fnmatch.filter(os.listdir(filePath),'*.csv')
		no_of_files=len(files_2_process)
		if no_of_files > 0:
			step_name="Uploading files to stage table"
			log_data(log_file,f"At step {step_name}",0)
			updDIYmstr(reqid,'current_phase','Uploading_Stage_Table',conn,err_file,log_file,emailAdd)
			insDIYlines(reqid,'Creating_Stage_Table','Uploading_Stage_Table',conn,err_file,emailAdd)
			upload_stg_tbl(files_2_process)
		ins_checkpoint('Update','Completed_Upload')
		curr_status='Completed_Upload'
	if curr_status == 'Completed_Upload':
		sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","FF2STG")
		conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,query_tag=sf_query_tag)
		sql=f"select count(*) cnt from {targetDb}.{targetSchema}.{targetTable}"
		cnt_obj=execute_df_qry(conn3,sql,reqid,conn)
		tgt_cnt=cnt_obj["cnt"][0]
		updDIYmstr(reqid,'tgt_rows',str(tgt_cnt),conn,err_file,log_file,emailAdd)
		sql=f"select count(*) cnt,header__operation from  {targetDb}.{targetSchema}.{targetTable} group by 2"
		grp_obj=execute_df_qry(conn3,sql,reqid,conn)
		log_data(log_file,f"Given below are SQL operation cnt.\n {grp_obj}",0)
		ins_jct_run_status('Update')
		insDIYlines(reqid,'Uploading_Stage_Table','Completed_Ingestion',conn,err_file,emailAdd)
		updDIYmstr(reqid,'end_time','curr_date',conn,err_file,log_file,emailAdd)
		updDIYmstr(reqid,'current_phase','Success',conn,err_file,log_file,emailAdd)
		upd_jct_status('C')
		ins_checkpoint('Update','Complete')
		conn3.close()
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	errMsg=e
	Sub=f"Error occurred at line number {linenu} while splitting stage data for upload at step - {step_name}\n{e}"
	log_data(err_file,Sub,1)
	sys.exit(1)	
	sendEmail(emailAdd,log_file,err_file,errMsg,Subject,cursor1)

