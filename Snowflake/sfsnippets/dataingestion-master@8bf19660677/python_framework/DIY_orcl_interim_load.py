#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_orcl_interim_load.py                                                                                              #
# Script to split and load files in parallel to snowflake stage                                                         #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Aug-2020                Initial Version                                                 #
# 1.1           Manick          Sep-2020                Make it common for both TD & Oracle                             #
# 1.2           Manick          Sep-2020                Issue with archive file mount creation for first time           #
# 1.3           Manick          Feb-2021                Raise exception from function to main thread                    #
# 1.4           Manick          May-2021                Replace fuser with psutil.get_files method                      #
#########################################################################################################################
import argparse,math,sys,pdb,os
from metadata_handler import *
from datetime import datetime
import threading,subprocess
from ConfigObject import ConfigObject
import psutil,shutil,time

def open_file_check(fname,prid,db_type):
	found=False
	if db_type == 'TERADATA':
		oscmd=f"fuser {fname} 2>/dev/null"
		in_use_chk=len(os.popen(oscmd).read())
		if in_use_chk > 0:
			found=True
		else:
			found=False
	else:
		pr_hand=psutil.Process(prid)
		op_files=pr_hand.open_files()
		for obj in op_files:
			if fname in str(obj):
				found=True
				break
	if found:
		return(1)
	else:
		return(0)
def recreate_stg_tbl(conn3,tgt_db_name,tgt_schema,tgt_table):
	global conn,reqid,step_name,log_file,env,src_base_schema,src_base_tbl,source_db_name,err_file,sql,jct
	sql=f"select nvl(job_stream_description,'') jsd,source_db_name sdn,source_schema ss,source_table_name stn from edw_job_streams where job_stream_id='{jct}'"
	desc_obj=execute_df_qry(conn, sql, reqid, conn)
	jct_desc=desc_obj["jsd"][0]
	source_db_name=desc_obj["sdn"][0]
	if ('srcBaseschema' in jct_desc) and ('srcBasetable' in jct_desc):
		src_base_schema=jct_desc.split(',')[0].split("=")[1]
		src_base_tbl=jct_desc.split(',')[1].split("=")[1]
	else:
		src_base_schema=desc_obj["ss"][0]
		src_base_tbl=desc_obj["stn"][0]

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

def file_line_check():
	try:
		global datafile,batch_id,log_file,reqid,jct,conn,cmd,src_rows,src_rows_flag
		log_data(log_file,"Now checking data with line breaks",0)
		file_lines_2_scan=1000
		batch_id_str=f'"{batch_id}"\n'
		end_count=0
		loop_cnt=0
		with open(datafile,'r') as f_dat:
			line=f_dat.readline()
			while line and loop_cnt < file_lines_2_scan:
				if line.endswith(batch_id_str):
					end_count+=1
				loop_cnt+=1
				line=f_dat.readline()
		if loop_cnt == end_count:
			log_data(log_file,"Data doesnt have line break. So skipping wc -l",0)
			if  src_rows_flag:
				return int(src_rows)
			else:
				sql=f"select src_rows,split_count from diy_master where reqid={reqid}"
				reqid_obj=execute_df_qry(conn,sql,reqid,conn)
				src_rows=reqid_obj["src_rows"][0]
				if src_rows == 1:
					log_data(log_file,"Since source row says 1 checking by counting data file",0)
					return(int(os.popen(cmd).read()))
				else:
					split_count=reqid_obj["split_count"][0]
					tot_lines=round(src_rows/split_count)
					return int(tot_lines)
		else:
			log_data(log_file,"Even though file size > 1 GB data has line break. So wc -l is mandatted",0)
			return(int(os.popen(cmd).read()))
	except Exception as e:
		print(f"Inside exception while checking for line count in {datafile}")
		print(str(e))
		return -1

def zip_and_upload(chunkfile,bucket_num):
	try:
		global log_file,step_name,target_table_name,configFile,sf_role,sf_wh,reqid,df_path,target_db,target_schema,env,err_file,sf_user,job_group_id,df_name,logs_dir,jct
		temp_wd='/apps/python/cwd'
		step_name=f'begining thread {bucket_num}'
		log_data(log_file,step_name,0)
		sql_list={}
		conn3=''
		out=os.popen(f'gzip -fc {chunkfile} > {temp_wd}/{chunkfile}.gz').read()
		if configFile.Config.PRDSUFF in env:
			sfEnv = configFile.Config.PRDSFACC
			datafile_dest=configFile.Config.datafile_dest_prd
		elif configFile.Config.STGSUFF in env:
			sfEnv = configFile.Config.STGSFACC
			datafile_dest=configFile.Config.datafile_dest_stg
		elif configFile.Config.DEVSUFF in env:
			sfEnv = configFile.Config.DVSFACC
			datafile_dest=configFile.Config.datafile_dest_dev
		datafile_dest=datafile_dest.replace('jobgroup',job_group_id)
		datafile_dest=datafile_dest.replace('env',env.upper())
		#sf_user=configFile.Config.sfUser
		messg=''
		bid=datetime.now().strftime('%Y%m%d%H%M%S%f')
		TEMPDIR=f"{temp_wd}/{jct}_{bid}"
		os.mkdir(TEMPDIR)
		schema_name=f"{target_db}.{target_schema}"
		put_cmd=f"PUT 'file://{temp_wd}/{chunkfile}.gz' '@%{target_table_name}' SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE AUTO_COMPRESS=FALSE PARALLEL=24" 
		step_name=f"Invoking put command for bucket {bucket_num}"
		log_data(log_file,step_name,0)
		run_cmd=f"/apps/edwsfdata/python/scripts/DIY_perform_put_with_s.py -e '{sfEnv}' -u '{sf_user}' -w '{sf_wh}' -r '{sf_role}' -p '{put_cmd}' -i {reqid} -t '{TEMPDIR}' -b {bucket_num} -s {schema_name} -n {target_table_name}"
		#print(f"Invoking put command for bucket {bucket_num}")
		cmnd=subprocess.Popen(run_cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		pid=cmnd.pid
		step_name=f"Invoking perform_put for bucket {bucket_num} with pid {pid}"
		log_data(log_file,step_name,0)
		(output, error) = cmnd.communicate()
		Err=error.decode('ascii')
		Out=output.decode('ascii')
		out=os.popen(f'rm -rf {TEMPDIR}').read()
		if (Err is not None and len(Err)>0) or (Out.strip('\n') != '1'):
			err_msg=f"Exception encountered while loading {chunkfile}.gz to SF Stage. Err is {Err} and Out is {Out}"
			raise Exception(f"err_msg. Exeception is {Err}")
		#time.sleep(15)## sleep 15 seconds before checking put data
		step_name=f"Checking staged data for bucket {bucket_num}"
		log_data(log_file,step_name,0)
		attempts=0
		succ=True
		step_name=f"opening sf connection for bucket {bucket_num}"
		log_data(log_file,step_name,0)
		sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","INTERIM_LOAD")
		conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,query_tag=sf_query_tag)
		conn=-1
		step_name=f"validating staged data for bucket {bucket_num}"
		log_data(log_file,step_name,0)
		while succ:
			try:
				sql_list[bucket_num]=f"list '@%{target_table_name}/{chunkfile}.gz'"
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
		sql=f"copy into {schema_name}.{target_table_name} from '@%{target_table_name}/{chunkfile}.gz'"
		sql+=f" ON_ERROR = 'CONTINUE' PURGE = TRUE FILE_FORMAT=EDW_UTIL_DB.FILE_FORMATS.CSV_PIPE_WITHOUT_HEADER_NO_UTF8"
		#execute_qry(conn3,sql,reqid,conn)
		copy_res_obj=execute_df_qry(conn3,sql,reqid,conn)
		if type(copy_res_obj) is int:
			raise Exception(f"Copy to stage failed. Please check sql {sql} and retry")
		rows_parsed=copy_res_obj["rows_parsed"][0]
		rows_loaded=copy_res_obj["rows_loaded"][0]
		errors=copy_res_obj["errors_seen"][0]
		row_details=f"{bucket_num}~{rows_parsed}~{rows_loaded}~{errors}"
		if errors>0:
			log_data(err_file,f"Stage file (split {bucket_num}) had {rows_parsed} rows out of which {rows_loaded} rows loaded and {errors} rows failed",1)
			raise Exception(f"Some of the rows for bucket {bucket_num} failed to load. Please check")
		log_data(log_file,row_details,0)
		#os.remove(chunkfile + '.gz')
		#shutil.move(f"{chunkfile}.gz",datafile_dest)
		compress_log=open(f"{logs_dir}/compress.log",'a')
		if bucket_num == 1:
			out=os.popen(f'mkdir -p {datafile_dest}').read()
			oscmd=f"mv {temp_wd}/{chunkfile}.gz {datafile_dest}/{df_name}.gz && rm {chunkfile}"
		else:
			oscmd=f"cat {temp_wd}/{chunkfile}.gz >> {datafile_dest}/{df_name}.gz && rm {temp_wd}/{chunkfile}.gz && rm {chunkfile}"
		proc=subprocess.Popen([oscmd],stdout=compress_log, stderr=compress_log, shell=True)
		pid=proc.pid
		step_name=f"Compress and move file script submitted with pid {pid}"
		log_data(log_file,step_name,0)
		if isinstance(conn3,snowflake.connector.connection.SnowflakeConnection):
			conn3.close()
	
	except  Exception as e:
		print(f"Inside exception at step_name {step_name} for bucket {bucket_num}")
		print(str(e))
		print(messg)
		raise


## Main Program start here
pgmVersion='1.0'
parser = argparse.ArgumentParser(description='Script used to push split and load to SF stg',
    epilog='Example: python DIY_split_load.py -e environment -r reqid -j jct -t timestamp')
parser.add_argument('-e','--env',required=True,
        help='Environment for which src2stg needs to be executed (DV3/TS3/PRD)')
parser.add_argument('-r','--reqid',
        help='Reqid from DIY tables')
parser.add_argument('-j','--jct',
        help='Job Stream Id')
parser.add_argument('-f','--fileName',
        help='Flat File Name')
parser.add_argument('-s','--srcRows',
        help='Source batch row count')
parser.add_argument('-t','--ts',
        help='Timestamp Suffix of datafile generation')
args=parser.parse_args()
env=args.env
reqid=args.reqid
jct=args.jct
flat_file=args.fileName
src_rows=args.srcRows
ts=args.ts
try:
	currTime=datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
	step_name="Establishing Repos connection"
	configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
	conn = open_oracle_connection(env,None,True)
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	sql=f"select logs_dir from diy_master where reqid={reqid}"
	logs_dir_obj=execute_df_qry(conn, sql, reqid, conn)
	logs_dir=logs_dir_obj["logs_dir"][0]
	#jct=param_obj["job_stream_id"][0]
	if not os.path.exists(os.path.dirname(logs_dir)):
		raise Exception("Unable to locate logs_dir. Please check")
	step_name="Opening log and error File"
	log_file=f"{logs_dir}/{jct}_splitload.log"
	err_file=f"{logs_dir}/{jct}_splitload.err"
	step_name="Querying Job Params"
	log_data(log_file,step_name,0)
	sql=f"select target_table_name,batch_id,SRC2STG_FF_PATH df_path,CURRENT_SRC2STG_FF_NAME df_name,sf_role,sf_warehouse"
	sql+=f",target_db_name,target_schema,target_db_connection,job_group_id,connection_type"
	sql+=f" from diy_job_params where reqid={reqid} and job_stream_id='{jct}'"
	param_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(param_obj) == 0:
		raise Exception(f"Could not query job params for reqid {reqid}")
	target_table_name=param_obj["target_table_name"][0]
	batch_id=param_obj["batch_id"][0]
	df_path=param_obj["df_path"][0]
	df_name=param_obj["df_name"][0]
	sf_role=param_obj["sf_role"][0]
	sf_wh=param_obj["sf_warehouse"][0]
	target_db=param_obj["target_db_name"][0]
	target_schema=param_obj["target_schema"][0]
	tgt_db_conn=param_obj["target_db_connection"][0]
	job_group_id=param_obj["job_group_id"][0]
	db_type=param_obj["connection_type"][0]
	sql=f"select to_char(parameter_value) pv from eds_data_catalog.edw_parameter where parameter_type='{tgt_db_conn}'"
	sql+=f" and environment_name='{env}' and parameter_name='CONNECTION_USER'"
	sf_user_obj=execute_df_qry(conn, sql, reqid, conn)
	sf_user=sf_user_obj["pv"][0]
	log_data(log_file,f"flat_file is ~{flat_file}~",0)
	if flat_file is None or len(flat_file) == 0:
		datafile=f"{df_path}/{df_name}"
	else:
		datafile=flat_file
	src_rows_flag=False
	if src_rows is not None:
		src_rows=int(src_rows)
		src_rows_flag=True
	if configFile.Config.PRDSUFF in env:
		sfEnv = configFile.Config.PRDSFACC
	elif configFile.Config.STGSUFF in env:
		sfEnv = configFile.Config.STGSFACC
	elif configFile.Config.DEVSUFF in env:
		sfEnv = configFile.Config.DVSFACC
	step_name="Switching current directory"
	log_data(log_file,step_name,0)
	sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","INTERIM_LOAD")
	os.chdir(df_path)
	curr_pid=os.getpid()
	line=''
	thread_array=[]
	file_id=1
	file_list=list()
	touch_file=f"{logs_dir}/{jct}_{ts}"
	loop_cnt=1
	main_thread=None
	tbl_created=False
	while (os.path.exists(touch_file)):
		ppid=os.getppid()
		log_data(log_file,"Touch file found",0)
		if db_type == 'TERADATA':
			file_2_process=f"{datafile}-1-{file_id}"
			chunkfile=f"{df_name}-1-{file_id}"
		else:
			file_2_process=f"{datafile}_{ts}_{file_id}"
			chunkfile=f"{df_name}_{ts}_{file_id}"
		oscmd=f"fuser {chunkfile} 2>/dev/null"
		log_data(log_file,f"file_2_process is {file_2_process}",0)
		if (os.path.exists(file_2_process)):
			log_data(log_file,"Checking if file is in use",0)
			#while (len(os.popen(oscmd).read())) > 0:
			while (open_file_check(chunkfile,ppid,db_type) == 1):
				log_data(log_file,f"now sleeping as {chunkfile} is in use",0)
				time.sleep(10) ## checks every 10 seconds if file handle is open or closed
			if file_id == 1:
				conn4=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,query_tag=sf_query_tag)
				recreate_stg_tbl(conn4,target_db,target_schema,target_table_name)
				tbl_created=True
				conn4.close()
			step_name=f"Iterating datafile {file_2_process}"
			log_data(log_file,step_name,0)
			bid=datetime.now().strftime('%Y%m%d%H%M%S')
			#prcmd=f"ps -ef | grep {curr_pid}  | grep -v defunct | grep -v grep | wc -l"
			prcmd="ps -ef | grep -v defunct | grep -v grep | awk '{print $3}' | grep " + str(curr_pid) + "  | wc -l"
			#chunkfile=file_2_process
			log_data(log_file,f"Chunkfile is {chunkfile}",0)
			thread_array=threading.Thread(target=zip_and_upload, args=(chunkfile,file_id))
			thread_array.start()
			time.sleep(5) ## sleep 5 second. 0.5 changed to 5 to avoid bug with file not found error.
			main_thread = threading.currentThread()
			file_list.append(chunkfile)
			file_id+=1
			#cnt=int(os.popen(prcmd).read())
			#cnt=cnt-1 ## ignore parent pid
			loop_cnt=threading.active_count()-1
			while loop_cnt >= 10:
				log_data(log_file,f"More than 10 threads submitted. Waiting for few to complete",0)
				loop_cnt=threading.active_count()-1
				ins_while_flag=True
				time.sleep(30) ## Sleep 30 seconds before checking curreng active threads
		else:
			log_data(log_file,f"Sleeping as touch file found but no file is available to process",0)
			time.sleep(6) ## sleep 6 seconds before checking for next available file
	if (threading.active_count() > 1):
		for t in threading.enumerate():
			if main_thread is not None and t is main_thread:
				continue
			if t.getName() != 'MainThread':
				log_data(log_file,f"Waiting for {t.getName()} to complete",0)
				t.join()
	
	if db_type == 'TERADATA':
		filecmd=f"ls -rt {df_name}* 2>/dev/null | grep -v '.gz'"
	else:
		filecmd=f"ls -rt {df_name}_{ts}_* 2>/dev/null | grep -v '.gz'"
	out=os.popen(filecmd).read()
	thread_array=[]
	thread_count=0
	file_out_list=out.split('\n')
	for file_ptr in file_out_list[:-1]:
		if file_ptr not in file_list:
			if not tbl_created:
				conn4=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,query_tag=sf_query_tag)
				recreate_stg_tbl(conn4,target_db,target_schema,target_table_name)
				tbl_created=True
				conn4.close()
			file_2_process=file_ptr
			chunkfile=file_ptr
			step_name=f"Iterating Datafile {file_2_process}"
			log_data(log_file,step_name,0)
			thread_array.append(threading.Thread(target=zip_and_upload, args=(chunkfile,file_id)))
			#zip_and_upload(chunkfile,file_id)
			file_list.append(file_2_process)
			file_id+=1
	loop_cnt=1
	tot_work_count=len(thread_array)
	cum_loop_cnt=0
	ins_while_flag=False
	for worker in thread_array:
		t=worker.start()
		time.sleep(2) ## sleep 2 seconds between each thread invocation
		main_thread = threading.currentThread()
		while loop_cnt >= 10:
			loop_cnt=threading.active_count()-1
			ins_while_flag=True
			time.sleep(30) ## Sleep 30 seconds before checking curreng active threads
		cum_loop_cnt+=1
		if cum_loop_cnt >= tot_work_count:
			log_data(log_file,"Done with launching all workers. Waiting for final set to complete",0)
			for t in threading.enumerate():
				if t is main_thread:
					continue
				if t.getName() != 'MainThread':
					log_data(log_file,f"Waiting for {t.getName()} to complete",0)
					t.join()
		if not ins_while_flag:
			loop_cnt+=1
			ins_while_flag=False
		loop_cnt+=1

	log_data(log_file,f"Succesfully processed {file_list} files",0)
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Error occurred at line number {linenu} while splitting stage data for upload at step - {step_name}")
	print (str(e))
	sys.exit(1)	

