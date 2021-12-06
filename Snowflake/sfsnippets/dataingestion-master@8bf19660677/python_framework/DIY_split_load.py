#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_split_load.py                                                                                                     #
# Script to split and load files in parallel to snowflake stage                                                         #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Apr-2020                Initial Version                                                 #
# 1.1           Manick          May-2020                Ged rid of wc -l since it is costly for large files             #
# 1.2           Manick          May-2020                Control parallel put to max of 10 at a time                     #
# 1.3           Manick          May-2020                Enhance multi file upload for HANA NRT                          #
# 1.4           Manick          May-2020                Parallel call is overwrite zip file                             #
# 1.5           Manick          May-2020                Pass src_rows for HANANRT type                                  #
# 1.6           Manick          Jun-2020                Fetch SF user from JOB_Params                                   #
# 1.7           Manick          Sep-2020                Enable SF query tag                                             #
#########################################################################################################################
import argparse,math,sys,pdb,os
from metadata_handler import *
from datetime import datetime
import threading,subprocess
from ConfigObject import ConfigObject

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
		global log_file,step_name,target_table_name,configFile,sf_role,sf_wh,reqid,df_path,target_db,target_schema,env,err_file,sf_user
		step_name=f'begining thread {bucket_num}'
		log_data(log_file,step_name,0)
		sql_list={}
		conn3=''
		out=os.popen(f'gzip -f {chunkfile}').read()
		if configFile.Config.PRDSUFF in env:
			sfEnv = configFile.Config.PRDSFACC
		elif configFile.Config.STGSUFF in env:
			sfEnv = configFile.Config.STGSFACC
		elif configFile.Config.DEVSUFF in env:
			sfEnv = configFile.Config.DVSFACC
		#sf_user=configFile.Config.sfUser
		messg=''
		bid=datetime.now().strftime('%Y%m%d%H%M%S%f')
		TEMPDIR=f"{df_path}/{bid}"
		os.mkdir(TEMPDIR)
		schema_name=f"{target_db}.{target_schema}"
		put_cmd=f"PUT 'file://{chunkfile}.gz' '@%{target_table_name}' SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE AUTO_COMPRESS=FALSE PARALLEL=24" 
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
		#out=os.popen(f'rm -rf {TEMPDIR}').read()
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
		sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","SPLIT_LOAD")
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
		os.remove(chunkfile + '.gz')
		try:
			os.rmdir(TEMPDIR)
		except Exception as e:
			pass
		if isinstance(conn3,snowflake.connector.connection.SnowflakeConnection):
			conn3.close()
	except  Exception as e:
		print(f"Inside exception at step_name {step_name} for bucket {bucket_num}")
		print(str(e))
		print(messg)


## Main Program start here
pgmVersion='1.0'
parser = argparse.ArgumentParser(description='Script used to push split and load to SF stg',
    epilog='Example: python DIY_split_load.py -e environment -r reqid -j jct ')
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
args=parser.parse_args()
env=args.env
reqid=args.reqid
jct=args.jct
flat_file=args.fileName
src_rows=args.srcRows
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
	sql+=f",target_db_name,target_schema,target_db_connection"
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
	step_name="Switching current directory"
	log_data(log_file,step_name,0)
	os.chdir(df_path)
	cmd=f"cat {datafile} | wc -l"
	step_name=f"Counting datafile {datafile}"
	log_data(log_file,step_name,0)
	#cmd=f"cat {datafile} | wc -l"
	#tot_lines=int(os.popen(cmd).read())
	#tot_lines=file_line_check()
	file_size=os.stat(datafile).st_size
	if file_size > 1073741824: 
		## > 1GB data file. wc -l will be costly. so switch to estimation
		tot_lines=file_line_check()
		if tot_lines == -1:
			raise Exception("Issue encountered while counting datafile")
	else:
		tot_lines=int(os.popen(cmd).read())
	log_data(log_file,f"{datafile} has {tot_lines} lines and it is of size {file_size}",0)
	if tot_lines == 0:
		log_data(log_file,f"{datafile} is zero byte file.. So nothing to process",0)
		sys.exit(0)
	chunk_size=int(configFile.Config.split_chunk_size) ## 512 MB
	parallel_thread_count=int(configFile.Config.split_parallel_count) ## 10
	if file_size > chunk_size:
		line_per_chunk=round((chunk_size*tot_lines)/file_size)
	else:
		line_per_chunk=tot_lines
	j=0
	line=''
	thread_array=[]
	step_name="Iteratng datafile"
	log_data(log_file,step_name,0)
	curr_pid=os.getpid()
	bid=datetime.now().strftime('%Y%m%d%H%M%S')
	#prcmd=f"ps -ef | grep {curr_pid}  | grep -v defunct | grep -v grep | wc -l"
	prcmd="ps -ef | grep -v defunct | grep -v grep | awk '{print $3}' | grep " + str(curr_pid) + "  | wc -l"
	main_thread=None
	with open(datafile,'r') as f_dat:
		line=f_dat.readline()
		written_line=line
		while line:
			chunkfile=f'chunk_{target_table_name}_{bid}_{curr_pid}_{j}'
			i=1
			j+=1
			c_dat=open(chunkfile,'w')
			while (i <= line_per_chunk or f'|"{batch_id}"' not in written_line) and line:
				c_dat.write(line)
				written_line=line
				line=f_dat.readline()
				i+=1
			c_dat.close()
			cnt=int(os.popen(prcmd).read())
			#cnt=cnt-1 ## ignore parent pid
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
	step_name="Completed Iteration"
	log_data(log_file,step_name,0)
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Error occurred at line number {linenu} while splitting stage data for upload at step - {step_name}")
	print (str(e))
	sys.exit(1)	

