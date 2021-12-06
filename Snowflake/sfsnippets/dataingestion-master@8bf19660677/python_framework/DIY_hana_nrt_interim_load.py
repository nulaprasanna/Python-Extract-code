#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_hana_nrt_interim_load.py                                                                                          #
# Script to Perform interim load for HANA NRT workflow                                                                  #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Mar-2020                Initial Version                                                 #
# 1.1           Manick          Sep-2020                Enable SF connection query tag                                  #
#########################################################################################################################
import argparse,pdb
import snowflake.connector
import os, re, sys
from datetime import datetime
from datetime import timedelta
import time
from ConfigObject import ConfigObject
from metadata_handler import *
import subprocess,pdb
import threading,calendar


def upload_for_hananrt(flat_file,batch_number,src_rows):
	global reqid,step_name,log_file,err_file,sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,tgt_table,env,jct,sf_query_tag
	curr_pid=os.getpid()
	conn = open_oracle_connection(env,None,True)
	if type(conn) is int:
		raise Exception (f"Unable to connect to repository DB for batch {batch_number}")
	sql=f"update DIY_HANA_NRT_BATCH_LIST set last_update_time=sysdate,run_status='I',attribute1={curr_pid} "
	sql+=f" where reqid={reqid} and run_status = 'U' and batch_number={batch_number}"
	step_name=f"Updating DIY_HANA_NRT_BATCH_LIST with sql {sql}"
	#log_data(log_file,step_name,0)
	execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,"commit",reqid,conn)
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
	conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,query_tag=sf_query_tag)
	step_name=f"Connecting to repository for batch_number {batch_number}"
	#log_data(log_file,step_name,0)
	sql=f"select count(1) cnt from {tgt_db_name}.{tgt_schema}.{tgt_table} where batch_number={batch_number}"
	#log_data(log_file,sql,0)
	cnt_obj=execute_df_qry(conn3,sql,reqid,conn)
	interim_stg_cnt=cnt_obj["cnt"][0]
	step_name=f"Uploaded {interim_stg_cnt} rows for {batch_number}"
	log_data(log_file,step_name,0)
	sql=f"update DIY_HANA_NRT_BATCH_LIST set tgt_rows={interim_stg_cnt},last_update_time=sysdate,run_status='A' "
	sql+=f" where reqid={reqid} and run_status = 'I' and batch_number={batch_number}"
	step_name=f"Updating DIY_HANA_NRT_BATCH_LIST with sql {sql}"
	#log_data(log_file,step_name,0)
	execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,"commit",reqid,conn)
	step_name=f"closing connections for batch number {batch_number}"
	log_data(log_file,step_name,0)
	conn3.close()
	conn.close()
def arch_files(jct):
	global step_name,job_group_id,env,logs_dir,sql,datafile_dest,flat_file,vBatch,batch_number
	step_name="Now compressing and archiving data file"
	log_data(log_file,step_name,0)
	processes = set()
	datafile_dest=datafile_dest.replace('jobgroup',job_group_id)
	datafile_dest=datafile_dest.replace('env',env.upper())
	compress_log=open(f"{logs_dir}/compress.log",'a')
	oscmd=f"gzip {flat_file} && mv {flat_file}.gz {datafile_dest}"
	proc=subprocess.Popen([oscmd],stdout=compress_log, stderr=compress_log, shell=True)
	pid=proc.pid
	pout=processes.add(proc)
	step_name=f"Compress file for batch_number {batch_number} submitted with pid {pid}"
	log_data(log_file,step_name,0)
	sql=f"update DIY_HANA_NRT_BATCH_LIST set run_status='C',end_time=sysdate "
	sql+=f" where reqid={reqid} and run_status = 'A' and batch_id= {vBatch} and batch_number={batch_number}"
	execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,"commit",reqid,conn)
	

pgmVersion='1.0'
parser = argparse.ArgumentParser(description='Script used to perform interim upload to SF',
    epilog='Example: python DIY_hana_nrt_interim_load.py -b batch_number')
parser.add_argument('-b','--batchNum',required=True,
        help='Batch Number whose data to be extracted')
args=parser.parse_args()
try:
	batch_number=int(args.batchNum)
	envType=os.environ['RUN_ENVIRONMENT']
	vBatch=os.environ['BATCH_ID']
	reqid=os.environ['REQID']
	run_id=os.environ['RUN_ID']
	sfEnv=os.environ['SF_ENV']
	jct=os.environ['JOB_STREAM_ID']
	sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","HANA_NRT_INTERIM_LOAD")
	launch_flag=False
	configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
	env=envType
	step_name=f"Beginning upload woker for batch {batch_number}"
	if env.lower() == 'prd':
		datafile_dest=configFile.Config.datafile_dest_prd
	elif 'DV' in env.upper():
		datafile_dest=configFile.Config.datafile_dest_dev
	elif 'TS' in env.upper():
		datafile_dest=configFile.Config.datafile_dest_stg
	else:
		raise Exception(f"Invalid data file archive destination")
	step_name="Connecting to Repos DB"
	conn = open_oracle_connection(envType,None,True)
	step_name="Querying logs dir"
	sql=f"select logs_dir from diy_master where reqid={reqid}"
	logs_dir_obj=execute_df_qry(conn, sql, reqid, conn)
	logs_dir=logs_dir_obj["logs_dir"][0]
	log_file=f"{logs_dir}/{reqid}_nrtload_{vBatch}_worker{run_id}.log"
	err_file=f"{logs_dir}/{reqid}_nrtload_{vBatch}_worker{run_id}.err"
	step_name="Running BTP Query for batch "
	log_data(log_file,step_name,0)
	sql=f"select src_rows,flat_file from DIY_HANA_NRT_BATCH_LIST where run_status='U' and reqid={reqid} and batch_id={vBatch}"
	sql+=f" and batch_number={batch_number}"
	btp_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(btp_obj) == 0:
		log_data(log_file,"Batch status ineligible for upload.. Hence not progressing further",0)
		sys.exit(0)
	src_rows=btp_obj["src_rows"][0]
	flat_file=btp_obj["flat_file"][0]
	sql=f"select sf_role,sf_warehouse,target_db_name,target_schema,target_table_name,job_group_id from diy_job_params where reqid={reqid} and job_stream_id='{jct}'"
	sf_conn_obj=execute_df_qry(conn, sql, reqid, conn)
	sf_user='EDW_DATALAKE_SVC'
	sf_wh=sf_conn_obj["sf_warehouse"][0]
	sf_role=sf_conn_obj["sf_role"][0]
	tgt_db_name=sf_conn_obj["target_db_name"][0]
	tgt_schema=sf_conn_obj["target_schema"][0]
	tgt_table=sf_conn_obj["target_table_name"][0]
	job_group_id=sf_conn_obj["job_group_id"][0]
	launch_flag=True
	upload_for_hananrt(flat_file,batch_number,src_rows)
	arch_files(jct)
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Exception occurred at line {linenu} step {step_name} for batch {batch_number} while uploading - {e}")
	if launch_flag:
		conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,query_tag=sf_query_tag)
		sql=f"delete from {tgt_db_name}.{tgt_schema}.{tgt_table} where batch_number={batch_number}"
		execute_qry(conn3,sql,reqid,conn)
		execute_qry(conn3,'commit',reqid,conn)
		sql=f"update DIY_HANA_NRT_BATCH_LIST set last_update_time=sysdate,run_status='U' "
		sql+=f" where reqid={reqid} and run_status = 'I' and batch_number={batch_number}"
		execute_qry(conn,sql,reqid,conn)
		execute_qry(conn,"commit",reqid,conn)
		conn3.close()
	sys.exit(7)
	
	