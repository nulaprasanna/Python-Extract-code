#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_soft_delete.py                                                                                                    #
# Script to perform soft deletes in Snowflake                                                                           #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Dec-2019                Initial Version                                                 #
# 1.1           Manick          Jan-2020                Have 500 as delta count since at source records gets deleted    #
# 1.2           Manick          Jan-2020                Change to_date to to_timestamp on bucket column                 #
# 1.3           Manick          Jan-2020                File having $ on it is causing problem  and add reqid           #
# 1.4           Manick          Jan-2020                If source count is 0 then stage count can be 0                  #
# 1.5           Manick          Jan-2020                Exclude object from ODS.OPS$scehma and improve excep handling   #
# 1.6           Manick          Jan-2020                Move TMPDIR location to NFS mount to avoid space shortage       #
# 1.7           Manick          Jan-2020                Change TD connection from teradata to teradatasql               #
# 1.8           Manick          Jan-2020                Handle null that is part of unique key                          #
# 1.9           Manick          Jan-2020                Change SF connection module on metadata_handler                 #
# 1.10          Manick          Jan-2020                Enhance split query for TD                                      #
# 1.11          Manick          Feb-2020                Issue with temp dir setting for put command                     #
# 1.12          Manick          Feb-2020                Dynamic wh resizing based on table size                         #
# 1.13          Manick          Feb-2020                Change nvl(0) to ::NUMBER to handle big numeric value           #
# 1.14          Manick          Feb-2020                check for 3 attempts if put command takes time to fnishi        #
# 1.15          Manick          Feb-2020                Use overwrite verb to overwrite stage files if exists           #
# 1.16          Manick          Feb-2020                Fine tune TD split logic for date time data type                #
# 1.17          Manick          Feb-2020                Enable soft deletes for Hana                                    #
# 1.18          Manick          Feb-2020                Fix with precision and scale on Number(38,5) for sf             #
# 1.19          Manick          Feb-2020                Bug fix with check_rebucket. Change ~ to . while splitting      #
# 1.20          Manick          Mar-2020                Check bucket data type before inserting max value               #
# 1.21          Manick          Mar-2020                Change . back to ~ as seperator as table has . on it            #
# 1.22          Manick          Mar-2020                Take filter into consideration while pulling source count       #
# 1.23          Manick          Mar-2020                If rowcount is passed reuse instead of pulling it               #
# 1.24          Manick          Mar-2020                If gap exists in bucket then increase it by one                 #
# 1.25          Manick          Mar-2020                Switch oracle db connection to LDAP                             #
# 1.26          Manick          Mar-2020                Consider numnulls incase of TD from stats while splitting       #
# 1.27          Manick          Mar-2020                Exclude casting decimals to Number                              #
# 1.28          Manick          May-2020                Implement QUery banding for Teradata                            #
# 1.29          Manick          May-2020                Switch data scale to data precision while casting decimal       #
# 1.30          Manick          May-2020                Change to threading process to have always 10 thread running    #
# 1.31          Manick          May-2020                Include HANANRT type for deletion                               #
# 1.32          Manick          Jun-2020                Include Snowflake for deletion                                  #
# 1.33          Manick          Jun-2020                Fix to_timestamp issue for date column in TD                    #
# 1.34          Manick          Jun-2020                Mark entire target tbl as soft del if source has 0 rows         #
# 1.35          Manick          Jun-2020                Make entry of user who is running soft del for audit purpose    #
# 1.36          Manick          Jul-2020                Convert Split logic to new Python module                        #
# 1.37          Manick          Sep-2020                Enable query tag                                                #
# 1.38          Manick          Sep-2020                Bug with split column when not available in DIY masetr          #
# 1.39          Manick          Oct-2020                Reuse Micro template for SD as well                             #
# 1.40          Manick          Oct-2020                Include filter while performing soft del if exists in JCT       #
# 1.41          Manick          Oct-2020                Enable lock row for access for TD to avoid read locks           #
# 1.42          Manick          Oct-2020                If timestamp has milliseconds adjustformat accordingly          #
# 1.43          Manick          Oct-2020                Bug fix with filtered all data ext count from SF                #
# 1.44          Manick          Oct-02020               Pass filter for rebucketting if exists in JCT                   #    
# 1.45          Manick          Nov-2020                Pull data from source and compare only if source buck > sf buck #
# 1.46          Manick          Nov-2020                Force pull source row count if sf has more to ignore current rec#
# 1.47          Manick          Dec-2020                Adjust source rows based on soft del bucket count at end        #
# 1.48          Manick          Dec-2020                Use to_timestamp instead of to_date for Oracle as well          #
#                                                       Include nullif condition while comparing blanks                 #
# 1.49          Manick          Jan-2021                Do further update on end time only if not "Nothing to do" type  #
# 1.50          Manick          Feb-2021                Dont count source count from bucket if source has 0 rows        #
# 1.51          Manick          Feb-2021                Bug fix with source query which has Float type and nullable     #
# 1.52          Manick          Mar-2021                Bug fix when table has # on it where put command is failing     #
# 1.53          Manick          Mar-2021                Switch to to_date to use index instead of timestamp for oracle  #
# 1.54          Manick          Apr-2021                Issue with soft delete on TD table that has blanks on unique key#
# 1.55          Manick          Apr-2021                Fix with Hana SC soft delete on date columns with format        #
# 1.56          Manick          Apr-2021                Stop iterating more buckets by delta count against del count    #
# 1.57          Manick          Apr-2021                Use Large warehouse only for > 1 B rows, > 500M med else small  #
# 1.58          Manick          Apr-2021                Move using large warehouse only for upd and based on size       #
# 1.59          Manick          Apr-2021                Logic to rebucket skewed data                                   #
# 1.60          Manick          Apr-2021                Use a smaller wh if del col is part of clustering key           #
# 1.61          Manick          May-2021                Use _dels concept to reduce cost and increase time              #
# 1.62          Manick          May-2021                Handle 390114 during stage creation if data ext runs long       #
# 1.63          Manick          Jun-2021                New version with additional comments for easy readability       #
#########################################################################################################################
import argparse
import snowflake.connector
import cx_Oracle
import teradatasql
import os, re, string, subprocess, sys, csv
from datetime import datetime
from datetime import timedelta
import time
import hvac
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from ConfigObject import ConfigObject
import faulthandler; faulthandler.enable()
import math
from metadata_handler import *
import pdb,random,string
import threading,calendar
import snowflake.connector
from splitter import diy_split
thread_errors = [] #global list

## Below method is obsolete and not in use.
def random_string(stringLength=6):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

## Below method is used to create dels tabls. Every key which exists in Snowflake but not in source will be logged here

def cr_dels_table(unique_key_cols_set,tgt_db_name,tgt_table,tgt_schema,conn3,reqid,conn):
	tgt_db=tgt_db_name.replace("BR","ETL")
	cr_tbl_sql=f"create or replace table {tgt_db}.STG.{tgt_table}_dels("
	for colname in unique_key_cols_set:
		sql=f"select decode(data_type,'TEXT','VARCHAR',data_type) dt,character_maximum_length len,numeric_precision prec,numeric_scale sca,datetime_precision dprec "
		sql+=f"from {tgt_db_name}.information_schema.columns where table_name='{tgt_table}' and table_schema='{tgt_schema}' and column_name='{colname}'"
		col_obj=execute_df_qry(conn3,sql,reqid,conn)
		if len(col_obj) == 0:
			 raise Exception (f"Metadata for column {colname} not found")
		dt=col_obj['dt'][0]
		leng=col_obj['len'][0]
		prec=col_obj['prec'][0]
		sca=col_obj['sca'][0]
		dprec=col_obj['dprec'][0]
		if dt == 'TIMESTAMP' and dprec is not None:
			dt_up=f"{dt}({dprec})"
		elif dt == 'NUMBER' and prec is not None:
			if sca is not None:
				dt_up=f"{dt}({prec},{sca})"
			else:
				dt_up=f"{dt}({prec})"
		elif dt == "VARCHAR":
			dt_up=f"{dt}({leng})"
		else:
			dt_up=dt
		cr_tbl_sql+=f" {colname} {dt_up},"
	cr_tbl_sql=cr_tbl_sql[:-1]
	cr_tbl_sql+=")"
	execute_qry(conn3, cr_tbl_sql, reqid, conn)
	return (f"{tgt_db}.STG.{tgt_table}_dels")

## Below method is used to populate the soft delete buckets table with range of values used for comparision

def pop_split_table(split_qry_dict,reqid,tbl_name,col_name,null_exists):
	global sql
	loop_cnt=0
	ins_tbl_name=tbl_name.replace('~','.')
	for key,value in split_qry_dict.items():
		loop_cnt+=1
		min_val=value.split('~')[0]
		max_val=value.split('~')[1]
		sql="insert into diy_soft_del_buckets (reqid,table_name,bucket_number,condition,min_value,max_value,column_name) values "
		sql+=f"({reqid},'{ins_tbl_name}',{loop_cnt},'between','{min_val}','{max_val}','{col_name}')"
		execute_qry(conn, sql, reqid, conn)
		execute_qry(conn,"commit",reqid,conn)
	if null_exists:
		loop_cnt+=1
		sql="insert into diy_soft_del_buckets (reqid,table_name,bucket_number,condition,min_value,max_value,column_name) values "
		sql+=f"({reqid},'{ins_tbl_name}',{loop_cnt},'is null',NULL,NULL,'{col_name}')"
		execute_qry(conn, sql, reqid, conn)
		execute_qry(conn,"commit",reqid,conn)

## Below method is used to compare the keys between source and target and pull associated keys incase of mismatch.

def perform_soft_delete(bucket_num,filter):
	try:
		global sql,log_file,parallel_clause,src_db_name,src_schema,src_table,split_col_name,reqid,tgt_db_name,connstr2,db_type,unique_key_cols_set,sf_user,sf_wh,sf_role,TEMPDIR,sf_wh_4_upd
		global tgt_schema,tgt_table,env,job_group_id,tgt_uk_qry,src_uk_qry_4_del,src_uk_qry,err_file,soft_del_cnt,srcdbParams,srcPassword,sfEnv,sfDict,logs_dir,col_data_type,tgt_db_name,tgt_schema
		global sf_data_type,sf_query_tag,where_clause
		sql_list={}
		currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
		if currOsUser == "":
			try:
				currOsUser=os.getlogin()
			except Exception as e:
				currOsUser=c_getuser()
				pass
		log_data(log_file,f"Setting tempdir for bucket {bucket_num} to {TEMPDIR}",0)
		step_name=f"Establishing db connections for bucket {bucket_num}"
		log_data(log_file,f"At step {step_name}",0)
		conn = open_oracle_connection(env,None,True)
		conn3=''
		if type(conn) is int:
			raise Exception ("Unable to connect to repository DB")
		## Establish connection to source system
		if db_type == "ORACLE":
			conn2 = cx_Oracle.connect(srcdbParams['SOURCE_LOGIN'],srcPassword, srcdbParams['SOURCE_SERVICE_NAME'],encoding="UTF-16", nencoding="UTF-16")
		elif db_type=="TERADATA":
			#conn2 = udaexec.connect(method="odbc", DSN="tdprodetloffload")
			conn2 = teradatasql.connect(host=srcdbParams['SOURCE_HOST'],user=srcdbParams['SOURCE_LOGIN'], password=srcPassword)
			cursor2 = conn2.cursor()
			cursor2.execute(f"set query_band='Module=SoftDel;user={currOsUser};' for Session")
			cursor2.close()
		elif db_type in ("HANA","HANANRT"):
			hostName=srcdbParams['SOURCE_HOST'].split(';')[0].split(':')[0]
			if 'SOURCE_ADDITIONAL_PARAMS' in srcdbParams:
				conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword,srcdbParams['SOURCE_ADDITIONAL_PARAMS'])
			else:
				conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword)
		elif db_type in ('SNOWFLAKE'):
			conn2=snowflake.connector.connect(user=srcdbParams['SOURCE_LOGIN'],password=srcPassword,account=srcdbParams['ACCOUNT'],warehouse=srcdbParams['WAREHOUSE'])
		## Establish connection to Snowflake system
		conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,query_tag=sf_query_tag)
		step_name=f"Performing soft delete for bucket {bucket_num}"
		log_data(log_file,f"At step {step_name}",0)
		file_path=f"/apps/edwsfdata/{env}/{job_group_id}/data/inbox/"
		arc_file_path=f"/apps/edwsfdata_nfs_prd/{env}/{job_group_id}/data/archive"
		out=os.popen(f'mkdir -p /apps/edwsfdata_nfs_prd/{env}/{job_group_id}/data/archive').read()
		file_name=f"{src_db_name}_{src_schema}_{src_table}"
		step_name=f"Pulling source bucket row count for bucket {bucket_num}"
		log_data(log_file,f"At step {step_name}",0)
		log_data(log_file,f" Pulling source count for bucket number {bucket_num}",0)
		if len(filter)>0:
			if db_type == "HANA":
				sql_list[bucket_num]=f'select {parallel_clause} {count_clause} from {src_schema}."{src_table}" a where {split_col_name} {filter}'
			elif db_type == "ORACLE" and 'to_timestamp' in filter:
				nf=filter.replace('to_timestamp','to_date')
				sql_list[bucket_num]=f'select {parallel_clause} {count_clause} from {src_schema}."{src_table}" a where {split_col_name} {nf}'
			else:
				sql_list[bucket_num]=f"select {parallel_clause} {count_clause} from {src_schema}.{src_table} a where {split_col_name} {filter}"
		else:
			if db_type == "HANA":
				sql_list[bucket_num]=f'select {parallel_clause} {count_clause} from {src_schema}."{src_table}" a'
			else:
				sql_list[bucket_num]=f"select {parallel_clause} {count_clause} from {src_schema}.{src_table} a"
		if where_clause is not None and len(where_clause) > 0:
			if len(filter)>0:
				sql_list[bucket_num] += f" AND {where_clause}"
			else:
				sql_list[bucket_num] += f" WHERE {where_clause}"
		if db_type == "TERADATA":
			sql_list[bucket_num] = f"LOCK ROW FOR ACCESS {sql_list[bucket_num]}"
		mm_count_obj=execute_df_qry(conn2, sql_list[bucket_num], reqid, conn)
		src_row_cnt=int(mm_count_obj["cnt"][0])
		step_name=f"Pulling sf bucket row count for bucket {bucket_num}"
		log_data(log_file,f"At step {step_name}",0)
		if len(filter)>0:
			if db_type == "TERADATA":
				if col_data_type == "CHAR" or col_data_type == "VARCHAR":
					sql_list[bucket_num]=f"select count(*) cnt from {tgt_db_name}.{tgt_schema}.{tgt_table} where collate({split_col_name},'en-ci') {filter} and nvl(edwsf_source_deleted_flag,'N')='N'"
				else:
					sql_list[bucket_num]=f"select count(*) cnt from {tgt_db_name}.{tgt_schema}.{tgt_table} where {split_col_name} {filter} and nvl(edwsf_source_deleted_flag,'N')='N'"
			else:
				sql_list[bucket_num]=f"select count(*) cnt from {tgt_db_name}.{tgt_schema}.{tgt_table} where {split_col_name} {filter} and nvl(edwsf_source_deleted_flag,'N')='N'"
				if col_data_type == "DATE" and db_type == "ORACLE":
					sql_list[bucket_num]=sql_list[bucket_num].replace('to_date','to_timestamp')
				
		else:
			sql_list[bucket_num]=f"select count(*) cnt from {tgt_db_name}.{tgt_schema}.{tgt_table} where nvl(edwsf_source_deleted_flag,'N')='N'"
		if where_clause is not None and len(where_clause) > 0:
				sql_list[bucket_num] += f" AND {where_clause}"
		sf_mm_count_obj=execute_df_qry(conn3, sql_list[bucket_num], reqid, conn)
		sf_row_cnt=sf_mm_count_obj["cnt"][0]
		upd_source_rows(reqid,bucket_num,src_row_cnt,sf_row_cnt,conn)
		## Compare source and snowflake count and if Snowflake has more records than source then pull the keys in CSV format from source and upload to Snowflake stage for comparision.
		if src_row_cnt < sf_row_cnt:
			log_data(log_file,f"Since row count mis match exists for bucket number {bucket_num} performing soft del by pulling source count",0)
			csv_file_dest = f"{file_path}{file_name}_{bucket_num}_{reqid}.csv"
			csv_file_dest = csv_file_dest.replace('#','') ## As put command is failing if file name has # on it
			#csv_file_data = f"{file_path}{file_name}_{bucket_num}_d.csv"
			#csv_file_header = f"{file_path}{file_name}_{bucket_num}_h.csv"
			if len(filter)>0:
				sql_list[bucket_num]=src_uk_qry + f" where {split_col_name} {filter}"
				if where_clause is not None and len(where_clause) > 0:
					sql_list[bucket_num] += f" and {where_clause} "
			else:
				sql_list[bucket_num]=src_uk_qry 
				if where_clause is not None and len(where_clause) > 0:
					sql_list[bucket_num] += f" where {where_clause} "
			if db_type == "ORACLE" or db_type == "HANA":
				output_file = open(csv_file_dest,'w') 
				output = csv.writer(output_file, dialect='excel',delimiter='|',quotechar='"',quoting=csv.QUOTE_ALL)
				step_name=f"Generating CSV for bucket {bucket_num}"
				log_data(log_file,f"At step {step_name}",0)
				cursor2=conn2.cursor()
				cursor2.arraysize=5000
				if db_type == 'ORACLE':
					if 'to_timestamp' in sql_list[bucket_num]:
						sql_list[bucket_num]=sql_list[bucket_num].replace('to_timestamp','to_date')
				cursor2.execute(sql_list[bucket_num])
				if print_header: # add column headers if requested
						cols = []
						for col in cursor2.description:
								cols.append(col[0])
						output.writerow(cols)
				for row_data in cursor2: # add table rows
						output.writerow(row_data)
				output_file.close()
			elif db_type=="TERADATA":
				step_name=f"Generating CSV through tbuild for bucket {bucket_num}"
				log_data(log_file,f"At step {step_name}",0)
				scr_dir="/apps/edwsfdata/python/scripts"
				template="Tbuild_template_Micro.txt"
				tdsql_list={}
				with open(f"{scr_dir}/{template}","r") as tf, open(f"{logs_dir}/tbuild_param_{bucket_num}.txt","w") as pf:
					line=tf.read()
					pf.write(line.replace("TBLNAME",src_table).replace("TFNAME",csv_file_dest).replace("QBAND",f"Module=SoftDel;user={currOsUser};").replace("SOURCEDB",srcdbParams['SOURCE_HOST']).replace("TUSER",srcdbParams['SOURCE_LOGIN']).replace("TPASSWORD",srcPassword).replace("TDELIMITER",'|'))
				with open (f"{logs_dir}/tbuild_param_{bucket_num}.txt","a") as pf:
					tdsql_list[bucket_num]=sql_list[bucket_num].replace("'","''")
					pf.write(f",SelectStmt     = 'LOCK ROW FOR ACCESS {tdsql_list[bucket_num]}'")
					pf.write("\n")
				step_name=f"Invoking tbuild for bucket {bucket_num}"
				log_data(log_file,f"At step {step_name}",0)
				cmnd=subprocess.Popen(f"tbuild -f {scr_dir}/jobdef.txt -v {logs_dir}/tbuild_param_{bucket_num}.txt -j {tgt_table}_{bucket_num}",stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
				(output, error) = cmnd.communicate()
				Err=error.decode('ascii')
				Out=output.decode('ascii')
				if Err is not None and len(Err)>0:
					log_data(err_file,f"Exception encountered during tbuild for bucket {bucket_num} \n" + Err,1)
					raise Exception(Err)
				else:
					log_data(log_file,f"Completed tbuild for bucket {bucket_num}",0)
					with open(f"{logs_dir}/tbuild_log_{bucket_num}","w") as lf:
						lf.write(Out + "\n")
				#step_name=f"Appending header for bucket {bucket_num}"
				#log_data(log_file,f"At step {step_name}",0)
				#catcmd=f"cat {logs_dir}/header.txt {csv_file_data} > {csv_file_dest}"  
				#out=os.popen(catcmd).read().strip("\n")
				#out2=os.popen(f"rm {csv_file_data}").read().strip("\n")

			step_name=f"gzipping csv file at source for bucket {bucket_num}"
			log_data(log_file,f"At step {step_name}",0)
			rc=os.system(f"gzip -f '{csv_file_dest}'")
			if rc !=0:
				err_msg=f"Exception encountered while compressing {csv_file_dest} to SF Stage for bucket {bucket_num}"
				raise Exception(err_msg)
			step_name=f"Creating stage for bucket {bucket_num}"
			log_data(log_file,f"At step {step_name}",0)
			sql_list[bucket_num]=f"create stage if not exists DEMO_DB.PUBLIC.soft_del_stage_{reqid}_{bucket_num} file_format = EDW_UTIL_DB.FILE_FORMATS.CSV_PIPE_WITHOUT_HEADER_NO_UTF8_SD" ## 1.54 change
			try:
				rc=execute_qry(conn3, sql_list[bucket_num], reqid, conn)
			except Exception as e:
				if '390114' in str(e):
					log_data(log_file,f"Connection timed out At step {step_name} for bucket {bucket_num}",0)
					conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,query_tag=sf_query_tag)
					rc=execute_qry(conn3, sql_list[bucket_num], reqid, conn)
			if rc == -1:
				raise Exception("Cannot create stage. Please check and retry")
			put_cmd=f"PUT 'file://{csv_file_dest}.gz' '@demo_db.public.soft_del_stage_{reqid}_{bucket_num}/{tgt_table}_{bucket_num}' SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE AUTO_COMPRESS=FALSE PARALLEL=24" ## replace this with actual stage with proper format 
			step_name=f"Uploading CSV to Snowflake stage for bucket {bucket_num}"
			log_data(log_file,f"At step {step_name}",0)
			#rc=os.system(f'snowsql -a {sfEnv} -u edw_datalake_svc -q "{put_cmd}" 1> {file_path}{file_name}_snowsql.log 2> {file_path}{file_name}_snowsql.out')
			#out2=os.popen(f'ls -rt {TEMPDIR}/TMP* | tail -1').read().strip('\n').split(':')[0]
			#if len(out2) > 0:
			#	last_put_cmd_time=os.stat(f"{out2}").st_mtime
			#	curr_time=calendar.timegm(time.gmtime())
			#	if (curr_time - last_put_cmd_time) < 30:
			#		#out3=os.popen(f'ls -rt {logs_dir}/{reqid}/now_sleeping | tail -1').read().strip('\n').split(':')[0]
			#		if os.path.exists(logs_dir + "/" + reqid + "/now_sleeping"):
			#			out4=os.popen(f'cat {logs_dir}/{reqid}/now_sleeping').read().strip('\n').split(':')[0]
			#			sleep_time=int(out4)+10
			#			if sleep_time > 120:
			#				sleep_time=10
			#			out5=os.popen(f' echo {sleep_time} > {logs_dir}/{reqid}/now_sleeping').read().strip('\n').split(':')[0]
			#		else:
			#			out4=os.popen(f' echo 30 > {logs_dir}/{reqid}/now_sleeping').read().strip('\n').split(':')[0]
			#			sleep_time=10
			#		step_name=f"Sleeping {sleep_time} seconds for bucket {bucket_num}"
			#		log_data(log_file,f"At step {step_name}",0)
			#		time.sleep(sleep_time) ## sleep for 30 seconds since last put invoked less than 30 seconds
			#rc=execute_qry(conn3, put_cmd, reqid, conn)
			run_cmd=f"/apps/edwsfdata/python/scripts/DIY_perform_put.py -e '{sfEnv}' -u '{sf_user}' -w '{sf_wh}' -r '{sf_role}' -p '{put_cmd}' -i {reqid} -t '{TEMPDIR}' -b {bucket_num}"
			cmnd=subprocess.Popen(run_cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
			(output, error) = cmnd.communicate()
			Err=error.decode('ascii')
			Out=output.decode('ascii')
			#out=os.popen(f'rm -rf {TEMPDIR}').read()
			if (Err is not None and len(Err)>0) or (Out.strip('\n') != '1'):
				err_msg=f"Exception encountered while loading {csv_file_dest}.gz to SF Stage. Err is {Err} and Out is {Out}"
				raise Exception(f"err_msg. Exeception is {Err}")
			time.sleep(15)## sleep 15 seconds before checking put data
			step_name=f"Checking staged data"
			log_data(log_file,f"At step {step_name}",0)
			attempts=0
			succ=True
			while succ:
				try:
					sql_list[bucket_num]=f"select count(1) cnt from '@demo_db.public.soft_del_stage_{reqid}_{bucket_num}/{tgt_table}_{bucket_num}'"
					sf_stg_cnt_obj=execute_df_qry(conn3, sql_list[bucket_num], reqid, conn)
					sf_stg_cnt=sf_stg_cnt_obj["cnt"][0]
					succ=False
				except Exception as e:
					attempts+=1
					messg=f"Attempting to query stage data for bucket {bucket_num} on attempt {attempts} failed with exception - {e}"
					log_data(log_file,messg,0)
					time.sleep(10)
					if attempts == 3:
						succ=False
						raise Exception (f"Checking staged data failed even after 3 attempts.. Hence giving up for bucket {bucket_num}")
					pass
			#if (sf_stg_cnt == 0 and src_row_cnt > 0) or (sf_stg_cnt/src_row_cnt < 0.75):
			if (sf_stg_cnt == 0 and src_row_cnt > 0) or (src_row_cnt >0 and sf_stg_cnt/src_row_cnt < 0.75):
				raise Exception(f"Wrong number of records {sf_stg_cnt} in stage @demo_db.public.soft_del_stage_{reqid}_{bucket_num}/{tgt_table}_{bucket_num}")
			step_name=f"Running comparision and marking soft delete for bucket {bucket_num}"
			log_data(log_file,f"At step {step_name}",0)
			#sql_list[bucket_num]=f"select count(1) cnt from ( {tgt_uk_qry} from {tgt_db_name}.{tgt_schema}.{tgt_table} "
			## Insert the keys which doesnt present in source into dels table .
			sql_list[bucket_num]=f"insert into {dels_tbl} select * from ( {tgt_uk_qry} from {tgt_db_name}.{tgt_schema}.{tgt_table} "
			if len(filter)>0:
				if db_type == "TERADATA":
					if col_data_type == "CHAR" or col_data_type == "VARCHAR":
						sql_list[bucket_num]+=f" where collate({split_col_name},'en-ci') {filter} and nvl(edwsf_source_deleted_flag,'N')='N'"
					else:
						sql_list[bucket_num]+=f" where {split_col_name} {filter} and nvl(edwsf_source_deleted_flag,'N')='N'"
				else:
					sql_list[bucket_num]+=f" where {split_col_name} {filter} and nvl(edwsf_source_deleted_flag,'N')='N'"
			else:
				sql_list[bucket_num]+=f" where nvl(edwsf_source_deleted_flag,'N')='N'"
			if where_clause is not None and len(where_clause) > 0:
					sql_list[bucket_num] += f" and {where_clause} "
			sql_list[bucket_num]+=" minus "
			sql_list[bucket_num]+=f" {stg_uk_qry} from '@demo_db.public.soft_del_stage_{reqid}_{bucket_num}/{tgt_table}_{bucket_num}')"
			ins_cnt_obj=execute_df_qry(conn3,sql_list[bucket_num], reqid, conn)
			sf_delta_cnt=ins_cnt_obj["number of rows inserted"][0]
			#sql_list[bucket_num]=f"select count(1) cnt from {dels_tbl}"
			#sf_cnt_diff_obj=execute_df_qry(conn3, sql_list[bucket_num], reqid, conn)	
			#sf_delta_cnt=sf_cnt_diff_obj["cnt"][0]
			#if sf_delta_cnt == src_row_cnt:
			#	raise Exception(f"something wrong for {tgt_table}_{bucket_num}. Entire record cannot be soft deleted. Please check")
			upd_sf_del_cnt(reqid,bucket_num,sf_delta_cnt,conn)
			soft_del_cnt+=sf_delta_cnt
			### create table to log data
			#if sf_delta_cnt > 0:
			#	tgt_col_list=src_uk_qry_4_del.replace("select ","")
			#	log_data(log_file,f"Performing soft delete for {tgt_table}_{bucket_num}",0)
			#	step_name=f"Performing soft delete for bucket {bucket_num}"
			#	log_data(log_file,f"At step {step_name}",0)
			#	rc=execute_qry(conn3, f'use warehouse {sf_wh_4_upd}', reqid, conn)
			#	sql_list[bucket_num]=f"update {tgt_db_name}.{tgt_schema}.{tgt_table} set edwsf_source_deleted_flag='Y' where ({tgt_col_list}) in ( "
			#	sql_list[bucket_num]+=f" {tgt_uk_qry} from {tgt_db_name}.{tgt_schema}.{tgt_table} "
			#	if len(filter)>0:
			#		if db_type == "TERADATA":
			#			if col_data_type == "CHAR" or col_data_type == "VARCHAR":
			#				sql_list[bucket_num]+=f" where collate({split_col_name},'en-ci') {filter} and nvl(edwsf_source_deleted_flag,'N')='N'"
			#			else:
			#				sql_list[bucket_num]+=f" where {split_col_name} {filter} and nvl(edwsf_source_deleted_flag,'N')='N'"
			#		else:
			#			sql_list[bucket_num]+=f" where {split_col_name} {filter} and nvl(edwsf_source_deleted_flag,'N')='N'"
			#	else:
			#		sql_list[bucket_num]+=f" where nvl(edwsf_source_deleted_flag,'N')='N'"
			#	if where_clause is not None and len(where_clause) > 0:
			#		sql_list[bucket_num] += f" and {where_clause} "
			#	sql_list[bucket_num]+=" minus "
			#	sql_list[bucket_num]+=f" {stg_uk_qry} from '@demo_db.public.soft_del_stage_{reqid}_{bucket_num}/{tgt_table}_{bucket_num}')"
			#	sf_cnt_diff_obj=execute_df_qry(conn3, sql_list[bucket_num], reqid, conn)	
			#else:
			#	log_data(log_file,f"Nothing to delete for bucket {bucket_num}",0)
			#rc=execute_qry(conn3, f'use warehouse {sf_wh}', reqid, conn)
			sql_list[bucket_num]=f"rm '@demo_db.public.soft_del_stage_{reqid}_{bucket_num}/{tgt_table}_{bucket_num}'"
			rc=execute_qry(conn3, sql_list[bucket_num], reqid, conn)
			sql_list[bucket_num]=f"drop stage DEMO_DB.PUBLIC.soft_del_stage_{reqid}_{bucket_num}"
			rc=execute_qry(conn3, sql_list[bucket_num], reqid, conn)
			## archive csv file
			zipcmd=f"mv {csv_file_dest}.gz {arc_file_path}/ 2> /dev/null 1> /dev/null &"
			zipout=os.popen(zipcmd).read().strip("\n")

		else:
			log_data(log_file,f'source count matches for bucket {bucket_num}',0)
		## end of intndation for row_count mis match
		#upd_soft_del_cnt(reqid,soft_del_cnt,del_seq,conn)
	except Exception as e:
		log_data(err_file,f"Failure at step {step_name}. Please find below exception",1)
		log_data(err_file,str(e),1)
		log_data(err_file,sql_list[bucket_num],1)
		#thread_errors.append([repr(e), threading.current_thread.name])
		thread_errors.append([bucket_num,str(e)])
		raise
	finally:
		conn.close()
		conn2.close()
		conn3.close()

## Below method is used to split the table into multiple parititions which will be populated into soft_del_bucket table for comparision

def split_table(db_type,tbl_name,num_buckets,conn2,reqid,split_col_name,buck_filter):
	global sql,src_cnt,src_db_name,conn
	srctable=tbl_name.split("~")[2]
	srcschema=tbl_name.split("~")[1]
	split_qry_dict={}
	null_exists=False
	if len(split_col_name) == 0:
		split_col_name=None
	splitQrydict=diy_split(conn2,srcschema,srctable,num_buckets,reqid,conn,buck_filter,src_cnt,'FULL',split_col_name,db_type,src_db_name,'SOFTDEL')
	loopcnt=0
	if type(splitQrydict) == list:
		if len(splitQrydict) > 0:
			for item in splitQrydict:
				srcIndcol=item.split('~')[0]
				minValue=item.split('~')[1]
				maxValue=item.split('~')[2]
				split_qry_dict[loopcnt]=f"{minValue}~{maxValue}"
				loopcnt += 1
		else:
			raise Exception('Error occurred while creating split buckets')
	else:
		raise Exception(f'Error occurred while creating split buckets. Exception is {splitQrydict}')
	pop_split_table(split_qry_dict,reqid,tbl_name,srcIndcol,null_exists)

## Below method is used to update soft del falg based on keys that exists in dels table.

def perform_sof_del(conn3,reqid,conn):
	log_data(log_file,f"Performing soft delete",0)
	rc=execute_qry(conn3, f'use warehouse {sf_wh_4_upd}', reqid, conn)
	sql=f"update {tgt_db_name}.{tgt_schema}.{tgt_table} a from {dels_tbl} b set edwsf_source_deleted_flag='Y' where "
	for obj in uk_nvl_set:
		col_name=obj.split('~')[0]
		dt_type=obj.split('~')[1]
		nu_flag=obj.split('~')[2]
		if "CHAR" in dt_type:
			if nu_flag == "Y":
				sql+=f"nvl(a.{col_name},'0') = nvl(b.{col_name},'0')"
			else:
				sql+=f"a.{col_name} = b.{col_name}"
		elif "DATE" in dt_type:
			if nu_flag == "Y":
				sql+=f"nvl(a.{col_name},current_date) = nvl(b.{col_name},current_date)"
			else:
				sql+=f"a.{col_name} = b.{col_name}"
		elif "TIMESTAMP" in dt_type:
			if nu_flag == "Y":
				sql+=f"nvl(a.{col_name},current_timestamp) = nvl(b.{col_name},current_timestamp)"
			else:
				sql+=f"a.{col_name} = b.{col_name}"
		else:
			if nu_flag == "Y":
				sql+=f"nvl(a.{col_name},0) = nvl(b.{col_name},0)"
			else:
				sql+=f"a.{col_name} = b.{col_name}"
		sql+= " and " 
	sql = sql[:-4]
	res=execute_df_qry(conn3,sql,reqid,conn)
	soft_del_cnt=res["number of rows updated"][0]
	upd_soft_del_cnt(reqid,soft_del_cnt,del_seq,conn)
	log_data(log_file,f"Succesful completion of soft delete",0)

## Below method is used to upadate buckets table with delete count
	
def upd_sf_del_cnt(reqid,bucket_num,sf_delta_cnt,conn):
	global sql
	sql=f"update diy_soft_del_buckets set softdel_cnt={sf_delta_cnt} where reqid={reqid} and bucket_number={bucket_num}"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)

## Below method is used to update buckets table with source count and Snowflake count

def upd_source_rows(reqid,bucket_num,src_row_cnt,sf_row_cnt,conn):
	global sql
	sql=f"update diy_soft_del_buckets set source_count={src_row_cnt},sf_count={sf_row_cnt} where reqid={reqid} and bucket_number={bucket_num}"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)

## Below method is used to update soft delete table with cummulative delete count

def upd_soft_del_cnt(reqid,soft_del_cnt,del_seq,conn):
	global sql
	sql=f"update diy_soft_delete set  soft_del_count={soft_del_cnt},last_update_time=sysdate where reqid={reqid} and delete_id={del_seq}"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)

## Below method is used to update end time in soft delete table.
## Also on highly transactional tables we found source count changes between the time at which we started executing the process
## and the time at which it completes. As a result, sum of all buckets is checked and updated on soft delete table if it seems to be the case.
## This is done to reflect upto date value in audit dashboard as it picks values from soft delete table

def upd_end_time(reqid,status,msg,del_seq):
	global sql,log_file
	sql=f"update diy_soft_delete set  result='{status}',last_update_time=sysdate,end_time=sysdate,err_msg='{msg}' where reqid={reqid} and delete_id={del_seq}"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)
	if status == 'Success' and msg != 'Nothing to do':
		sql=f"select source_count sc,target_count tc,nvl(soft_del_count,0) sdc from diy_soft_delete where reqid={reqid} and delete_id={del_seq}"
		orig_cnt_obj=execute_df_qry(conn, sql, reqid, conn)
		orig_src_count=orig_cnt_obj["sc"][0]
		orig_tgt_count=orig_cnt_obj["tc"][0]
		sdc=orig_cnt_obj["sdc"][0]
		tgt_count=orig_tgt_count-sdc
		if tgt_count > orig_src_count:
			log_data(log_file,f"Below is the original count from job run. \n src_count~tgt_count={orig_src_count}~{tgt_count} ",0)
			if orig_src_count > 0:
				sql=f"select sum(source_count) sc from diy_soft_del_buckets where reqid={reqid}"
				src_cnt_obj=execute_df_qry(conn, sql, reqid, conn)
				rev_src_cnt=src_cnt_obj["sc"][0]
				if rev_src_cnt > orig_src_count:
					log_data(log_file,"Adjusting source_count in soft_del table based on soft del bucket values now to update audit dashboard",0)
					sql=f"update diy_soft_delete set source_count={rev_src_cnt}  where reqid={reqid} and delete_id={del_seq}"
					execute_qry(conn, sql, reqid, conn)
					execute_qry(conn,"commit",reqid,conn)

## Below method is used to check if existing buckets are still valid or a rebucket is required due to change in source data

def check_rebucket(reqid,conn,num_buckets,tbl_name,db_type,conn2,split_col_name):
	global sql,log_file,buck_filter
	sql=f"select count(1) cnt from diy_soft_del_buckets where reqid={reqid}"
	buck_obj=execute_df_qry(conn, sql, reqid, conn)
	buck_cnt=buck_obj["cnt"][0]
	sql=f"select count(*) cnt from"
	sql+=f" (select a.reqid,a.source_table,a.source_count,count(*) cnt,max(b.source_count) mx from diy_soft_delete a,diy_soft_del_buckets b"
	sql+=f" where a.reqid={reqid} and a.reqid=b.reqid and b.condition <> 'ALL_DATA' group by a.reqid,a.source_table,a.source_count having max(b.source_count) > (a.source_count/count(*))*4)"
	skew_obj=execute_df_qry(conn, sql, reqid, conn)
	skew_cnt=skew_obj["cnt"][0]
	if buck_cnt < num_buckets or skew_cnt > 0:
		print("Existing bucket is not valid anymore. Hence rebucketing data")
		log_data(log_file,"Now deleting existing bucket values",0)
		sql=f"delete from diy_soft_del_buckets where reqid={reqid}"
		execute_qry(conn, sql, reqid, conn)
		execute_qry(conn,"commit",reqid,conn)
		src_db_name=tbl_name.split('~')[0]
		src_schema=tbl_name.split('~')[1]
		src_tbl=tbl_name.split('~')[2]
		split_table(db_type,tbl_name,num_buckets,conn2,reqid,split_col_name,buck_filter)

## Below method is used to populate current runs soft delete buckets from last runs buckets for same table.	

def pop_diy_soft_del_buck_from_bucket(split_buck_reqid,conn,reqid,split_col_name,tbl_name,db_type,conn2):
	global sql,num_buckets,env
	sql="insert into diy_soft_del_buckets (reqid,table_name,bucket_number,condition,min_value,max_value,column_name,source_count) "
	sql+=f"select {reqid},table_name,bucket_number,condition,min_value,max_value,column_name,source_count from diy_soft_del_buckets where reqid={split_buck_reqid} order by bucket_number"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)
	sql=f"select count(*) cnt from diy_soft_del_buckets where reqid={reqid} and condition not in ('between','is null')"
	oper_obj=execute_df_qry(conn, sql, reqid, conn)
	oper_cnt=oper_obj["cnt"][0]
	if oper_cnt < 2:
		db=tbl_name.split('~')[0]
		schema=tbl_name.split('~')[1]
		tbl=tbl_name.split('~')[2]
		sql=f"select data_type from eds_data_catalog.edw_table_column where table_name='{tbl}' and db_instance_name='{db}'"
		sql+=f" and db_schema_name='{schema}' and column_name='{split_col_name}' and environment_name='{env}'"
		col_data_type_obj=execute_df_qry(conn, sql, reqid, conn)
		col_data_type=col_data_type_obj["data_type"][0]
		sql=f"select max(bucket_number) bn from diy_soft_del_buckets where reqid={reqid}"
		max_buck_obj=execute_df_qry(conn, sql, reqid, conn)
		max_split_id=max_buck_obj["bn"][0]
		sql=f"select min_value min_val from diy_soft_del_buckets where reqid={reqid} and bucket_number=1"
		max_buck_obj=execute_df_qry(conn, sql, reqid, conn)
		min_split_val=max_buck_obj["min_val"][0]
		sql=f"select max_value max_val from diy_soft_del_buckets where reqid={reqid} and bucket_number={max_split_id}"
		max_buck_obj=execute_df_qry(conn, sql, reqid, conn)
		max_split_val=max_buck_obj["max_val"][0]
		ins_tbl_name=tbl_name.replace('~','.')
		sql="insert into diy_soft_del_buckets (reqid,table_name,bucket_number,condition,min_value,max_value,column_name) values "
		sql+=f"({reqid},'{ins_tbl_name}',{max_split_id}+1,'>', '{max_split_val}',NULL,'{split_col_name}')"
		execute_qry(conn, sql, reqid, conn)
		num_buckets+=1
		sql="insert into diy_soft_del_buckets (reqid,table_name,bucket_number,condition,min_value,max_value,column_name) values "
		sql+=f"({reqid},'{ins_tbl_name}',{max_split_id}+2,'<', '{min_split_val}',NULL,'{split_col_name}')"
		execute_qry(conn, sql, reqid, conn)
		execute_qry(conn,"commit",reqid,conn)
		num_buckets+=1
	check_rebucket(reqid,conn,num_buckets,tbl_name,db_type,conn2,split_col_name)

## Below method is used to populate buckets with min and max ranges for future records which would not have present when originally buckets were created

def pop_diy_soft_del_buckets(split_reqid,conn,reqid,num_buckets,split_col_name,tbl_name,db_type,conn2):
	global sql
	ins_tbl_name=tbl_name.replace('~','.')
	sql="insert into diy_soft_del_buckets (reqid,table_name,bucket_number,condition,min_value,max_value,column_name,source_count) "
	sql+=f"select {reqid},'{ins_tbl_name}',split_id+1,'between' ,min_value,max_value,'{split_col_name}',row_count from diy_splits where reqid={split_reqid} order by split_id"
	execute_qry(conn, sql, reqid, conn)
	sql=f"select max(split_id) si from diy_splits where reqid={split_reqid}"
	max_buck_obj=execute_df_qry(conn, sql, reqid, conn)
	max_split_id=max_buck_obj["si"][0]
	sql=f"select min_value min_val from diy_splits where reqid={split_reqid} and split_id=0"
	max_buck_obj=execute_df_qry(conn, sql, reqid, conn)
	min_split_val=max_buck_obj["min_val"][0]
	sql=f"select max_value max_val from diy_splits where reqid={split_reqid} and split_id={max_split_id}"
	max_buck_obj=execute_df_qry(conn, sql, reqid, conn)
	max_split_val=max_buck_obj["max_val"][0]
	sql="insert into diy_soft_del_buckets (reqid,table_name,bucket_number,condition,min_value,max_value,column_name) values "
	sql+=f"({reqid},'{ins_tbl_name}',{max_split_id}+2,'>', '{max_split_val}',NULL,'{split_col_name}')"
	execute_qry(conn, sql, reqid, conn)
	sql="insert into diy_soft_del_buckets (reqid,table_name,bucket_number,condition,min_value,max_value,column_name) values "
	sql+=f"({reqid},'{ins_tbl_name}',{max_split_id}+3,'<', '{min_split_val}',NULL,'{split_col_name}')"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)
	check_rebucket(reqid,conn,num_buckets,tbl_name,db_type,conn2,split_col_name)

#
## Main program starts here
#
pgmVersion='1.63'
parser = argparse.ArgumentParser(description='Script used to perform soft delete',
    epilog='Example: python DIY_soft_delete.py -j jct -e environment -l log_dir -r reqid -c tablerowcount -f force_split')
parser.add_argument('-j', '--jct',required=True,
    help='SRC2STG JCT for which soft delete needs to be performed ')
parser.add_argument('-e','--env',required=True,
        help='Environment for which source delete needs to be performed (DV3/TS3/PRD)')
parser.add_argument('-l','--dir',required=True,
        help='Directory for storing log files')
parser.add_argument('-r','--reqid',
        help='Reqid if you want to use existing one (optional)')
parser.add_argument('-c','--rowcount',
        help='table rowcount if you want to pass (optional)')
parser.add_argument('-f','--fsplit',
        help='Use this option if you to force soft delete bucket creation (optional)')
args=parser.parse_args()
jct=args.jct
env=args.env
logs_dir=args.dir
reqid=args.reqid
aud_num_rows=args.rowcount
fsplit=args.fsplit
if aud_num_rows is not None:
	aud_num_rows=int(aud_num_rows)
ftime=datetime.now().strftime('%Y%m%d%H%M%S')
if reqid is None:
	reqid=str(datetime.now().strftime('%Y%m%d%H%M%S%f'))
if fsplit is None:
	force_split=False
else:
	if fsplit.upper() == 'YES' or fsplit.upper() == 'TRUE':
		force_split=True
	else:
		force_split=False
#print_header = True # include column headers in each table output
print_header = False
log_file=f"{logs_dir}/soft_delete_{reqid}.log"
err_file=f"{logs_dir}/soft_delete_{reqid}.err"
parallel_thread_count=10
del_seq=None
global dels_tbl
try:
	log_data(log_file,'Launching soft delete',1)
	set_env()
	TEMPDIR=f"{logs_dir}/{reqid}"
	out=os.popen(f'mkdir -p {TEMPDIR}').read()
	os.environ['TEMPDIR']=TEMPDIR
	os.environ['TMPDIR']=TEMPDIR
	os.environ['TMP']=TEMPDIR
	#udaexec = teradata.UdaExec(appName="ModuleName", version="0.1",logConsole=False,odbcLibPath="/opt/teradata/client/ODBC_64/lib/libodbc.so",runNumberFile="/apps/edwsfdata/python/pylogs/.runNumber",logDir="/apps/edwsfdata/python/pylogs/logs")
	configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
	soft_del_buck_limit=10000000 ## move this to config file
	if configFile.Config.PRDSUFF in env:
		sfEnv = 'cisco.us-east-1'
	elif configFile.Config.STGSUFF in env:
		sfEnv = 'ciscostage.us-east-1'
	elif configFile.Config.DEVSUFF in env:
		sfEnv = 'ciscodev.us-east-1'
	step_name='Connecting to repository'
	log_data(log_file,f"At step {step_name}",0)
	conn = open_oracle_connection(env,None,True)
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	step_name='Fetching source dbc connection parameter'
	log_data(log_file,f"At step {step_name}",0)
	currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
	if currOsUser == "":
		try:
			currOsUser=os.getlogin()
		except Exception as e:
			currOsUser=c_getuser()
			pass
	sql=f"select source_db_connection sdc from edw_job_streams where job_stream_id='{jct}'"
	source_db_conn_obj=execute_df_qry(conn, sql, reqid, conn)
	source_db_conn=source_db_conn_obj["sdc"][0]
	step_name='Fetching source db type'
	log_data(log_file,f"At step {step_name}",0)
	sql=f"select distinct parameter_category pc from eds_data_catalog.edw_parameter where environment_name='{env}' and parameter_type='{source_db_conn}'"
	db_type_obj=execute_df_qry(conn, sql, reqid, conn)
	db_type=db_type_obj["pc"][0]
	step_name="connecting to source database"
	log_data(log_file,f"At step {step_name}",0)
	srcdbParams={}
	sql=f"select parameter_name pn,to_char(parameter_value) pv from EDS_DATA_CATALOG.EDW_PARAMETER  where environment_name='{env}'"
	sql+=f" and parameter_category='{db_type}' and parameter_type ='{source_db_conn}'"
	results=execute_df_qry(conn, sql, reqid, conn)
	if len(results) == 0:
		err_msg=f"Could not find connection details for connection name - {connectionName}"
		#print (err_msg)
		raise Exception(err_msg)
	srcdbParams['SOURCE_ADDITIONAL_PARAMS'] = None
	for index,row in results.iterrows():
		param=row["pn"]
		value=row["pv"]
		srcdbParams[param]=value
	sql= f"SELECT UTL_I18N.RAW_TO_CHAR('{srcdbParams['SOURCE_LOGIN_PASSWORD']}','AL32UTF8') pass from dual"
	passobj=execute_df_qry(conn, sql, reqid, conn)
	srcPassword=passobj["pass"][0]
	currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
	if currOsUser == "":
		try:
			currOsUser=os.getlogin()
		except  Exception as e:
			currOsUser=c_getuser()
			pass
	if db_type=="ORACLE":
		connstr2 = cx_Oracle.makedsn(srcdbParams['SOURCE_HOST'], srcdbParams['SOURCE_PORT'],service_name=srcdbParams['SOURCE_SERVICE_NAME'])
		#conn2 = cx_Oracle.connect(user=srcdbParams['SOURCE_LOGIN'], password=srcPassword, dsn=connstr2,encoding="UTF-16", nencoding="UTF-16")
		conn2 = cx_Oracle.connect(srcdbParams['SOURCE_LOGIN'], srcPassword, srcdbParams['SOURCE_SERVICE_NAME'],encoding="UTF-16", nencoding="UTF-16")
	elif db_type=="TERADATA":
		#conn2 = udaexec.connect(method="odbc", DSN="tdprodetloffload")
		conn2 = teradatasql.connect(host=srcdbParams['SOURCE_HOST'],user=srcdbParams['SOURCE_LOGIN'],password=srcPassword)
		cursor2 = conn2.cursor()
		cursor2.execute(f"set query_band='Module=SoftDel;user={currOsUser};' for Session")
	elif db_type in ("HANA","HANANRT"):
			hostName=srcdbParams['SOURCE_HOST'].split(';')[0].split(':')[0]
			if 'SOURCE_ADDITIONAL_PARAMS' in srcdbParams:
				conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword,srcdbParams['SOURCE_ADDITIONAL_PARAMS'])
			else:
				conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword)
	elif db_type in ('SNOWFLAKE'):
			conn2=snowflake.connector.connect(user=srcdbParams['SOURCE_LOGIN'],password=srcPassword,account=srcdbParams['ACCOUNT'],warehouse=srcdbParams['WAREHOUSE'])
	step_name="fetching stg2br jct"
	where_clause=""
	log_data(log_file,f"At step {step_name}",0)
	sql=f"select b.job_stream_id jsi,a.source_table_name,a.source_schema,a.source_db_name,a.job_group_id,a.where_clause from edw_job_streams a,edw_job_streams b where a.active_ind='Y'"
	sql+=f" and b.source_db_name=a.target_db_name and b.source_schema=a.target_schema and"
	sql+=f" b.source_table_name=a.target_table_name and a.active_ind=b.active_Ind and a.workflow_type='SRC2STG' and a.job_stream_id='{jct}'" 
	stg2br_jct_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(stg2br_jct_obj) == 0:
		raise Exception (f"Could not locate STG2BR/SS JCT corresponding to {jct} SRC2STG")
	stg2br_jct=stg2br_jct_obj["jsi"][0]
	src_table=stg2br_jct_obj["source_table_name"][0]
	src_schema=stg2br_jct_obj["source_schema"][0]
	src_db_name=stg2br_jct_obj["source_db_name"][0]
	job_group_id=stg2br_jct_obj["job_group_id"][0]
	where_clause=stg2br_jct_obj["where_clause"][0]
	## Exit if source is ODSPROD and SCHEMA contains OPS$ since it will be paritial data
	if 'ODSPROD' in src_db_name and 'OPS$' in src_schema:
		log_data(log_file,f"Table {src_db_name} is from {src_schema} which is unsupported.",1)
		log_data(log_file,f"Hence not proceeding with soft delete",1)
		#upd_end_time(reqid,'Success','Source contain partial data',del_seq)
		sys.exit(0)
	if stg2br_jct is None or stg2br_jct == "":
		raise Exception (f"Could not locate STG2BR/SS JCT corresponding to {jct} SRC2STG")
	step_name="connecting to snowflake"
	log_data(log_file,f"At step {step_name}",0)
	sql=f"select sf_role,sf_warehouse,target_db_name,target_schema,target_table_name,target_db_connection from edw_job_streams where job_stream_id='{stg2br_jct}'"
	sf_conn_obj=execute_df_qry(conn, sql, reqid, conn)
	sf_wh=sf_conn_obj["sf_warehouse"][0]
	sf_wh_orig=sf_wh
	sf_role=sf_conn_obj["sf_role"][0]
	tgt_db_name=sf_conn_obj["target_db_name"][0]
	tgt_schema=sf_conn_obj["target_schema"][0]
	tgt_table=sf_conn_obj["target_table_name"][0]
	tgt_db_connection=sf_conn_obj["target_db_connection"][0]
	sql=f"select to_char(parameter_value) sf_username from eds_data_catalog.edw_parameter where parameter_type='{tgt_db_connection}'"
	sql+=f" and parameter_category='SNOWFLAKE' and environment_name='{env}' and parameter_name='CONNECTION_USER'"
	sf_user_dict=execute_df_qry(conn, sql, reqid, conn)
	sf_user=sf_user_dict["sf_username"][0]
	if sf_user is None or sf_user == '':
		raise Exception("Unable to determine snowflake username to connect")
	if 'sf_query_tag' in os.environ:
		sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","SOFT_DELETE")
	else:
		sf_query_tag=f"Module=SOFT_DELETE;User={currOsUser};Table={tgt_db_name}.{tgt_schema}.{tgt_table}"
		os.environ["sf_query_tag"]=sf_query_tag

	#sfDict={"sfwh":sf_wh,"sfrole":sf_role,"dbname":tgt_db_name,"schemaname":tgt_schema}
	#conn3=open_sf_connection(sfEnv,reqid,conn,sfDict)
	conn3=''
	if db_type not in ('SNOWFLAKE'):
		conn3=open_sf_connection(sfEnv,sf_user,sf_wh,sf_role,tgt_db_name,tgt_schema,query_tag=sf_query_tag)
	else:
		targetdbType=""
		sql=f"select distinct parameter_category pc from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='{envType}' and parameter_type ='{tgt_db_connection}'"
		targetdbType_obj=execute_df_qry(conn, sql, reqid, conn)	
		targetdbType=targetdbType_obj["pc"][0]
		sql=f"select parameter_name pn,to_char(parameter_value) pv from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='{envType}'"
		sql+=f" and upper(parameter_category)='{targetdbType}' and parameter_type ='{tgt_db_connection}'"
		tgtconnobj=execute_df_qry(conn, sql, reqid, conn)	
		tgtdbParams={}
		if len(tgtconnobj) == 0:
			raise Exception (f"Could not find connection details {targetdbType}.{tgt_db_connection}")
		for index,row in tgtconnobj.iterrows():
			param=row["pn"]
			value=row["pv"]
			tgtdbParams[param]=value
		sql2="SELECT UTL_I18N.RAW_TO_CHAR('"+ tgtdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') from dual"
		cursor1.execute(sql2)
		results=cursor1.fetchall()
		for passobj in results:
			tgtPassword=passobj[0]
		if targetdbType == 'TERADATA':
			conn3=teradatasql.connect(host=tgtdbParams['SOURCE_HOST'],user=tgtdbParams['SOURCE_LOGIN'], password=tgtPassword)
		else:
			raise Exception("Unsupported Target db Type. Currently Teradata is only supported db type")
	if type(conn3) is int:
		raise Exception ("Unable to connect to Snowflake DB")
	## Sequence used to maintain unique delete id for each run
	sql="select delete_seq.nextval val from dual"
	seq_obj=execute_df_qry(conn, sql, reqid, conn)
	del_seq=seq_obj["val"][0]
	if db_type == "ORACLE":
		parallel_clause="/*+ parallel(4) */"
		count_clause="count(*) cnt"
	elif db_type == "TERADATA" or db_type == "HANA" or db_type == "HANANRT" or db_type == "SNOWFLAKE":
		parallel_clause=""
		count_clause="cast(count(*) as bigint) cnt"
	if db_type=="TERADATA":
		log_data(log_file,"Now validating base schema and table",0)
		sql=f" exec PERFMETRICSDB.BASEVIEWCHECK ('{src_schema}','{src_table}')"
		basetblobj=execute_df_qry(conn2, sql, reqid, conn)
		basetbl=basetblobj["src_obj"][0]
		srcBaseschema=basetbl.split(".")[0]
		srcBasetable=basetbl.split(".")[1]
		if srcBaseschema != src_schema:
			log_data(log_file,"Since base schema and JCT schema are not matching switching the same",0)
			src_schema=srcBaseschema
			src_table=srcBasetable
	elif db_type == "HANANRT":
		log_data(log_file,"Now fetching base table and schema",0)
		sql=f"select object_type from sys.objects where object_name='{src_table}' and schema_name='{src_schema}'"
		basetblobj=execute_df_qry(conn2, sql, reqid, conn)
		obj_type=basetblobj["object_type"][0]
		if obj_type == 'VIEW':
			sql=f"select base_schema_name,base_object_name from SYS.object_dependencies where dependent_object_name = '{src_table}'"
			sql+=f" and dependent_schema_name='{src_schema}' and base_object_type='TABLE' and base_schema_name not like '%SYS%'"
			sql+=f" AND   BASE_OBJECT_NAME <> 'BATCHES_TO_PUBLISH'"
			viewobj=execute_df_qry(conn2, sql, reqid, conn)
			if len(viewobj) != 1:
				raise Exception (f"Passed object {src_schema}.{src_table} is not 1-1 View")
			else:
				src_schema=viewobj["base_schema_name"][0]
				src_table=viewobj["base_object_name"][0]
	step_name="fetching target table count"
	log_data(log_file,f"At step {step_name}",0)
	sql=f"select count(*) cnt from {tgt_db_name}.{tgt_schema}.{tgt_table} where nvl(edwsf_source_deleted_flag,'N')='N'"
	if where_clause is not None and len(where_clause) > 0:
		sql+=f" and {where_clause} "
	tgt_cnt_obj=execute_df_qry(conn3, sql, reqid, conn)
	tgt_cnt=tgt_cnt_obj["cnt"][0]
	step_name="fetching source table count"
	log_data(log_file,f"At step {step_name}",0)
	if where_clause is None or len(where_clause) == 0:
		buck_filter=None
		if db_type == "HANA":
			sql=f'select {parallel_clause} {count_clause} from {src_schema}."{src_table}" a'
		elif db_type == "HANANRT":
			sql=f'select {parallel_clause} {count_clause} from "{src_schema}"."{src_table}" a'
		elif db_type == "SNOWFLAKE":
			sql=f'select {count_clause} from "{src_db_name}"."{src_schema}"."{src_table}" a'
		elif db_type == "ORACLE":
			sql=f'select /*+ parallel(16) */ {count_clause} from "{src_schema}"."{src_table}" a'
		else:
			sql=f"select {parallel_clause} {count_clause} from {src_schema}.{src_table} a"
	else:
		buck_filter=where_clause
		sql=f'select {parallel_clause} {count_clause} from {src_schema}."{src_table}" a where {where_clause}'
	## Source count pulled at beginning of ingestion process will be attempted to re-use. But if it is greater than target count
	## (reason could be more records get generated while ingestion is in progress) source count is pulled again just to get latest count.
	if aud_num_rows is not None and aud_num_rows > 0 and aud_num_rows >= tgt_cnt:
		log_data(log_file,f"Reusing source count {aud_num_rows} passed as parameter",0)
		src_cnt=aud_num_rows
	else:
		if db_type == "TERADATA":
			sql = f"LOCK ROW FOR ACCESS {sql}"
		src_cnt_obj=execute_df_qry(conn2, sql, reqid, conn)
		src_cnt=int(src_cnt_obj["cnt"][0]) ## since TD is returning count in decimal
		log_data(log_file,f"Pulled source count {src_cnt} from source db",0)
	sql="insert into diy_soft_delete(delete_id,reqid,source_table,target_table,source_count,target_count,start_time,last_update_time,attribute1,attribute2) values "
	sql+=f"({del_seq},{reqid},'{src_db_name}.{src_schema}.{src_table}','{tgt_db_name}.{tgt_schema}.{tgt_table}',{src_cnt},{tgt_cnt},sysdate,sysdate,'{pgmVersion}','{currOsUser}')"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)
	step_name="compare count"
	log_data(log_file,f"At step {step_name}",0)
	if src_cnt >= tgt_cnt:
		log_data(log_file,f"Table {tgt_db_name}.{tgt_schema}.{tgt_table} - {tgt_cnt} is less than or equal to {src_schema}.{src_table} - {src_cnt}.",1)
		log_data(log_file,f"Hence not proceeding with soft delete",1)
		upd_end_time(reqid,'Success','Nothing to do',del_seq)
		sys.exit(0)
	if src_cnt == 0:
		if tgt_cnt > 0:
			log_data(log_file,f"Table {src_schema}.{src_table} has 0 rows where as {tgt_db_name}.{tgt_schema}.{tgt_table} has {tgt_cnt} rows.. Hence marking all as deleted",1)
			sql=f"Update {tgt_db_name}.{tgt_schema}.{tgt_table} set edwsf_source_deleted_flag='Y'"
			execute_qry(conn3, sql, reqid, conn)
			execute_qry(conn3, "Commit", reqid, conn)
			upd_end_time(reqid,'Success','Full table marked as deleted',del_seq)
			sys.exit(0)
	if db_type == "HANANRT":
		log_data(log_file,f"Table {src_schema}.{src_table} is of type HANANRT. Hence not proceeding further",1)
		upd_end_time(reqid,'Success','Nothing to do',del_seq)
		sys.exit(0)
	if db_type == "SNOWFLAKE":
		log_data(log_file,f"Table {src_schema}.{src_table} is of type SNOWFLAKE. Hence not proceeding further",1)
		upd_end_time(reqid,'Success','Nothing to do',del_seq)
		sys.exit(0)
	## New change to perform dynamic wh resize
	sql=f"select bytes/(1024*1024*1024) byt from {tgt_db_name}.information_schema.tables  where table_name='{tgt_table}' and table_schema='{tgt_schema}'"
	resultsObj=execute_df_qry(conn3,sql,reqid,conn)
	sftblSize = resultsObj["byt"][0]
	if sf_wh != 'EDW_DATALAKE_WH':
		sf_wh=sf_wh.replace('COMMON_WH','COMMON_EXT1_WH')
		if sftblSize >= 50:
			sf_wh_4_upd=sf_wh_orig.replace('COMMON_WH','COMMON_EXT3_WH')
		elif sftblSize >= 25:
			sf_wh_4_upd=sf_wh_orig.replace('COMMON_WH','COMMON_EXT2_WH')
		else:
			sf_wh_4_upd=sf_wh_orig.replace('COMMON_WH','COMMON_EXT1_WH')
		log_data(log_file,f"Switching warehouse to {sf_wh}",0)
		execute_qry(conn3,f"use warehouse {sf_wh}",reqid,conn)	
	## End of change to perform dynamic wh resize
	step_name="pulling source unique key details"
	log_data(log_file,f"At step {step_name}",0)
	sql=f"select unique_key_name ukn from eds_data_catalog.edw_unique_key where environment_name='{env}' and db_instance_name='{src_db_name}'"
	sql+=f" and db_schema_name='{src_schema}' and table_name='{src_table}' and active_flag='A'"
	unique_key_obj=execute_df_qry(conn, sql, reqid, conn)
	if type(unique_key_obj) is int:
		raise Exception("Exception encountered while pulling unique key for {src_table}")
	unique_key=unique_key_obj["ukn"][0]
	sql=f"select a.column_name,b.data_type,b.nullable_flag,nvl(data_scale,0) data_scale,nvl(data_precision,0) data_precision from eds_data_catalog.edw_unique_key_column a,eds_data_catalog.edw_table_column b "
	sql+=f" where a.environment_name='{env}' and a.db_instance_name='{src_db_name}' and a.db_schema_name='{src_schema}'"
	sql+=f" and a.table_name='{src_table}' and unique_key_name='{unique_key}' and a.environment_name=b.environment_name and a.db_instance_name=b.db_instance_name"
	sql+=f" and a.db_schema_name=b.db_schema_name and a.table_name=b.table_name and a.column_name=b.column_name order by column_position"
	unique_key_cols_set=[]
	uk_nvl_set=[]
	results=execute_df_qry(conn, sql, reqid, conn)
	if type(results) is int:
		raise Exception("Exception encountered while pulling unique key columns for {src_table}")
	src_uk_qry="select "
	src_uk_qry_4_del="select "
	stg_uk_qry="select "
	col_num=1
	for index,row in results.iterrows():
		unique_key_cols_set.append(row["column_name"])
		uk_nvl_set.append(f'{row["column_name"]}~{row["data_type"]}~{row["nullable_flag"]}')
		if row["data_type"] == "DATE":
			if db_type=="TERADATA":
				stg_uk_qry += f"to_date(${col_num},'YYYY/MM/DD'),"
			elif db_type=="HANA": ## 1.55 change
				stg_uk_qry += f"to_date(${col_num},'AUTO'),"
			else:
				stg_uk_qry += f"to_timestamp(${col_num},'AUTO'),"
		elif "TIMESTAMP" in row["data_type"] or "SECONDDATE" in row["data_type"]:
			stg_uk_qry += f"to_timestamp(${col_num},'AUTO'),"
		elif row["data_type"] == "CHAR" or row["data_type"] == "VARCHAR" or row["data_type"] == "VARCHAR2" or row["data_type"] == "NVARCHAR":
			if db_type=="TERADATA":
				if row["nullable_flag"].upper() == "Y":
					stg_uk_qry += f"nvl(nullif(rtrim(${col_num}),''),'0'),"
				else:
					stg_uk_qry += f"rtrim(${col_num}),"
			else:
				if row["data_type"] == "VARCHAR2":
					if row["nullable_flag"].upper() == "Y":
						stg_uk_qry += f"nvl(nullif(rtrim(${col_num}),''),'0'),"
					else:
						stg_uk_qry += f"rtrim(${col_num}),"
				else:
					if row["nullable_flag"].upper() == "Y":
						stg_uk_qry += f"nvl(${col_num},'0'),"
					else:
						stg_uk_qry += f"${col_num},"
		else:
			if row["nullable_flag"].upper() == "Y":
				#stg_uk_qry += f"nvl(${col_num},0),"
				## 1.27 change
				if row["data_type"].upper() in ('DECIMAL','FLOAT','DOUBLE','BIGDECIMAL') or (row["data_type"] == 'NUMBER' and row["data_scale"] > 0):
					stg_uk_qry += f"nvl(${col_num}::float,0),"
				else:
					stg_uk_qry += f"nvl(${col_num}::Number,0),"
			else:
				stg_uk_qry += f"${col_num},"
		if row["nullable_flag"].upper() == "Y":
			if  row["data_type"] == "CHAR" or row["data_type"] == "VARCHAR" or row["data_type"] == "VARCHAR2"  or row["data_type"] == "NVARCHAR":
				if db_type=="HANA":
					src_uk_qry += "ifnull(" + row["column_name"] + ",'0'),"
					src_uk_qry_4_del += "ifnull(" + row["column_name"] + ",'0'),"
				else:
					src_uk_qry += "nvl(" + row["column_name"] + ",'0'),"
					src_uk_qry_4_del += "nvl(" + row["column_name"] + ",'0'),"
			elif row["data_type"] in( "NUMBER","INTEGER","DECIMAL","BIGINT","BIGDECIMAL","FLOAT","INTEGER","DOUBLE","TINYINT","SMALLINT"):
				if db_type=="HANA":
					src_uk_qry += "ifnull(" + row["column_name"] + ",0),"
					src_uk_qry_4_del += "ifnull(" + row["column_name"] + ",0),"
				else:
					src_uk_qry += "nvl(" + row["column_name"] + ",0),"
					src_uk_qry_4_del  += "nvl(" + row["column_name"] + ",0),"
			else:
				src_uk_qry += row["column_name"] + ','
				src_uk_qry_4_del += row["column_name"] + ','
		else:
			if  row["data_type"] == "CHAR" or row["data_type"] == "VARCHAR" or row["data_type"] == "VARCHAR2"  or row["data_type"] == "NVARCHAR":
				src_uk_qry += "rtrim(" + row["column_name"] + ") " + row["column_name"] + "," ## 1.54 change
				if db_type=='TERADATA':
					src_uk_qry_4_del += f"rtrim({row['column_name']})," ## 1.54 change
				else:
					src_uk_qry_4_del += row["column_name"] + ','
			else:
				src_uk_qry += row["column_name"] + ','
				src_uk_qry_4_del += row["column_name"] + ','
			#src_uk_qry_4_del += row["column_name"] + ',' ## 1.54 change
		col_num+=1
	## Remove last comma from source query
	stg_uk_qry=stg_uk_qry[:-1]
	tgt_uk_qry=src_uk_qry[:-1]
	src_uk_qry_4_del = src_uk_qry_4_del[:-1]
	if db_type=="HANA":
		src_uk_qry=src_uk_qry[:-1] + f' from {src_schema}."{src_table}" a '
	else:
		src_uk_qry=src_uk_qry[:-1] + f" from {src_schema}.{src_table} a "
	step_name="pulling source table buckets"
	log_data(log_file,f"At step {step_name}",0)
	num_buckets=math.ceil(src_cnt/soft_del_buck_limit)
	if num_buckets > 50:
		num_buckets=50 ## Go with a max of 50 buckets even for large tables.
	sql=f"select * from "
	sql+=f"(select split_column,max(reqid) reqid from diy_master where source_table='{src_table}' and source_schema='{src_schema}' and source_db_name='{src_db_name}'"
	sql+=f" and load_type='ALL_DATA' and instr(current_phase,'Success')>0 and split_column is not null group by split_column order by 2 desc) where rownum=1"
	split_col_obj=execute_df_qry(conn, sql, reqid, conn)
	split_reqid=None
	split_col_name=""
	if len(split_col_obj) != 0:
		split_col_name_obj=split_col_obj["split_column"][0]
		split_col_name=split_col_name_obj.split('~',1)[0]
		split_reqid=split_col_obj["reqid"][0]
	sql=f"select nvl(max(reqid),0) reqid from diy_soft_del_buckets where table_name='{src_db_name}.{src_schema}.{src_table}' and condition <> 'ALL_DATA'"
	buck_obj=execute_df_qry(conn, sql, reqid, conn)
	split_buck_reqid=buck_obj["reqid"][0]
	if split_buck_reqid !=0 :
		sql=f"select distinct column_name split_column from diy_soft_del_buckets where reqid={split_buck_reqid}"
		split_col_obj=execute_df_qry(conn, sql, reqid, conn)
		split_col_name=split_col_obj["split_column"][0]
	## If no bucket exists or force split is requesting during invocation then try to create new set of splits. 
	if split_buck_reqid == 0 or force_split:
		if split_reqid is None or force_split:
			step_name="creating buckets"
			log_data(log_file,f"At step {step_name}",0)
			if not force_split:
				log_data(log_file,f"Looks like no prior split record exists for {src_table} in DIY tables. Hence attempting to create split buckets based on rowcount",0)
			else:
				log_data(log_file,f"Attempting to force creating new split buckets based on request",0)
			if num_buckets > 1:
				full_tbl_name=f"{src_db_name}~{src_schema}~{src_table}"
				split_table(db_type,full_tbl_name,num_buckets,conn2,reqid,split_col_name,buck_filter)
			else:
				sql=f"insert into diy_soft_del_buckets (reqid,table_name,bucket_number,condition) values ({reqid},'{src_db_name}.{src_schema}.{src_table}',1,'ALL_DATA') "
				execute_qry(conn, sql, reqid, conn)
				execute_qry(conn,"commit",reqid,conn)
		else:
			step_name="populating_soft_delete_buckets"
			log_data(log_file,f"At step {step_name}",0)
			pop_diy_soft_del_buckets(split_reqid,conn,reqid,num_buckets,split_col_name,src_db_name +"~" + src_schema + "~" + src_table,db_type,conn2)
	else:
		pop_diy_soft_del_buck_from_bucket(split_buck_reqid,conn,reqid,split_col_name,src_db_name +"~" + src_schema + "~" + src_table,db_type,conn2)
	prev_req_found=False
	buck_num_list=[]
	sql=f"select max(a.reqid) prev_req_id from diy_soft_del_buckets a,diy_soft_delete b where a.reqid < {reqid} and table_name='{src_db_name}.{src_schema}.{src_table}'"
	sql+=f" and a.reqid=b.reqid and b.result='Success'"
	#sql=f"select max(reqid) prev_req_id from diy_soft_del_buckets where reqid < {reqid} and table_name='{src_db_name}.{src_schema}.{src_table}'"
	prev_req_id_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(prev_req_id_obj) == 1:
		prev_req_id=prev_req_id_obj["prev_req_id"][0]
		if prev_req_id is not None:
			prev_req_found=True
			sql=f"select bucket_number from diy_soft_del_buckets where reqid={prev_req_id} order by nvl(softdel_cnt,0) desc "
			result_buck=execute_df_qry(conn, sql, reqid, conn)
			for index,row in result_buck.iterrows():
				buck_num_list.append(row["bucket_number"])
	sql=f"select min_value,max_value,source_count,condition,bucket_number from diy_soft_del_buckets where reqid={reqid} order by bucket_number"
	results=execute_df_qry(conn, sql, reqid, conn)
	if len(results) == 0:
		raise Exception("Could not find entry in soft_del_buckets. Please check")
	if len(results) > 1 :
		step_name="Fetching split column data type"
		log_data(log_file,f"At step {step_name}",0)
		sql=f"select distinct column_name from diy_soft_del_buckets where reqid={reqid}"
		col_name_obj=execute_df_qry(conn, sql, reqid, conn)
		split_col_name=col_name_obj["column_name"][0]
		sql=f"select data_type from eds_data_catalog.edw_table_column where column_name='{split_col_name}' and table_name='{src_table}'"
		sql+=f" and environment_name='{env}' and db_instance_name='{src_db_name}' and db_schema_name='{src_schema}'"
		col_data_type_obj=execute_df_qry(conn, sql, reqid, conn)
		col_data_type=col_data_type_obj["data_type"][0]
	else:
		col_data_type=''
	step_name="Fetching split column SF data type"
	sf_data_type=None
	sf_type=None
	if len(split_col_name) > 0:
		log_data(log_file,f"At step {step_name}",0)
		sql=f"select numeric_precision,numeric_scale,data_type from {tgt_db_name}.information_schema.columns where table_name='{tgt_table}'"
		sql+=f" and column_name='{split_col_name}' and table_schema='{tgt_schema}'";
		tgt_dtype_obj=execute_df_qry(conn3, sql, reqid, conn)
		sf_precision=tgt_dtype_obj["numeric_precision"][0]
		sf_scale=tgt_dtype_obj["numeric_scale"][0]
		sf_type=tgt_dtype_obj["data_type"][0]
		if sf_type == 'NUMBER':
			sf_data_type=f"{sf_type}({sf_precision},{sf_scale})"
			log_data(log_file,f"Determined snowflake column {split_col_name} data type as {sf_data_type}",0)
		#sql=f"select count(*) cnt from {tgt_db_name}.information_schema.tables where table_name='{tgt_table}' and table_schema='{tgt_schema}' and upper(nvl(clustering_key,'NONE')) like '%{split_col_name}%'"
		#cl_obj=execute_df_qry(conn3, sql, reqid, conn)
		#clusterkey_cnt=cl_obj["cnt"][0]
		#if clusterkey_cnt > 0:
		#	sf_wh_4_upd=sf_wh ## if clustering key exist on split column use small sized wh for soft del
	bucket_num=0
	soft_del_cnt=0
	step_name="Creating dels table"
	log_data(log_file,f"At step {step_name}",0)
	dels_tbl=cr_dels_table(unique_key_cols_set,tgt_db_name,tgt_table,tgt_schema,conn3,reqid,conn)
	thread_array=[]
	#if db_type=="TERADATA":
	#	step_name=f"Now generating header file"
	#	log_data(log_file,f"At step {step_name}",0)
	#	col_header=""
	#	for col_name in unique_key_cols_set:
	#		col_header+=f"{col_name}|"
	#	## Remove last pipe from column header
	#	col_header=col_header[:-1]
	#	with open (f"{logs_dir}/header.txt","w") as pf:
	#		pf.write(col_header)
	#		pf.write("\n")
	if prev_req_found:
		if len(results) == len(buck_num_list):
			prev_req_found=True
		else:
			prev_req_found=False
	min_max_buck=[]
	min_max_buck.append('')
	for index,row in results.iterrows():
		min_max_buck.append(row)
	loop_counter=0
	for i in range(len(min_max_buck)-1):
		if prev_req_found:
			v_bucket_number=buck_num_list[loop_counter]
			loop_counter+=1
		else:
			loop_counter+=1
			v_bucket_number=loop_counter
		condition=min_max_buck[v_bucket_number]["condition"]
		min_val=min_max_buck[v_bucket_number]["min_value"]
		max_val=min_max_buck[v_bucket_number]["max_value"]
		if sf_type == 'NUMBER':
			min_val=f"cast({min_val} as {sf_data_type})"
			max_val=f"cast({max_val} as {sf_data_type})"
		if col_data_type != '':
			if "date" in col_data_type.lower():
				if db_type=="TERADATA" or db_type=="HANA":
					min_val=f"to_date('{min_val}','MM/DD/YYYY')"
					max_val=f"to_date('{max_val}','MM/DD/YYYY')"
				else:
					min_val=f"to_timestamp('{min_val}','MM/DD/YYYY HH24:MI:SS')"
					max_val=f"to_timestamp('{max_val}','MM/DD/YYYY HH24:MI:SS')"
			if "time" in col_data_type.lower():
				sql=f"select max(length(min_value)-length(replace(min_value,':',''))) lv from diy_soft_del_buckets where reqid={reqid}"
				len_obj=execute_df_qry(conn, sql, reqid, conn)
				col_len=len_obj["lv"][0]
				if col_len == 3:
					min_val=f"to_timestamp('{min_val}','MM/DD/YYYY HH24:MI:SS:FF6')"
					max_val=f"to_timestamp('{max_val}','MM/DD/YYYY HH24:MI:SS:FF6')"
				elif col_len == 2:
					min_val=f"to_timestamp('{min_val}','MM/DD/YYYY HH24:MI:SS')"
					max_val=f"to_timestamp('{max_val}','MM/DD/YYYY HH24:MI:SS')"
				else:
					raise Exception("Invalid timestamp value format. Please check")
			elif "char" in col_data_type.lower():
				min_val=f"'{min_val}'"
				max_val=f"'{max_val}'"
			if condition == "between":
				filter=f"between {min_val} and {max_val}"
			elif condition in ('>','<'):
				filter=f" {condition} {min_val}"
			elif condition == "is null":
				filter=f" {condition} "
		else:
			filter=''
		src_row_cnt=row["source_count"]
		#perform_soft_delete(bucket_num,conn,conn2,conn3,filter)
		thread_array.append(threading.Thread(target=perform_soft_delete, args=(v_bucket_number,filter)))
	## end of bucket for loop
	loop_cnt=1
	tot_work_count=len(thread_array)
	cum_loop_cnt=0
	ins_while_flag=False
	for worker in thread_array:
		t=worker.start()
		time.sleep(5) ## sleep 5 seconds between each thread invocation
		#time.sleep(30) ## sleep 30 seconds between each thread invocation.
		main_thread = threading.currentThread()
		while loop_cnt >= parallel_thread_count:
			loop_cnt=threading.active_count()-1
			ins_while_flag=True
			time.sleep(30) ## Sleep 30 seconds before checking curreng active threads
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
		#sql=f"select nvl(sum(softdel_cnt),0) cnt from diy_soft_del_buckets where reqid={reqid}" # 1.56 change
		sql=f"select nvl(sum(sf_count-source_count),0) cnt from diy_soft_del_buckets where reqid={reqid}"
		buck_del_cnt_obj=execute_df_qry(conn, sql, reqid, conn)
		buck_del_cnt=buck_del_cnt_obj["cnt"][0]
		sql=f"select target_count-({buck_del_cnt})-source_count diff_cnt from diy_soft_delete where reqid={reqid}"
		cur_diff_obj=execute_df_qry(conn, sql, reqid, conn)
		cur_diff_count=cur_diff_obj["diff_cnt"][0]
		if cur_diff_count <=0:
			log_data(log_file,f"Since difference between sf and source count is {cur_diff_count} not iterating any more buckets",0)
			break ## order execution by previous executed bucket number and break if SF count is less than or equal to source count
	## end of bucket for loop
	## New block added to wait till all threads complete
	if (threading.active_count() > 1):
		for t in threading.enumerate():
			if main_thread is not None and t is main_thread:
				continue
			if t.getName() != 'MainThread':
				log_data(log_file,f"Waiting for {t.getName()} to complete",0)
				t.join()
	if len(thread_errors) > 0: #check if there are any errors
		for e in thread_errors:
			err_msg=f"{e[1]} occurred in thread: {e[0]}"
			log_data(err_file,err_msg,1)
		upd_end_time(reqid,'Error',err_msg,del_seq)
	else:
		perform_sof_del(conn3,reqid,conn)
		log_data(log_file,f"Succesful soft delete completion for table {tgt_db_name}.{tgt_schema}.{tgt_table}",1)
		upd_end_time(reqid,'Success','',del_seq)
	conn.close()
	if isinstance(conn3,snowflake.connector.connection.SnowflakeConnection):
		conn3.close()
except Exception as e:
	log_data(err_file,f"Failure at step {step_name}. Please find below exception",1)
	log_data(err_file,str(e),1)
	log_data(err_file,f"Last sql executed was :- {sql}",1)
	if del_seq is not None:
		upd_end_time(reqid,f'Failure at step {step_name}',str(e).replace("'","''")[0:200],del_seq)
	print(f"Below exception occurred at step {step_name} while performing soft delete")
	print(str(e))
	print(sql)
	sys.exit(1)
