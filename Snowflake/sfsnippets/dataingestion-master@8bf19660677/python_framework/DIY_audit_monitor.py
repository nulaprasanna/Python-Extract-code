#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_audit_monitor.py                                                                                                  #
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Feb-2021                Initial Version                                                 #
# 1.1           Manick          Mar-2021                Move the audit collection into multiple workers                 #
# 1.2           Manick          Mar-2021                Include logic for load types other than DATE                    #
# 1.3           Manick          Mar-2021                Invoke audit fix if gap exists                                  #
# 1.4           Manick          Mar-2021                Integration with email_audit_report.py                          #
# 1.5           Manick          Mar-2021                Use parallel thread count and FTS based on table size           #
# 1.6           Manick          Mar-2021                Fix to take care of moving Job groups on JCTs                   #a
#########################################################################################################################
from metadata_handler import *
import cx_Oracle,teradatasql,threading,subprocess
from datetime import datetime
def derive_delta(env,reqid,log_file,target_db,target_schema,target_table,job_group,curr_delta,source_db,source_schema,source_table):
	global step_name
	autofix_flag='N'
	avg_ins_list=[]
	conn=open_oracle_connection(env,None,True)
	sql=f"select * from (select tgt_rows,start_time from diy_master where source_schema='{source_schema}'  and source_db_name='{source_db}' and source_table='{source_table}' and ingestion_type='JCT' and nvl(tgt_rows,0) > 0 order by start_time desc) where rownum<8"
	ins_obj=execute_df_qry(conn,sql,reqid,conn)
	for ind3,row3 in ins_obj.iterrows():
		if ind3 == 0:
			start_time=row3["start_time"]
			start_rows=row3["tgt_rows"]
		else:
			end_time=start_time
			start_time=row3["start_time"]
			end_rows=start_rows
			start_rows=row3["tgt_rows"]
			ins_rate=end_rows/((end_time-start_time).total_seconds()/60)
			avg_ins_list.append(ins_rate)
	rpm=round(sum(avg_ins_list)/len(avg_ins_list))
	sql=f"select max(reqid) reqid from diy_master where source_schema='{source_schema}'  and source_db_name='{source_db}' and source_table='{source_table}'  and ingestioN_type='JCT' and current_phase='Success'"
	maxreqid_obj=execute_df_qry(conn,sql,reqid,conn)
	max_reqid=maxreqid_obj["reqid"][0]
	sql=f"select count(1) cnt from diy_lines where reqid ={max_reqid} and step_name='Launch_STG2BR'"
	step_cnt_obj=execute_df_qry(conn,sql,reqid,conn)
	step_cnt=step_cnt_obj["cnt"][0]
	if step_cnt == 1:
		sql=f"select (step_end_time-start_time)*24*60 mins from diy_master a,diy_lines b  where a.reqid ={max_reqid} and a.reqid=b.reqid and b.step_name='Launch_STG2BR'"
	else:
		sql=f"select (end_time-start_time)*24*60 mins from diy_master a where a.reqid ={max_reqid}"
	run_mins_obj=execute_df_qry(conn,sql,reqid,conn)
	run_mins=run_mins_obj["mins"][0]
	ins_gwth_rate=round(run_mins*rpm)
	sql=f"select nvl(soft_del_count,0) soft_del_count,start_time from diy_soft_delete where target_table='{target_db}.{target_schema}.{target_table}' and start_time > sysdate-8 order by start_time desc"
	soft_del_obj=execute_df_qry(conn,sql,reqid,conn)
	if len(soft_del_obj) < 2:
		avg_del_rate=0
	else:
		avg_del_list=[]
		for ind2,row2 in soft_del_obj.iterrows():
			if ind2 == 0:
				start_time=row2["start_time"]
				start_rows=row2["soft_del_count"]
			else:
				end_time=start_time
				start_time=row2["start_time"]
				end_rows=start_rows
				start_rows=row2["soft_del_count"]
				del_rate=end_rows/((end_time-start_time).total_seconds()/60)
				avg_del_list.append(del_rate)
		avg_del_rate=round(sum(avg_del_list)/len(avg_del_list))
	del_gwth_rate=round(run_mins*avg_del_rate)
	perm_delta=del_gwth_rate+ins_gwth_rate
	if perm_delta >= curr_delta:
		sync_flag='N'
	else:
		sync_flag='Y'
	sql=f"select source_db_type sdt,load_type lt from diy_master where reqid={max_reqid}"
	sdt_obj=execute_df_qry(conn,sql,reqid,conn)
	sdt=sdt_obj["sdt"][0]
	load_type=sdt_obj["lt"][0]
	if sync_flag == 'Y':
		if sdt in ('ORACLE','HANA','TERADATA') :
			step_name=f"Querying source to make sure delta exists for table {target_table}"
			log_data(log_file,f"At step {step_name}",0)
			sql=f"select source_db_connection sdc,incremental_column_name icn,source_table_name stn,source_schema ss,source_db_name sdn,where_clause wc,to_extract_id tei,job_stream_id jsi from edw_job_streams "
			sql+=f" where target_table_name='{target_table}' and workflow_type='SRC2STG' and active_ind='Y' and job_group_id='{job_group}'"
			sdc_obj=execute_df_qry(conn,sql,reqid,conn)
			sdc=sdc_obj["sdc"][0]
			icn=sdc_obj["icn"][0]
			source_table=sdc_obj["stn"][0]
			source_schema=sdc_obj["ss"][0]
			source_db=sdc_obj["sdn"][0]
			where_clause=sdc_obj["wc"][0]
			max_id=sdc_obj["tei"][0]
			job_stream_id=sdc_obj["jsi"][0]
			srcdbParams={}
			sql=f"select parameter_name pn,to_char(parameter_value) pv from EDS_DATA_CATALOG.EDW_PARAMETER  where environment_name='PRD'"
			sql+=f" and parameter_category='{sdt}' and parameter_type ='{sdc}'"
			results=execute_df_qry(conn, sql, reqid, conn)
			if len(results) == 0:
				err_msg=f"Could not find connection details for connection name - {sdc}"
				raise Exception(err_msg)
			srcdbParams['SOURCE_ADDITIONAL_PARAMS'] = None
			for index,row in results.iterrows():
				param=row["pn"]
				value=row["pv"]
				srcdbParams[param]=value
			sql= f"SELECT UTL_I18N.RAW_TO_CHAR('{srcdbParams['SOURCE_LOGIN_PASSWORD']}','AL32UTF8') pass from dual"
			passobj=execute_df_qry(conn, sql, reqid, conn)
			srcPassword=passobj["pass"][0]
			cnt_clause="count(1)"
			par_clause=""
			sql="select to_char(trunc(sysdate-1),'mm/dd/yyyy hh24:mi:ss') dt  from dual"
			date_obj=execute_df_qry(conn,sql,reqid,conn)
			datev=date_obj['dt'][0]
			sql=f"select source_count cnt from EDW_AUDIT_MON where target_table='{target_table}' and target_schema='{target_schema}' and target_db='{target_db}'"
			repo_src_cnt_obj=execute_df_qry(conn,sql,reqid,conn)
			repo_src_cnt=repo_src_cnt_obj["cnt"][0]

			if sdt=="ORACLE":
				connstr2 = cx_Oracle.makedsn(srcdbParams['SOURCE_HOST'], srcdbParams['SOURCE_PORT'],service_name=srcdbParams['SOURCE_SERVICE_NAME'])
				conn2 = cx_Oracle.connect(srcdbParams['SOURCE_LOGIN'], srcPassword, srcdbParams['SOURCE_SERVICE_NAME'],encoding="UTF-16", nencoding="UTF-16")
				sql=f"select index_name,index_owner from all_ind_columns where table_name='{source_table}' and column_name='{icn}' and table_owner='{source_schema}'"
				ind_ex_obj=execute_df_qry(conn2,sql,reqid,conn)
				if len(ind_ex_obj) > 0:
					ind_name=ind_ex_obj["index_name"][0]
					ind_owner=ind_ex_obj["index_owner"][0]
					sql=f"select count(1) cnt from all_ind_columns where index_name='{ind_name}' and table_name='{source_table}' and index_owner='{ind_owner}'"
					ind_col_obj=execute_df_qry(conn2,sql,reqid,conn)
					ind_col_cnt=ind_col_obj["cnt"][0]
					if ind_col_cnt == 1:
						if repo_src_cnt > 800000000:
							par_clause="/*+ parallel(16) */"
						elif repo_src_cnt > 300000000:
							par_clause="/*+ parallel(8) */"
						else:
							par_clause=""
					else:
						par_clause="/*+ parallel(8) */"
				else:
					par_clause="/*+ parallel(8) */"
				sys_date=f"to_date('{datev}','mm/dd/yyyy hh24:mi:ss')"
			elif sdt=="TERADATA":
				conn2 = teradatasql.connect(host=srcdbParams['SOURCE_HOST'],user=srcdbParams['SOURCE_LOGIN'],password=srcPassword)
				cnt_clause="cast(count(1) as bigint)"
				sys_date=f"to_timestamp('{datev}','mm/dd/yyyy hh24:mi:ss')"
			elif sdt  == "HANA":
					hostName=srcdbParams['SOURCE_HOST'].split(';')[0].split(':')[0]
					if 'SOURCE_ADDITIONAL_PARAMS' in srcdbParams:
						conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword,srcdbParams['SOURCE_ADDITIONAL_PARAMS'])
					else:
						conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword)
					#sys_date="add_days(current_date,-2)"
					sys_date=f"to_timestamp('{datev}','mm/dd/yyyy hh24:mi:ss')"
			if load_type == 'DATE':
				sql=f'select {par_clause} {cnt_clause} cnt from "{source_schema}"."{source_table}" a where {icn} < {sys_date}'
				snow_sql=f"select count(1) cnt from \"{target_db}\".\"{target_schema}\".\"{target_table}\" where {icn} < to_timestamp('{datev}','mm/dd/yyyy hh24:mi:ss') and nvl(edwsf_source_deleted_flag,'N')='N'"
			elif load_type == 'ID':
				sql=f'select {par_clause} {cnt_clause} cnt from "{source_schema}"."{source_table}" a where {icn} < {max_id}'
				snow_sql=f"select count(1) cnt from \"{target_db}\".\"{target_schema}\".\"{target_table}\" where {icn} < {max_id} and nvl(edwsf_source_deleted_flag,'N')='N'"
			elif load_type == 'Filtered_DATE':
				sql=f'select {par_clause} {cnt_clause} cnt from "{source_schema}"."{source_table}" a where {icn} < {sys_date} and {where_clause}'
				snow_sql=f"select count(1) cnt from \"{target_db}\".\"{target_schema}\".\"{target_table}\" where {icn} < to_timestamp('{datev}','mm/dd/yyyy hh24:mi:ss') and nvl(edwsf_source_deleted_flag,'N')='N' and {where_clause}"
			elif load_type == 'Filtered_ID':
				sql=f'select {par_clause} {cnt_clause} cnt from "{source_schema}"."{source_table}" a where {icn} < {max_id} and {where_clause}'
				snow_sql=f"select count(1) cnt from \"{target_db}\".\"{target_schema}\".\"{target_table}\" where {icn} < {max_id} and nvl(edwsf_source_deleted_flag,'N')='N' and {where_clause}"
			else:
				## unsupport load type set count to -9999
				src_cnt=9999
				sf_cnt=-9999
			src_cnt_obj=execute_df_qry(conn2,sql,reqid,conn)
			src_cnt=src_cnt_obj["cnt"][0]
			conn2.close()
			conn3=open_sf_connection('cisco.us-east-1','EDW_DATALAKE_SVC')
			sf_cnt_obj=execute_df_qry(conn3,snow_sql,reqid,conn)
			sf_cnt=sf_cnt_obj["cnt"][0]
			conn3.close()
			if sf_cnt < src_cnt:
				log_data(log_file,f"Table {target_db}.{target_schema}.{target_table} has {sf_cnt} rows in Snowflake and {src_cnt} rows in source",1)
				log_data(log_file,f"Attempting to invoke auto fix to see if delta can be fixed",0)
				cmnd=subprocess.Popen("/apps/edwsfdata/python/scripts/DIY_aud_fix.py -e " + env + " -j " + job_stream_id + " -a  sf-ingestion-audit@cisco.com -n " + icn ,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
				pid=cmnd.pid
				log_data(log_file,f"Audit fix script for JCT {job_stream_id} launched with pid {pid}",1)
				(output, error) = cmnd.communicate()
				Err=error.decode('ascii')
				Out=output.decode('ascii')
				if Err is not None and len(Err)>0:
					log_data(err_file,f"Exception encountered while running audit fix for {job_stream_id}. Please find below the exception \n {Err}",0)
				else:
					log_data(log_file,f"Completed launching audit fix for {job_stream_id}. Please find below the output \n {Out}",0)
				if ('Succesfull completion of audit fix for jct' in Out) or ('Unable to find any key which has data mis-match' in Out):
					sync_flag='N'
					autofix_flag='Y'
					sql="select end_delta_rows edr from diy_aud_auto_fix_filter where reqid in "
					sql+= f"(select max(reqid) from diy_aud_auto_fix_filter where job_stream_id='{job_stream_id}' and start_time > sysdate-1) "
					delta_obj=execute_df_qry(conn,sql,reqid,conn)
					end_delta_rows=delta_obj["edr"][0]
					if end_delta_rows is not None:
						sql=f"update EDW_AUDIT_MON set delta={end_delta_rows} where target_table='{target_table}' and job_group='{job_group}' and target_db='{target_db}'"
						execute_qry(conn,sql,reqid,conn)
						execute_qry(conn,'commit',reqid,conn)
						if perm_delta < end_delta_rows:
							sync_flag='Y'
			else:
				log_data(log_file,f"##Table {target_db}.{target_schema}.{target_table} has {sf_cnt} rows in Snowflake and {src_cnt} rows in source",0)
				sync_flag='N'
	step_name=f"Now Updating delta and out of sync flag for table {target_table}"
	log_data(log_file,f"At step {step_name}",0)
	sql=f"update EDW_AUDIT_MON set permissible_delta={perm_delta},out_of_sync_flag='{sync_flag}' where target_table='{target_table}' and job_group='{job_group}' and target_db='{target_db}'"
	execute_qry(conn,sql,reqid,conn)
	if sync_flag == 'Y' or autofix_flag == 'Y':
		sql=f"insert into edw_audit_mon_history(select a.*,'{autofix_flag}' from EDW_AUDIT_MON a where target_table='{target_table}' and job_group='{job_group}' and target_db='{target_db}')"
		execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,"commit",reqid,conn)
	conn.close()

try:
	env='PRD'
	thread_array=[]
	parallel_thread_count=5 ## launch 5 workers at a time
	reqid=str(datetime.now().strftime('%Y%m%d%H%M%S%f'))
	print(f'{reqid}')
	logs_dir=f"/apps/edwsfdata/python/logs/auditmon/{reqid}"
	if not os.path.exists(f'{logs_dir}'):
		os.mkdir(f'{logs_dir}')
	log_file=f"{logs_dir}/main_{reqid}.log"
	err_file=f"{logs_dir}/main_{reqid}.err"
	step_name='Connecting to repository'
	log_data(log_file,f"At step {step_name}",0)
	conn = open_oracle_connection(env,None,True)
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	step_name="Deleting current audit data"
	log_data(log_file,f"At step {step_name}",0)
	sql="delete from EDW_AUDIT_MON"
	execute_qry(conn,sql,reqid,conn)
	step_name="Populating current audit data"
	log_data(log_file,f"At step {step_name}",0)
	sql="insert  into EDW_AUDIT_MON (SOURCE_DB,SOURCE_SCHEMA,SOURCE_TABLE,TARGET_DB,TARGET_SCHEMA,TARGET_TABLE,SOURCE_COUNT,SNOWFLAKE_COUNT,DELTA,LOAD_TYPE,JOB_GROUP,"
	sql+="START_TIME,END_TIME,LOG_DATE,PERMISSIBLE_DELTA,OUT_OF_SYNC_FLAG)"
	sql+="(select /*+ parallel(8) */  SOURCE_DB,SOURCE_SCHEMA,SOURCE_TABLE,TARGET_DB,TARGET_SCHEMA,TARGET_TABLE,SOURCE_COUNT,SNOWFLAKE_COUNT,DELTA,LOAD_TYPE,JOB_GROUP,"
	sql+="START_TIME,END_TIME,trunc(sysdate) as log_date,0 as permissible_delta,'Y' as out_of_sync_flag from EDW_INGESTION_DATA_AUDIT a "
	sql+=" where load_type='INCREMENTAL' and delta <> 0 and (a.target_table,a.target_db) not in (select target_table_name,target_db_name from diy_audit_exception))"
	execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,"commit",reqid,conn)
	sql="select SOURCE_DB,SOURCE_SCHEMA,SOURCE_TABLE,TARGET_DB,TARGET_SCHEMA,TARGET_TABLE,SOURCE_COUNT,SNOWFLAKE_COUNT,DELTA,LOAD_TYPE,JOB_GROUP,START_TIME,END_TIME,LOG_DATE,PERMISSIBLE_DELTA,OUT_OF_SYNC_FLAG "
	sql+= " from EDW_AUDIT_MON order by source_count"
	results=execute_df_qry(conn,sql,reqid,conn)
	step_name="Now looping through tables to update permissible delta"
	log_data(log_file,f"At step {step_name}",0)
	for index,row in results.iterrows():
		job_group=row["job_group"]
		target_table=row["target_table"]
		target_schema=row["target_schema"]
		target_db=row["target_db"]
		curr_delta=abs(row["delta"])
		source_db=row["source_db"]
		source_schema=row["source_schema"]
		source_table=row["source_table"]
		thread_array.append(threading.Thread(target=derive_delta, args=(env,reqid,log_file,target_db,target_schema,target_table,job_group,curr_delta,source_db,source_schema,source_table)))
		## end of bucket for loop
	loop_cnt=1
	tot_work_count=len(thread_array)
	cum_loop_cnt=0
	ins_while_flag=False
	for worker in thread_array:
		tname=""
		log_data(log_file,f"Launching worker {cum_loop_cnt}",1)
		t=worker.start()
		time.sleep(5) ## sleep 5 seconds between each thread invocation
		#time.sleep(30) ## sleep 30 seconds between each thread invocation.
		main_thread = threading.currentThread()
		print_flag=True
		while loop_cnt >= parallel_thread_count:
			loop_cnt=threading.active_count()-1
			ins_while_flag=True
			for t in threading.enumerate():
				if t is main_thread:
					continue
				tname+=f"{t.getName()},"	
			tname=tname.replace('Thread-','')
			tname='Thread-' + tname
			if print_flag:
				log_data(log_file,f"{tname} threads are running",1)
				print_flag=False
			time.sleep(5) ## Sleep 5 seconds before checking curreng active threads
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
		else:
			loop_cnt+=1
	step_name="Re-conciling data in JIRA table for followup"
	log_data(log_file,f"At step {step_name}",0)
	sql="select target_db,target_schema,target_table from  EDW_AUDIT_MON where out_of_sync_flag='Y'"
	results=execute_df_qry(conn,sql,reqid,conn)
	for index,row in results.iterrows():
		target_db=row["target_db"]
		target_schema=row["target_schema"]
		target_table=row["target_table"]
		sql=f"select count(1) cnt from edw_audit_mon_jira where target_db='{target_db}' and target_schema='{target_schema}' and target_table='{target_table}'"
		cnt_obj=execute_df_qry(conn,sql,reqid,conn)
		cnt=cnt_obj["cnt"][0]
		if cnt == 0:
			sql=f"insert into edw_audit_mon_jira(target_db,target_schema,target_table,occurrence_cnt,first_reported_date,last_reported_date,resolved_flag) values "
			sql+=f"('{target_db}','{target_schema}','{target_table}',1,sysdate,sysdate,'N')"
			execute_qry(conn,sql,reqid,conn)
		else:
			sql=f"update edw_audit_mon_jira set occurrence_cnt=occurrence_cnt+1,last_reported_date=sysdate,resolved_flag='N' "
			sql+=f" where target_db='{target_db}' and target_schema='{target_schema}' and target_table='{target_table}'"
			execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,'commit',reqid,conn)
	conn.close()
	step_name="Generating and emailing report"
	log_data(log_file,f"At step {step_name}",0)
	cmnd=subprocess.Popen(f"/apps/edwsfdata/python/scripts/email_audit_report.py -e {env} -r {reqid} -a sf-ingestion-audit@cisco.com",stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	pid=cmnd.pid
	log_data(log_file,f"Report generation script launched with pid {pid}",1)
	(output, error) = cmnd.communicate()
	Err=error.decode('ascii')
	Out=output.decode('ascii')
	if (Err is not None and len(Err)>0) or ('Failure in line' in Out) :
		raise Exception(Out)

except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	log_data(err_file,f"Failure in line {linenu} at step {step_name}. Please find below exception",1)
	log_data(err_file,str(e),1)
	log_data(err_file,f"Last sql executed was :- {sql}",1)
					


