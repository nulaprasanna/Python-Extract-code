#!/apps/python/install/bin/python
#########################################################################################################################
# audit_functions.py                                                                                                    #
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Nov-2020                Initial Version                                                 #
# 1.1           Manick          Nov-2020                Drill down deep till min rows returned                          #
# 1.2           Manick          Nov-2020                Recurring drill till less number of rows found                  #
# 1.3           Manick          Nov-2020                Add new module to drill down by date                            #
# 1.4           Manick          Nov-2020                Add capability to pass column name for drilling                 #
# 1.5           Manick          Nov-2020                Add JCT filter if exists while comparing data                   #
# 1.6           Manick          Dec-2020                Include logic for TD                                            #
# 1.7           Manick          Jan-2021                Exclude current date while comparing delta                      #
# 1.8           Manick          Jan-2021                Pull distinct count from SF while choosing column for drill     #
# 1.9           Manick          Jan-2021                Use different sized WH based on tbl count                       #
# 1.10          Manick          Mar-2021                Include logic for Hana                                          #
# 1.11          Manick          Mar-2021                Move audit JCT to EDW_AUD_FIX while audit in progress           #
#########################################################################################################################
import argparse,pdb
import os, re, string, subprocess, sys
from datetime import datetime,date
from datetime import timedelta
import time
import faulthandler; faulthandler.enable()
import math
from metadata_handler import *
from operator import itemgetter, attrgetter
import numbers
from ConfigObject import ConfigObject

def validate_jct(repo_conn,jct,reqid,envType):
	try:
		jct_dict={}
		sql=f"select source_db_connection sdc from edw_job_streams where job_stream_id='{jct}' and active_ind='Y'" 
		jct_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(jct_obj) != 1:
			return(f"Invalid JCT {jct}")
		sdc = jct_obj["sdc"][0]
		jct_dict["sdc"]=sdc
		sql=f"select distinct parameter_category pc from eds_data_catalog.edw_parameter where parameter_type='{sdc}'"
		type_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(type_obj) != 1:
			return(f"Could not find source db type")
		db_type = type_obj["pc"][0]
		jct_dict["db_type"]=db_type
		sql=f"select source_db_connection sdc,job_stream_id js from edw_job_streams where job_stream_id_link='{jct}' and active_ind='Y'"
		brjct_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(brjct_obj) != 1:
			return(f"Could not locate BR JCT for SRC2STG JCT {jct}")
		tdc = brjct_obj["sdc"][0]
		jct_dict["tdc"]=tdc
		jct_dict["brjct"]=brjct_obj["js"][0]
		sql="select parameter_name pn,to_char(parameter_value) pv from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "'"
		sql+=f" and parameter_category='{db_type}' and parameter_type ='{sdc}'"
		sdc_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(sdc_obj) == 0:
			return(f"Could not query parameters for object {sdc}")
		for index,row in sdc_obj.iterrows():
			if row["pn"] == "SOURCE_LOGIN_PASSWORD":
				sql="SELECT UTL_I18N.RAW_TO_CHAR('"+ row["pv"] +"','AL32UTF8') pv from dual"
				pass_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
				jct_dict[row["pn"]]=pass_obj["pv"][0]
			else:
				jct_dict[row["pn"]]=row["pv"]
		return(jct_dict)
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred at line {linenu} inside validate_jct module. \n")

def fetch_sf_conn_values(tdc,envType,repo_conn,reqid,brjct,jct):
	try:
		jct_dict={}
		sql="select parameter_name pn,to_char(parameter_value) pv from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "'"
		sql+=f" and parameter_type ='{tdc}'"
		sdc_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(sdc_obj) == 0:
			return(f"Could not query parameters for object {sdc}")
		for index,row in sdc_obj.iterrows():
			if row["pn"] == "SOURCE_LOGIN_PASSWORD":
				sql="SELECT UTL_I18N.RAW_TO_CHAR('"+ row["pv"] +"','AL32UTF8') pv from dual"
				pass_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
				jct_dict[row["pn"]]=pass_obj["pv"][0]
			else:
				jct_dict[row["pn"]]=row["pv"]
		sql=f"select sf_role,sf_warehouse,target_db_name,target_schema,target_table_name from edw_job_streams where job_stream_id='{brjct}'"
		brjct_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		jct_dict["sf_role"]=brjct_obj["sf_role"][0]
		jct_dict["sf_warehouse"]=brjct_obj["sf_warehouse"][0]
		jct_dict["target_db_name"]=brjct_obj["target_db_name"][0]
		jct_dict["target_schema"]=brjct_obj["target_schema"][0]
		jct_dict["target_table_name"]=brjct_obj["target_table_name"][0]
		sql=f"select max(reqid) jreqid from diy_master where attribute3='{jct}'"
		jreq_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		jreq=jreq_obj["jreqid"][0]
		tcnt=0
		if len(jreq_obj) > 0:
			sql=f"select target_count from diy_soft_delete where reqid={jreq}"
			tcnt_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
			if len(tcnt_obj) > 0:
				tcnt=tcnt_obj["target_count"][0]
		if "_EXT" not in jct_dict["sf_warehouse"]:
			if tcnt > 100000000:
				jct_dict["sf_warehouse"]=jct_dict["sf_warehouse"].replace("_WH","_EXT3_WH")
			elif tcnt > 10000000:
				jct_dict["sf_warehouse"]=jct_dict["sf_warehouse"].replace("_WH","_EXT2_WH")
			elif tcnt > 1000000:
				jct_dict["sf_warehouse"]=jct_dict["sf_warehouse"].replace("_WH","_EXT1_WH")
				
		return(jct_dict)
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred inside fetch_sf_conn_values module at line {linenu}.\n")

def upd_aud_job_group(repo_conn,jct,phase,reqid):
	if phase == 'START':
		sql=f"select job_group_id,run_status from edw_job_streams where job_stream_id='{jct}'"
		group_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		job_group_id=group_obj["job_group_id"][0]
		run_status=group_obj["run_status"][0]
		if run_status != 'C':
			raise Exception(f"Invalid run_status '{run_status}' for jct {jct}.. Hence not proceeding with audit fix")
		sql=f"update DIY_AUD_AUTO_FIX_FILTER set job_group='{job_group_id}' where reqid={reqid}"
		execute_qry(repo_conn, sql, reqid, repo_conn)
		sql=f"update edw_job_streams set job_group_id='EDW_AUD_FIX' where (job_stream_id='{jct}' or job_stream_id_link='{jct}') and active_ind='Y'"
		execute_qry(repo_conn, sql, reqid, repo_conn)
		execute_qry(repo_conn, "commit", reqid, repo_conn)
	else:
		sql=f"select job_group from DIY_AUD_AUTO_FIX_FILTER  where reqid={reqid}"
		group_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		job_group_id=group_obj["job_group"][0]
		sql=f"update edw_job_streams set job_group_id='{job_group_id}' where (job_stream_id='{jct}' or job_stream_id_link='{jct}') and active_ind='Y'"
		execute_qry(repo_conn, sql, reqid, repo_conn)
		execute_qry(repo_conn, "commit", reqid, repo_conn)



def fetch_aud_count(repo_conn,tgt_tbl,reqid,jct):
	try:
		sql=f"select delta from EDW_INGESTION_DATA_AUDIT where target_db || '.' || target_schema || '.' || target_table ='{tgt_tbl}'"
		aud_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(aud_obj) == 0:
			return(f"Data doesnt exist in audit view for table {tgt_tbl}")
		aud_cnt=aud_obj["delta"][0]
		if aud_cnt == 0:
			return(f"Audit is perfect match for table {tgt_tbl}")
		pattern_followed=True
		init_aud=aud_cnt
		sql=f"select distinct delta,creation_date cd from edw_ingestion_audit_log where target_db || '.' || target_schema || '.' || target_table ='{tgt_tbl}'  and creation_date>sysdate-8 order by creation_date desc"
		hist_dict={}
		less_flag=False
		more_flag=False
		equal_flag=False
		hist_delta=[]
		aud_hist_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		for index,row in aud_hist_obj.iterrows():
			hist_delta.append(row["delta"])
			his_del=row["delta"]
			if init_aud < his_del:
				less_flag=True
				init_aud=his_del
				if more_flag:
					pattern_followed=False
			elif init_aud > his_del:
				more_flag=True
				init_aud=his_del
				if less_flag:
					pattern_followed=False
			else:
				equal_flag=True
				init_aud=his_del
			hist_dict[row["cd"]]=row["delta"]
		#if not pattern_followed:
		#	return(f"History of delta record count doesnt follow pattern. Goes up and down. Plese check..\n {hist_dict} \n")
		#else:
		sql="insert into diy_aud_auto_fix_filter(reqid,job_stream_id,start_time,start_delta_rows) values "
		sql+=f"({reqid},'{jct}',sysdate,{aud_cnt})"
		execute_qry(repo_conn, sql, reqid, repo_conn)
		execute_qry(repo_conn, "commit", reqid, repo_conn)
		return(aud_cnt)
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred inside fetch_aud_count module at line {linenu}.\n")

def check_sched_stat(repo_conn,jct,reqid):
	try:
		sql=f"select count(1) cnt from diy_master where start_time>=(select max(start_time) st from diy_master where attribute3='{jct}') and attribute3='{jct}' and end_time is null"
		curr_run_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		curr_run_cnt=curr_run_obj["cnt"][0]
		if curr_run_cnt > 0:
			return(f"Load for Job stream {jct} is currently in progress.. Hence cannot apply audit fix")
		sql=f"select * from (select  to_char(start_time,'mm/dd/yyyy hh24:mi:ss') start_time from diy_master where attribute3='{jct}' and attribute1='root' order by to_date(start_time,'mm/dd/yyyy hh24:mi:ss') desc) where rownum <=4"
		hist_run_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		run1=""
		run2=""
		interval=[]
		for index,row in hist_run_obj.iterrows():
			if len(run1) == 0:
				run1 = row["start_time"]
			else:
				run2=row["start_time"]
				sql=f"select (to_date('{run1}','mm/dd/yyyy hh24:mi:ss')-to_date('{run2}','mm/dd/yyyy hh24:mi:ss'))*24*60 inter from dual"
				inter_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
				interval.append(inter_obj["inter"][0])
				run1=run2
		if len(interval) > 0:
			interval=sum(interval)/len(interval)
		else:
			return(f"Could not find history of executions for job stream {jct} from DIY_Master. Please check")
		sql=f"select to_char(max(start_time),'mm/dd/yyyy hh24:mi:ss') st from diy_master where attribute3='{jct}'"
		last_run_time_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		last_run_time=last_run_time_obj["st"][0]
		sql=f"select round((to_date('{last_run_time}','mm/dd/yyyy hh24:mi:ss')+{interval}/24/60-sysdate)*24*60,0) del from dual"
		run_delta_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		run_delta=run_delta_obj["del"][0]
		if run_delta <= 15:
			return(f"Next scheduled run for jct {jct} happens within next 15 minutes.. Hence attempt to fix audit issue later")
		else:
			return("Pass")
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred inside check_sched_stat module at line {linenu}.\n")

def trig_ingestion_fw(repo_conn,jct,reqid,envType,emailAdd,brjct):
	try:
		sql=f"update edw_job_streams set run_status='P' where job_stream_id in ('{jct}','{brjct}')"
		execute_qry(repo_conn, sql, reqid, repo_conn)
		execute_qry(repo_conn, 'Commit', reqid, repo_conn)
		oscmd="/apps/edwsfdata/python/scripts/DIY_src2stg.py"
		cmnd=subprocess.Popen(f"{oscmd} -i jct -e {envType} -j {jct} -a {emailAdd} ",stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		pid=cmnd.pid
		(output, error) = cmnd.communicate()
		retcode=cmnd.returncode
		Err=error.decode('ascii')
		Out=output.decode('ascii')
		if retcode != 0:
			return(f"Ingestion Framework resulted in return code {retcode} . Please find below the exception \n {Err} \n {Out}")
		else:
			sql=f"select count(*) cnt from edw_job_streams where job_stream_id in ('{jct}','{brjct}') and run_status <> 'C'"
			job_stat_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
			job_stat_cnt=job_stat_obj["cnt"][0]
			if job_stat_cnt == 0:
				return("Success")
			else:
				return("Job stream status not in completed status. Unknown error occurred. Please check the logs")
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred inside trig_ingestion_fw module at line {linenu}.\n")

def drill_delta(comp_col_name,inter,src_tbl,sf_tbl,sf_conn,src_conn,reqid,repo_conn,filter,categ,where_clause):
	global source_mod_obj,sf_mod_obj,mod_array,rows2_pull,db_type_local
	source_dict={}
	sf_dict={}
	if db_type_local == 'ORACLE':
		parallel_clause = " /*+ parallel(8) */ "
	else:
		parallel_clause=""
	#mod_array=[]
	currdate=date.today().strftime("%d/%m/%Y")
	if categ == 'DATE':
		if len(filter) == 0:
			if len(where_clause) == 0:
				source_sql=f"select {parallel_clause} to_char({comp_col_name},'{inter}') mv,cast(count(*) as number) cnt from {src_tbl} a where {comp_col_name} < to_date('{currdate}','dd/mm/yyyy') group by to_char({comp_col_name},'{inter}')  order by 1"
				sf_sql=f"select  to_char({comp_col_name},'{inter}') mv,count(*) cnt from {sf_tbl} a where nvl(edwsf_source_deleted_flag,'N')='N' and {comp_col_name} < to_date('{currdate}','dd/mm/yyyy') group by to_char({comp_col_name},'{inter}') order by 1"
			else:
				source_sql=f"select {parallel_clause} to_char({comp_col_name},'{inter}') mv,cast(count(*) as number) cnt from {src_tbl} a where {where_clause} and {comp_col_name} < to_date('{currdate}','dd/mm/yyyy') group by to_char({comp_col_name},'{inter}')  order by 1"
				sf_sql=f"select  to_char({comp_col_name},'{inter}') mv,count(*) cnt from {sf_tbl} a where nvl(edwsf_source_deleted_flag,'N')='N' and {where_clause} and {comp_col_name} < to_date('{currdate}','dd/mm/yyyy') group by to_char({comp_col_name},'{inter}') order by 1"
		else:
			filter += f" and {comp_col_name} < to_date('{currdate}','dd/mm/yyyy')"
			if len(where_clause) > 0:
				filter += f" and {where_clause}"
			source_sql=f"select {parallel_clause} to_char({comp_col_name},'{inter}') mv,cast(count(*) as number) cnt from {src_tbl} a where {filter} group by to_char({comp_col_name},'{inter}')  order by 1"
			sf_sql=f"select  to_char({comp_col_name},'{inter}') mv,count(*) cnt from {sf_tbl} a where {filter} and nvl(edwsf_source_deleted_flag,'N')='N' group by to_char({comp_col_name},'{inter}') order by 1"
	else:
		if len(filter) == 0:
			if len(where_clause) == 0:
				source_sql=f"select {parallel_clause} round(({comp_col_name})/{inter}) mv,cast(count(*) as number) cnt from {src_tbl} a group by round(({comp_col_name})/{inter})  order by 1"
				sf_sql=f"select  round(({comp_col_name})/{inter}) mv,count(*) cnt from {sf_tbl} a where nvl(edwsf_source_deleted_flag,'N')='N' group by round(({comp_col_name})/{inter}) order by 1"
			else:
				source_sql=f"select {parallel_clause} round(({comp_col_name})/{inter}) mv,cast(count(*) as number) cnt from {src_tbl} a where {where_clause} group by round(({comp_col_name})/{inter})  order by 1"
				sf_sql=f"select  round(({comp_col_name})/{inter}) mv,count(*) cnt from {sf_tbl} a where nvl(edwsf_source_deleted_flag,'N')='N' and {where_clause} group by round(({comp_col_name})/{inter}) order by 1"
		else:
			if len(where_clause) > 0:
				filter += f" and {where_clause}"
			source_sql=f"select {parallel_clause} round(({comp_col_name})/{inter}) mv,cast(count(*) as number) cnt from {src_tbl} a where {filter} group by round(({comp_col_name})/{inter})  order by 1"
			sf_sql=f"select  round(({comp_col_name})/{inter}) mv,count(*) cnt from {sf_tbl} a where {filter} and nvl(edwsf_source_deleted_flag,'N')='N' group by round(({comp_col_name})/{inter})  order by 1"
	source_mod_obj=execute_df_qry(src_conn, source_sql, reqid, repo_conn)
	sf_mod_obj=execute_df_qry(sf_conn, sf_sql, reqid, repo_conn)
	#rows2_pull=0

	for index,row in source_mod_obj.iterrows():
		if db_type_local == 'HANA':
			source_dict[row["mv"]]=int(row["cnt"])
		else:
			if categ == 'NUMBER':
				source_dict[row["mv"].astype("int")]=row["cnt"].astype("int")
			else:
				source_dict[row["mv"]]=row["cnt"]
	for index,row in sf_mod_obj.iterrows():
		if categ == 'NUMBER':
			sf_dict[row["mv"].astype("int")]=row["cnt"].astype("int")
		else:
			sf_dict[row["mv"]]=row["cnt"]
	for obj in source_dict:
		if obj in sf_dict:
			if source_dict[obj] > sf_dict[obj]:
				#mod_array.append(f"{obj}~{source_dict[obj]}")
				if categ == 'DATE':
					filter=f"to_char({comp_col_name},'{inter}') = {obj}"
				else:
					filter=f"round(({comp_col_name})/{inter}) = {obj} "
				mod_array.append(f"{obj}~{inter}~{filter}~True")
				rows2_pull+=source_dict[obj]
		else:
			#mod_array.append(f"{obj}~{source_dict[obj]}")
			if categ == 'DATE':
				filter=f"to_char({comp_col_name},'{inter}') = {obj}"
			else:
				filter=f"round(({comp_col_name})/{inter}) = {obj} "
			mod_array.append(f"{obj}~{inter}~{filter}~False")
			rows2_pull+=source_dict[obj]
	return(mod_array)
	
def prep_jct_4_delta_load(repo_conn,jct,reqid,log_file,err_file,envType,sf_conn,src_conn,aud_count,sf_tbl,colType,db_type):
	try:
		global mod_array,rows2_pull,db_type_local
		db_type_local=db_type
		rows2_pull=0
		col_list=list()
		mod_array_pers=[]
		comp_col_name=""
		sql=f"select where_clause wc,source_schema sc,source_table_name sto from edw_job_streams where job_stream_id='{jct}'"
		src_tbl_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		#src_tbl=src_tbl_obj["st"][0]
		where_clause=src_tbl_obj["wc"][0]
		if where_clause is None:
			where_clause=''
		src_tbl_name=src_tbl_obj["sto"][0]
		src_schema_name=src_tbl_obj["sc"][0]
		src_tbl=f'"{src_schema_name}"."{src_tbl_name}"'
		if "~" in colType:
			categ=colType.split('~')[0]
			comp_col_name=colType.split('~')[1]
		else:
			categ=colType
			if db_type == 'HANA':
				if categ == 'DATE':
					sql=f"select column_name cn from sys.table_columns where table_name='{src_tbl_name}' and schema_name='{src_schema_name}' and data_type_name in ('SECONDDATE','DATE','TIMESTAMP')"
					col_obj_list=execute_df_qry(src_conn, sql, reqid, repo_conn)
					dist_cnt=0
					for index,row in col_obj_list.iterrows():
						colname=row['cn']
						sql=f'select count(distinct {colname}) cnt from "{src_schema_name}"."{src_tbl_name}"'
						col_count_obj=execute_df_qry(src_conn, sql, reqid, repo_conn)
						col_count=col_count_obj["cnt"][0]
						if col_count > dist_cnt:
							dist_cnt=col_count
							comp_col_name=colname
				elif categ == 'NUMBER' or 'INT' in categ:
					sql=f"select column_name cn from sys.table_columns where table_name='{src_tbl_name}' and schema_name='{src_schema_name}' and data_type_name in ('BIGINT','INTEGER','TINYINT','SMALLINT')"
					col_obj_list=execute_df_qry(src_conn, sql, reqid, repo_conn)
					dist_cnt=0
					for index,row in col_obj_list.iterrows():
						colname=row['cn']
						sql=f'select count(distinct {colname}) cnt from "{src_schema_name}"."{src_tbl_name}"'
						col_count_obj=execute_df_qry(src_conn, sql, reqid, repo_conn)
						col_count=col_count_obj["cnt"][0]
						if col_count > dist_cnt:
							dist_cnt=col_count
							comp_col_name=colname
			else:
				if db_type == 'ORACLE':
					sql=f"select column_name cn from all_tab_columns where table_name='{src_tbl_name}' and owner='{src_schema_name}' and data_type='{categ}' order by num_distinct desc"
				elif db_type == 'TERADATA':
					if categ == 'DATE':
						sql=f"select a.columnname cn from dbc.columnsv a left outer join dbc.statsv b on  a.columnname=b.columnname and a.databasename=b.databasename and a.tablename=b.tablename where a.tablename='{src_tbl_name}' and a.databasename='{src_schema_name}' and a.columntype in ('DA','TS','TZ') order by nvl(b.uniquevaluecount,0) desc"
					if categ == 'NUMBER' or 'INT' in categ:
						sql=f"select a.columnname cn from dbc.columnsv a left outer join dbc.statsv b on  a.columnname=b.columnname and a.databasename=b.databasename and a.tablename=b.tablename where a.tablename='{src_tbl_name}' and a.databasename='{src_schema_name}' and a.columntype in ('I','I8','I1','I2','D') order by nvl(b.uniquevaluecount,0) desc"
				col_obj=execute_df_qry(src_conn, sql, reqid, repo_conn)
				if len(col_obj) == 0:
					return(f"No valid {categ} column found to drill finding delta")
				comp_col_name=col_obj["cn"][0]
		start_ind=100
		rows2_pull=0
		if categ == 'NUMBER' or 'INT' in categ:
			sql=f"select round((max({comp_col_name})-min({comp_col_name}))/100) ave from {sf_tbl} a where nvl(edwsf_source_deleted_flag,'N')='N'"
			max_len_obj=execute_df_qry(sf_conn, sql, reqid, repo_conn)
			inter=max_len_obj["ave"][0]
			iter_count=10
		else:
			inter='YYYY'
			iter_count=5
		counter=1
		mod_val=100
		filter=""
		new_filter="("
		mod_array=[]
		mod_array_pers=drill_delta(comp_col_name,inter,src_tbl,sf_tbl,sf_conn,src_conn,reqid,repo_conn,filter,categ,where_clause)
		mod_array=[]
		#print(str(rows2_pull))
		log_data(log_file,f"{mod_array_pers} will retrieve {rows2_pull} for fixing {aud_count}",1)
		loop_count=0
		prev_rows2_pull=-1
		while (rows2_pull/aud_count >=3 and rows2_pull > 500000 and loop_count<iter_count  and len(mod_array_pers) < 100 ):
			prev_rows2_pull=rows2_pull
			rows2_pull=0
			mod_array=[]
			new_filter="("
			if categ == 'DATE':
				if loop_count == 0:
					inter="YYYYMM"
				if loop_count == 1:
					inter="YYYYMMDD"
				if loop_count == 2:
					inter="YYYYMMDDHH24"
				if loop_count == 3:
					inter="YYYYMMDDHH24MI"
				if loop_count == 4:
					inter="YYYYMMDDHH24MISS"
			for counter in range(len(mod_array_pers)):
				if categ == 'NUMBER':
					round_val=int(float(mod_array_pers[counter].split('~')[0]))
					count_val=int(float(mod_array_pers[counter].split('~')[1]))
				filter=mod_array_pers[counter].split('~')[2]
				sf_has_data=mod_array_pers[counter].split('~')[3]
				if sf_has_data == 'True':
					if categ == 'NUMBER':
						sql=f"select decode(round((max({comp_col_name})-min({comp_col_name}))/100),0,2,round((max({comp_col_name})-min({comp_col_name}))/100)) ave from {sf_tbl} where  {filter} and  nvl(edwsf_source_deleted_flag,'N')='N'"
						avg_obj=execute_df_qry(sf_conn, sql, reqid, repo_conn)
						new_inter=avg_obj["ave"][0]
					else:
						new_inter=inter
					mod_temp_array=drill_delta(comp_col_name,new_inter,src_tbl,sf_tbl,sf_conn,src_conn,reqid,repo_conn,filter,categ,where_clause)
				else:
					mod_array.append(mod_array_pers[counter])
				
				new_filter+=f" {filter} or "
			#print(str(rows2_pull))
			log_data(log_file,f"{mod_array} will retrieve {rows2_pull} for fixing {aud_count}",1)
			mod_array_pers=mod_array
			loop_count+=1
				
		if len(mod_array_pers) == 0:
			return("Unable to find any key which has data mis-match between source and SF")
			sql=f"update diy_aud_auto_fix_filter set status='Unable to find any key which has data mis-match between source and SF',end_time=sysdate where reqid={reqid} and job_stream_id='{jct}'"
			execute_qry(repo_conn, sql, reqid, repo_conn)
			execute_qry(repo_conn, "commit", reqid, repo_conn)
		else:
			new_filter="("
			filter="("
			for counter in range(len(mod_array_pers)):
				round_val=mod_array_pers[counter].split('~')[0]
				count_val=mod_array_pers[counter].split('~')[1]
				filter2=mod_array_pers[counter].split('~')[2]
				filter_mod_part=filter2.split("=")[0]
				if filter_mod_part in filter2:
					if f"{filter_mod_part} in" in filter:
						filter+=f',{filter2.split("=")[1]}'
					else:
						if filter == "(":
							filter+=filter2.replace(f"{filter_mod_part}=",f"{filter_mod_part} in (")
						else:
							filter+=f') or '
							filter+=filter2.replace(f"{filter_mod_part}=",f"{filter_mod_part} in (")
				else:
					filter += f"{filter2} or "
			filter+="))"
			upd_filter=filter.replace("'","''")
			sql=f"update diy_aud_auto_fix_filter set filter='{upd_filter}',proj_rows_to_pull={rows2_pull},analyze_end_time=sysdate where reqid={reqid} and job_stream_id='{jct}'"
			execute_qry(repo_conn, sql, reqid, repo_conn)
			execute_qry(repo_conn, "commit", reqid, repo_conn)
			if rows2_pull> 10000000 and rows2_pull/aud_count > 3:
				sql=f"update diy_aud_auto_fix_filter set filter='{upd_filter}',proj_rows_to_pull={rows2_pull},status='Too much data projected to be pulled for fixing',end_time=sysdate where reqid={reqid} and job_stream_id='{jct}'"
				execute_qry(repo_conn, sql, reqid, repo_conn)
				execute_qry(repo_conn, "commit", reqid, repo_conn)
				return("Projected data volume for fixing audit is way too much than current audit.. Look at using different column")
			log_data(log_file,f"Filter {filter} can be applied to pull {rows2_pull} rows for fixing a difference of {aud_count} rows",1)
			return(f'Success~{filter}')
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred inside prep_jct_4_delta_load module at line {linenu}.\n")


def upd_jct_4_with_filter(repo_conn,jct,reqid,vfilter,brjct,envType,emailAdd,target_fqn,log_file):
	try:
		filter=vfilter.replace("'","''")
		sql=f"select extract_type,where_clause from edw_job_streams where job_stream_id='{jct}' and active_ind='Y' and run_status='C' and extract_type not in ('ALL_DATA')"
		jct_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(jct_obj) == 0:
			sql=f"update diy_aud_auto_fix_filter set status='Unable to query JCT for running filtered load',end_time=sysdate where reqid={reqid} and job_stream_id='{jct}'"
			execute_qry(repo_conn, sql, reqid, repo_conn)
			execute_qry(repo_conn, "commit", reqid, repo_conn)
		else:
			orig_ext_type=jct_obj["extract_type"][0]
			orig_filter=jct_obj["where_clause"][0]
			if orig_filter is not None:
				orig_filter=orig_filter.replace("'","''")
			log_data(log_file,"Setting filter for filtered data load",0)
			if orig_filter is None:
				sql=f"update edw_job_streams set extract_type='ALL_DATA',where_clause='{filter}' where job_stream_id='{jct}' "
			else:
				sql=f"update edw_job_streams set extract_type='ALL_DATA',where_clause='{filter} and {orig_filter}' where job_stream_id='{jct}' "
			execute_qry(repo_conn, sql, reqid, repo_conn)
			execute_qry(repo_conn, 'commit', reqid, repo_conn)
			log_data(log_file,"Start of filtered data load",0)
			out=trig_ingestion_fw(repo_conn,jct,reqid,envType,emailAdd,brjct)
			if out != 'Success':
				sql=f"update diy_aud_auto_fix_filter set status='Filtered load execution of JCT failed',end_time=sysdate where reqid={reqid} and job_stream_id='{jct}'"
				execute_qry(repo_conn, sql, reqid, repo_conn)
				execute_qry(repo_conn, "commit", reqid, repo_conn)
				raise Exception(out)
			sql=f"update diy_aud_auto_fix_filter set  filtered_data_load_et=sysdate where reqid={reqid} and job_stream_id='{jct}'"
			execute_qry(repo_conn, sql, reqid, repo_conn)
			execute_qry(repo_conn, "commit", reqid, repo_conn)
			log_data(log_file,"Resetting filter settings for JCT",0)
			if orig_filter is None:
				sql=f"update edw_job_streams set extract_type='{orig_ext_type}',where_clause=Null where job_stream_id='{jct}' "
			else:
				sql=f"update edw_job_streams set extract_type='{orig_ext_type}',where_clause='{orig_filter}' where job_stream_id='{jct}'"
			execute_qry(repo_conn, sql, reqid, repo_conn)
			execute_qry(repo_conn, 'commit', reqid, repo_conn)
			log_data(log_file,"Running incremental to update audit dashboard",0)
			out=trig_ingestion_fw(repo_conn,jct,reqid,envType,emailAdd,brjct)
			if out != 'Success':
				sql=f"update diy_aud_auto_fix_filter set status='Inc load post audit fix failed',end_time=sysdate where reqid={reqid} and job_stream_id='{jct}'"
				execute_qry(repo_conn, sql, reqid, repo_conn)
				execute_qry(repo_conn, "commit", reqid, repo_conn)
				raise Exception(out)
			sql=f"update diy_aud_auto_fix_filter set  final_data_load_et=sysdate where reqid={reqid} and job_stream_id='{jct}'"
			execute_qry(repo_conn, sql, reqid, repo_conn)
			execute_qry(repo_conn, "commit", reqid, repo_conn)
			log_data(log_file,"Pulling audit delta post filtered load",0)
			#sql=f"select delta from EDW_INGESTION_DATA_AUDIT where target_db || '.' || target_schema || '.' || target_table ='{target_fqn}'"
			sql=f"select target_count-source_count-nvl(soft_del_count,0) delta from diy_soft_delete where target_table ='{target_fqn}' and reqid=("
			sql+=f"select max(reqid) from diy_soft_delete where target_table ='{target_fqn}')"
			aud_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
			aud_cnt=aud_obj["delta"][0]
			sql=f"update diy_aud_auto_fix_filter set status='Success',end_time=sysdate,end_delta_rows={aud_cnt} where reqid={reqid} and job_stream_id='{jct}'"
			execute_qry(repo_conn, sql, reqid, repo_conn)
			execute_qry(repo_conn, 'commit', reqid, repo_conn)
			return('Success')
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred inside upd_jct_4_with_filter module at line {linenu}.\n")

def validate_column(envType,repo_conn,reqid,jct,colName):
	try:
		sql=f"select data_type  from eds_data_catalog.edw_table_column a,edw_job_streams b where b.job_stream_id='{jct}' and "
		sql+=f" a.db_instance_name=b.source_db_name and a.db_schema_name=b.source_schema and a.table_name=b.source_table_name"
		sql+=f" and a.column_name='{colName}' and a.environment_name='{envType}'"
		col_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(col_obj) > 0:
			data_type=col_obj["data_type"][0]
			if 'TIMESTAMP' in data_type.upper():
				data_type='DATE'
			return(f"Success~{data_type}")
		else:
			return(f"Column {colName} could not be located on table associated with JCT {jct}")
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred inside validate_column module at line {linenu}.\n")

