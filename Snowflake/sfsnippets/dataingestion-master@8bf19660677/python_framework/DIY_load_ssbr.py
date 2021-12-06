#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_load_ssbr.py                                                                                                      #
# Script to perform load data from stage to SS/BR layer                                                                 #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Feb-2020                Initial Version                                                 #
# 1.1           Manick          Apr-2020                use new PL/SQL functions for generating sqls per 6.0 change     #
# 1.2           Manick          May-2020                Enable HANANRT                                                  #
# 1.3           Manick          Jun-2020                Enable Delete before Insert     for Append type load            #
# 1.4           Manick          Sep-2020                Integrate with attuinity trail file upload merge                #
# 1.5           Manick          Apr-2021                Enable delete before merging to handle MT scenario              #
# 1.6           Manick          Apr-2021                Check warehouse to be used instead of getting from JCT          #
# 1.7           Manick          Apr-2021                Use small wh for non merge type workflows                       #
# 1.8           Manick          May-2021                Use transaction during delete and insert for consistent read    #
# 1.9           Manick          Jun-2021                New version with additional comments for easy readability       #
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

## Method to update run status in EDW_JOB_STREAMS table

def upd_jct_status(run_status):
	global conn,jct,reqid,step_name,log_file,step_name
	step_name=f"Updating edw_job_stream status to {run_status}"
	log_data(log_file,step_name,0)
	sql=f"update edw_job_streams set run_status='{run_status}' where job_stream_id='{jct}'"
	execute_qry(conn, sql, reqid, conn)
	execute_qry(conn,"commit",reqid,conn)

## Method to make entry in EDW_JOB_STREAM_RUNS table

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

## Method to generate target DML statement (Insert/Update)

def modify_br_data(load_type):
	global conn,jct,reqid,stgtbl,sstbl,conn3,step_name,log_file,env,src_base_schema,src_base_tbl,db_type,source_table_name,source_schema,source_db_name,stg_jct
	global target_db,target_table,target_schema
	sql="alter session set autocommit=false"
	execute_qry(conn3,sql,reqid,conn)
	sql=f"select workflow_type wt from edw_job_streams where job_stream_id='{stg_jct}'"
	wt_obj=execute_df_qry(conn, sql, reqid, conn)
	workflow_type=wt_obj["wt"][0]
	if db_type != 'HANANRT':
		if load_type == "OVERWRITE":
			step_name=f"Now deleting {sstbl}"
			sql="begin transaction"
			execute_qry(conn3,sql,reqid,conn)
			log_data(log_file,step_name,0)
			sql=f"delete from {sstbl}"
			execute_qry(conn3,sql,reqid,conn)
			step_name=f"Now generating insert statement"
			log_data(log_file,step_name,0)
			sql=f"SELECT EDS_DATA_CATALOG.FN_GET_INSERT_STMT_NEW"
			sql+=f"('{env}',SOURCE_DB_NAME,SOURCE_SCHEMA,SOURCE_TABLE_NAME,SF_SOURCE_DB_NAME,SF_SOURCE_SCHEMA,SF_SOURCE_TABLE_NAME,"
			sql+=f"SF_TARGET_DB_NAME,SF_TARGET_SCHEMA,SF_TARGET_TABLE_NAME)||CHR(10) AS PARAM_STRING"
			sql+=f" FROM "
			sql+=f"(SELECT SOURCE_DB_NAME,SOURCE_SCHEMA,SOURCE_TABLE_NAME FROM EDW_JOB_STREAMS "
			sql+=f" WHERE JOB_STREAM_ID =(SELECT JOB_STREAM_ID_LINK FROM EDW_JOB_STREAMS WHERE JOB_STREAM_ID = '{jct}')) SRC,"
			sql+=f" (SELECT SOURCE_DB_NAME AS SF_SOURCE_DB_NAME,SOURCE_SCHEMA AS SF_SOURCE_SCHEMA,SOURCE_TABLE_NAME AS SF_SOURCE_TABLE_NAME,"
			sql+=f" TARGET_DB_NAME AS SF_TARGET_DB_NAME,TARGET_SCHEMA AS SF_TARGET_SCHEMA,TARGET_TABLE_NAME AS SF_TARGET_TABLE_NAME,MERGE_TYPE AS SF_MERGE_TYPE"
			sql+=f" FROM EDW_JOB_STREAMS WHERE JOB_STREAM_ID = '{jct}') SF"
			ss_ins_obj=execute_df_qry(conn, sql, reqid, conn)
			ss_ins_sql=str(ss_ins_obj["param_string"][0])
			log_data(log_file,ss_ins_sql,0)
			step_name=f"Now inserting into {sstbl}"
			log_data(log_file,step_name,0)
			execute_qry(conn3,ss_ins_sql,reqid,conn)
			execute_qry(conn3,"commit",reqid,conn)
		elif load_type == "MERGE_ONE2ONE":
			step_name=f"Generating merge statement for {sstbl}"
			log_data(log_file,step_name,0)
			if workflow_type == 'FF2STG':
				sql=f"SELECT upper(eukc.column_name) cn FROM eds_data_catalog.edw_unique_key_column eukc, eds_data_catalog.edw_unique_key euk"
				sql+=f" where  eukc.environment_name = euk.environment_name  and eukc.db_instance_name = euk.db_instance_name"
				sql+=f" and eukc.db_schema_name = euk.db_schema_name and eukc.table_name = euk.table_name and eukc.unique_key_name = euk.unique_key_name"
				sql+=f" AND euk.environment_name  = '{env}'  AND euk.db_instance_name  = '{source_db_name}' AND euk.db_schema_name = '{src_base_schema}'"
				sql+=f" AND euk.table_name = '{src_base_tbl}' and euk.active_flag = 'A'"
				sql+=f" ORDER BY eukc.column_position"
				uk_obj=execute_df_qry(conn, sql, reqid, conn)
				if len(uk_obj) == 0:
					raise Exception(f"Unique key could not be located in repos for {source_db_name}.{src_base_schema}.{src_base_tbl} in env {env}")
				join_clause=""
				uk_col_list=""
				for index,row in uk_obj.iterrows():
					join_clause+=f"nvl(nullif(SRC.\"{row['cn']}\",''),'0')=nvl(TGT.\"{row['cn']}\",'0') AND "
					uk_col_list+=f"{row['cn']},"
				join_clause=join_clause[:-4] ## remove last and
				uk_col_list=uk_col_list[:-1] ## remove last comma
				sql=f"SELECT '\"' || upper(etc.column_name) || '\"' cn, etc.nullable_flag nf, etc.data_type dt FROM eds_data_catalog.edw_table_column etc WHERE"
				sql+=f" etc.environment_name = '{env}' and etc.db_instance_name = '{source_db_name}' and etc.db_schema_name = '{src_base_schema}'"
				sql+=f" and etc.table_name = '{src_base_tbl}' and etc.active_flag ='A' ORDER BY etc.column_sequence"
				tbl_col_obj=execute_df_qry(conn, sql, reqid, conn)
				if len(tbl_col_obj) == 0:
					raise Exception(f"Table columns could not be queried in repos for {source_db_name}.{src_base_schema}.{src_base_tbl} in env {env}")
				sel_col_list=""
				src_col_list=""
				tgt_col_list=""
				upd_col_list=""
				batch_id=str(datetime.now().strftime('%Y%m%d%H%M%S'))
				rownum_col=',ROW_NUMBER() OVER (PARTITION BY '
				for index,row in tbl_col_obj.iterrows():
					col_name=row["cn"]
					null_flag=row["nf"]
					data_type=row["dt"]
					if null_flag == 'Y':
						sel_col_name=f"NULLIF({col_name},'') {col_name}"
					elif data_type in ('VARCHAR','VARCHAR2','CLOB','CHAR','LONG','NCLOB','NVARCHAR','NVARCHAR2','STRING') and null_flag == 'N':
						sel_col_name=f"NVL(NULLIF({col_name},''),' ') {col_name}"
					else:
						sel_col_name=col_name
					sel_col_list +=f"{sel_col_name},"
					src_col_list +=f"SRC.{col_name},"
					tgt_col_list +=f"{col_name},"
					if col_name.replace('"','') not in uk_col_list:
						upd_col_list +=f"TGT.{col_name} = SRC.{col_name},"
				rownum_col=',ROW_NUMBER() OVER (PARTITION BY '
				rownum_col +=f"{uk_col_list} ORDER BY header__timestamp DESC) ROW_NUM FROM {stgtbl} "
				rownum_col +=f" where header__operation not in ('BEFOREIMAGE','DELETE')) IN_SRC WHERE IN_SRC.ROW_NUM = 1) AS SRC ON ("
				upd_col_list += f"TGT.EDWSF_UPDATE_DTM = SRC.EDWSF_UPDATE_DTM,TGT.EDWSF_UPDATE_USER = SRC.EDWSF_UPDATE_USER,TGT.EDWSF_BATCH_ID = SRC.EDWSF_BATCH_ID,"
				upd_col_list +=f"TGT.EDWSF_SOURCE_DELETED_FLAG = SRC.EDWSF_SOURCE_DELETED_FLAG "
				who_col_list="EDWSF_CREATE_DTM,EDWSF_CREATE_USER,EDWSF_UPDATE_DTM,EDWSF_UPDATE_USER,EDWSF_BATCH_ID,EDWSF_SOURCE_DELETED_FLAG"
				who_col_val_list=" current_timestamp::varchar as EDWSF_CREATE_DTM, 'edwadm' as EDWSF_CREATE_USER,current_timestamp::varchar as EDWSF_UPDATE_DTM"
				who_col_val_list+=f",'edwadm' as EDWSF_UPDATE_USER, {batch_id} as EDWSF_BATCH_ID,'N' as EDWSF_SOURCE_DELETED_FLAG"
				ss_merge_sql=f"MERGE INTO {target_db}.{target_schema}.{target_table} AS TGT USING (SELECT "
				ss_merge_sql+=f"{sel_col_list} {who_col_list} FROM (SELECT {sel_col_list} {who_col_val_list} {rownum_col} {join_clause}) WHEN MATCHED THEN UPDATE SET"
				ss_merge_sql+=f" {upd_col_list} WHEN NOT MATCHED THEN INSERT ({tgt_col_list} {who_col_list}) VALUES ({src_col_list} {who_col_list})"
				sql=f"select count(*) cnt from {stgtbl} where header__operation='DELETE'"
				delcnt_obj=execute_df_qry(conn3, sql, reqid, conn)
				delcnt=delcnt_obj["cnt"][0]
				log_data(log_file,f"current iteration has {delcnt} rows deleted..",0)
				if delcnt > 0:
					log_data(log_file,f"Now marking {delcnt} rows as soft deleted..",0)
					delsql=f"update {target_db}.{target_schema}.{target_table} set edwsf_source_deleted_flag='Y' where ({uk_col_list}) in "
					delsql+=f"(select {uk_col_list} from {stgtbl} where header__operation='DELETE')"
					execute_qry(conn3,delsql,reqid,conn)
			else:
				sql=f"SELECT EDS_DATA_CATALOG.FN_GET_SS_MERGE_NEW2"
				sql+=f"('{env}',SOURCE_DB_NAME,SOURCE_SCHEMA,SOURCE_TABLE_NAME,SF_SOURCE_DB_NAME,SF_SOURCE_SCHEMA,SF_SOURCE_TABLE_NAME,"
				sql+=f"SF_TARGET_DB_NAME,SF_TARGET_SCHEMA,SF_TARGET_TABLE_NAME,SF_MERGE_TYPE)||CHR(10) AS PARAM_STRING"
				sql+=f" FROM "
				sql+=f"(SELECT SOURCE_DB_NAME,'{src_base_schema}' As SOURCE_SCHEMA,'{src_base_tbl}' As SOURCE_TABLE_NAME FROM EDW_JOB_STREAMS "
				sql+=f" WHERE JOB_STREAM_ID =(SELECT JOB_STREAM_ID_LINK FROM EDW_JOB_STREAMS WHERE JOB_STREAM_ID = '{jct}')) SRC,"
				sql+=f" (SELECT SOURCE_DB_NAME AS SF_SOURCE_DB_NAME, SOURCE_SCHEMA AS SF_SOURCE_SCHEMA, SOURCE_TABLE_NAME AS SF_SOURCE_TABLE_NAME,"
				sql+=f" TARGET_DB_NAME AS SF_TARGET_DB_NAME, TARGET_SCHEMA AS SF_TARGET_SCHEMA,TARGET_TABLE_NAME AS SF_TARGET_TABLE_NAME,MERGE_TYPE AS SF_MERGE_TYPE"
				sql+=f" FROM EDW_JOB_STREAMS WHERE JOB_STREAM_ID = '{jct}') SF"
				ss_merge_obj=execute_df_qry(conn, sql, reqid, conn)
				ss_merge_sql=str(ss_merge_obj["param_string"][0])
			log_data(log_file,ss_merge_sql,0)
			sql=f"select trim(where_clause) wc from edw_job_streams where job_stream_id='{jct}'"
			br_where_obj=execute_df_qry(conn, sql, reqid, conn)
			br_where_clause=br_where_obj["wc"][0]
			if br_where_clause is not None and len(br_where_clause) > 0:
				log_data(log_file,f"Since filter {br_where_clause} exists deleting the data before merging",0)
				sql=f"delete from {target_db}.{target_schema}.{target_table} where {br_where_clause}"
				log_data(log_file,sql,0)
				execute_qry(conn3,sql,reqid,conn)
			step_name=f"Now merging into {sstbl}"
			log_data(log_file,step_name,0)
			execute_qry(conn3,ss_merge_sql,reqid,conn)
		elif load_type == "APPEND":
			step_name=f"Generating append statement for {sstbl}"
			log_data(log_file,step_name,0)
			sql=f"SELECT EDS_DATA_CATALOG.FN_GET_INSERT_STMT_NEW"
			sql+=f"('{env}',SOURCE_DB_NAME,SOURCE_SCHEMA,SOURCE_TABLE_NAME,SF_SOURCE_DB_NAME,SF_SOURCE_SCHEMA,SF_SOURCE_TABLE_NAME,"
			sql+=f"SF_TARGET_DB_NAME,SF_TARGET_SCHEMA,SF_TARGET_TABLE_NAME)||CHR(10) AS PARAM_STRING"
			sql+=f" FROM "
			sql+=f"(SELECT SOURCE_DB_NAME,SOURCE_SCHEMA,SOURCE_TABLE_NAME FROM EDW_JOB_STREAMS "
			sql+=f" WHERE JOB_STREAM_ID =(SELECT JOB_STREAM_ID_LINK FROM EDW_JOB_STREAMS WHERE JOB_STREAM_ID = '{jct}')) SRC,"
			sql+=f" (SELECT SOURCE_DB_NAME AS SF_SOURCE_DB_NAME,SOURCE_SCHEMA AS SF_SOURCE_SCHEMA,SOURCE_TABLE_NAME AS SF_SOURCE_TABLE_NAME,"
			sql+=f" TARGET_DB_NAME AS SF_TARGET_DB_NAME,TARGET_SCHEMA AS SF_TARGET_SCHEMA,TARGET_TABLE_NAME AS SF_TARGET_TABLE_NAME,MERGE_TYPE AS SF_MERGE_TYPE"
			sql+=f" FROM EDW_JOB_STREAMS WHERE JOB_STREAM_ID = '{jct}') SF"
			ss_ins_obj=execute_df_qry(conn, sql, reqid, conn)
			ss_ins_sql=str(ss_ins_obj["param_string"][0])
			log_data(log_file,ss_ins_sql,0)
			step_name=f"Now checking for any filter to delete data from {sstbl}"
			log_data(log_file,step_name,0)
			sql=f"select trim(where_clause) wc from edw_job_streams where job_stream_id='{jct}'"
			br_where_obj=execute_df_qry(conn, sql, reqid, conn)
			br_where_clause=br_where_obj["wc"][0]
			if br_where_clause is not None and len(br_where_clause) > 0:
				log_data(log_file,f"Since filter {br_where_clause} exists deleting the data before appending",0)
				sql=f"delete from {target_db}.{target_schema}.{target_table} where {br_where_clause}"
				log_data(log_file,sql,0)
				execute_qry(conn3,sql,reqid,conn)
			step_name=f"Now inserting into {sstbl}"
			log_data(log_file,step_name,0)
			execute_qry(conn3,ss_ins_sql,reqid,conn)
	else:
		step_name="Entering into HANANRT merge flow"
		log_data(log_file,step_name,0)
		if load_type != "MERGE_ONE2ONE":
			raise Exception("Only permitted merge type for HANANRT is MERGE_ONE2ONE. Please fix and retry")
		step_name="Querying replication key from EDW_UNIQUE_KEY_COLUMN table"
		log_data(log_file,step_name,0)
		sql=f"select b.column_name from EDS_DATA_CATALOG.EDW_UNIQUE_KEY a,EDS_DATA_CATALOG.EDW_UNIQUE_KEY_COLUMN b where a.table_name='{source_table_name}' and a.environment_name='{env}'"
		sql=sql + f" and a.db_instance_name='{source_db_name}' and a.db_schema_name='{source_schema}' and a.active_flag='A' and a.unique_key_name=b.unique_key_name "
		sql=sql + " and a.environment_name=b.environment_name and a.db_instance_name=b.db_instance_name and a.table_name=b.table_name and a.db_schema_name=b.db_schema_name"
		uk_obj=execute_df_qry(conn, sql, reqid, conn)
		if type(uk_obj) is int or len(uk_obj) == 0:
			raise Exception(f"Unable to query unique key for {source_db_name}.{source_schema}.{source_table_name}")
		uk_col_list=''
		for index,row in uk_obj.iterrows():
			uk_col_list+=f"{row[0]},"
		uk_col_list=uk_col_list[:-1] ## remove last comma
		log_data(log_file,f"Identified replication key is {uk_col_list}",0)
		step_name="Querying stage and SS table details"
		log_data(log_file,step_name,0)
		sql=f"select source_db_name,source_schema,source_table_name,target_db_name,target_schema,target_table_name from edw_job_streams "
		sql+=f" where job_stream_id='{jct}'"
		ss_obj=execute_df_qry(conn, sql, reqid, conn)
		if type(ss_obj) is int or len(ss_obj) == 0:
			raise Exception(f"Unable to query SS details for JCT {jct}")
		stg_db=ss_obj["source_db_name"][0]
		stg_schema=ss_obj["source_schema"][0]
		stg_table=ss_obj["source_table_name"][0]
		ss_db=ss_obj["target_db_name"][0]
		ss_schema=ss_obj["target_schema"][0]
		ss_table=ss_obj["target_table_name"][0]
		step_name="Deleting rows from SS"
		sql=f"select count(*) cnt from {ss_db}.{ss_schema}.{ss_table} where ({uk_col_list}) in "
		sql+=f" (select {uk_col_list} from {stg_db}.{stg_schema}.{stg_table})"
		sf_cnt_obj=execute_df_qry(conn3, sql, reqid, conn)
		sf_del_cnt=sf_cnt_obj["cnt"][0]
		log_data(log_file,f"{sf_del_cnt} rows will be deleted before insertion from {ss_db}.{ss_schema}.{ss_table}",0)
		log_data(log_file,step_name,0)
		sql="begin transaction"
		execute_qry(conn3,sql,reqid,conn)
		ss_del_sql=f"delete from {ss_db}.{ss_schema}.{ss_table} where ({uk_col_list}) in "
		ss_del_sql+=f" (select {uk_col_list} from {stg_db}.{stg_schema}.{stg_table})"
		execute_qry(conn3,ss_del_sql,reqid,conn)
		step_name="inserting data to ss"
		log_data(log_file,step_name,0)
		sql=f"SELECT EDS_DATA_CATALOG.FN_GET_INSERT_STMT_NEW"
		sql+=f"('{env}',SOURCE_DB_NAME,SOURCE_SCHEMA,SOURCE_TABLE_NAME,SF_SOURCE_DB_NAME,SF_SOURCE_SCHEMA,SF_SOURCE_TABLE_NAME,"
		sql+=f"SF_TARGET_DB_NAME,SF_TARGET_SCHEMA,SF_TARGET_TABLE_NAME)||CHR(10) AS PARAM_STRING"
		sql+=f" FROM "
		sql+=f"(SELECT SOURCE_DB_NAME,SOURCE_SCHEMA,SOURCE_TABLE_NAME FROM EDW_JOB_STREAMS "
		sql+=f" WHERE JOB_STREAM_ID =(SELECT JOB_STREAM_ID_LINK FROM EDW_JOB_STREAMS WHERE JOB_STREAM_ID = '{jct}')) SRC,"
		sql+=f" (SELECT SOURCE_DB_NAME AS SF_SOURCE_DB_NAME,SOURCE_SCHEMA AS SF_SOURCE_SCHEMA,SOURCE_TABLE_NAME AS SF_SOURCE_TABLE_NAME,"
		sql+=f" TARGET_DB_NAME AS SF_TARGET_DB_NAME,TARGET_SCHEMA AS SF_TARGET_SCHEMA,TARGET_TABLE_NAME AS SF_TARGET_TABLE_NAME,MERGE_TYPE AS SF_MERGE_TYPE"
		sql+=f" FROM EDW_JOB_STREAMS WHERE JOB_STREAM_ID = '{jct}') SF"
		ss_ins_obj=execute_df_qry(conn, sql, reqid, conn)
		ss_ins_sql=str(ss_ins_obj["param_string"][0])
		log_data(log_file,ss_ins_sql,0)
		step_name=f"Now inserting into {sstbl}"
		log_data(log_file,step_name,0)
		execute_qry(conn3,ss_ins_sql,reqid,conn)
	execute_qry(conn3,'commit',reqid,conn)
## Main program starts here
pgmVersion='1.0'
parser = argparse.ArgumentParser(description='Script used to update BR data',
    epilog='Example: python DIY_load_ssbr.py -j jct -e environment -r reqid')
parser.add_argument('-j', '--jct',required=True,
    help='STG2BR JCT which needs to be executed ')
parser.add_argument('-e','--env',required=True,
        help='Environment for which stg2ss/br needs to be executed (DV3/TS3/PRD)')
parser.add_argument('-r','--reqid',
        help='Reqid from DIY tables')
args=parser.parse_args()
jct=args.jct
env=args.env
reqid=args.reqid
conn3=''
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
	log_file=f"{logs_dir}/{jct}_ssbrload.log"
	err_file=f"{logs_dir}/{jct}_ssbrload.err"
	step_name="Querying JCT to get metadata"
	log_data(log_file,step_name,0)
	sql=f"select workflow_type,target_db_connection,source_db_name,source_schema,source_table_name,target_table_name"
	sql+=f",target_db_name,target_schema,sf_warehouse,sf_role,merge_type,job_stream_id_link,run_status,active_ind"
	sql+=f" from edw_job_streams where job_stream_id='{jct}'"
	jct_obj=execute_df_qry(conn, sql, reqid, conn)
	if len(jct_obj) == 0:
		raise Exception(f"Unable to query jobstream {jct}")
	workflow_type=jct_obj["workflow_type"][0]
	target_db_connection=jct_obj["target_db_connection"][0]
	source_db=jct_obj["source_db_name"][0]
	source_schema=jct_obj["source_schema"][0]
	source_table=jct_obj["source_table_name"][0]
	stgtbl=f"{source_db}.{source_schema}.{source_table}"
	target_table=jct_obj["target_table_name"][0]
	target_db=jct_obj["target_db_name"][0]
	target_schema=jct_obj["target_schema"][0]
	sstbl=f"{target_db}.{target_schema}.{target_table}"
	sf_warehouse=jct_obj["sf_warehouse"][0]
	sf_role=jct_obj["sf_role"][0]
	merge_type=jct_obj["merge_type"][0]
	stg_jct=jct_obj["job_stream_id_link"][0]
	run_status=jct_obj["run_status"][0]
	active_ind=jct_obj["active_ind"][0]
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
	sql=f"select job_stream_description desc1,source_db_connection,source_table_name,source_schema,source_db_name from edw_job_streams where job_stream_id='{stg_jct}'"
	base_obj=execute_df_qry(conn, sql, reqid, conn)
	base_obj_str=base_obj["desc1"][0]
	source_db_conn=base_obj["source_db_connection"][0]
	source_table_name=base_obj["source_table_name"][0]
	source_schema=base_obj["source_schema"][0]
	source_db_name=base_obj["source_db_name"][0]
	if len(base_obj_str) == 0:
		raise Exception(f"Unable to find base object from JCT {jct}")
	src_base_schema=base_obj_str.split(',')[0].split('=')[1]
	src_base_tbl=base_obj_str.split(',')[1].split('=')[1]
	step_name="Querying db type"
	sql=f"select parameter_category from eds_data_catalog.edw_parameter where parameter_type='{source_db_conn}'"
	sql+=f" and parameter_name='SOURCE_LOGIN'"
	dbtype_obj=execute_df_qry(conn, sql, reqid, conn)
	db_type=dbtype_obj["parameter_category"][0]
	if len(db_type) == 0:
		raise Exception(f"Unable to find db_type for {source_db_conn}")
	step_name="Fetching sf connection details"
	log_data(log_file,step_name,0)
	if configFile.Config.PRDSUFF in env:
		sfEnv = 'cisco.us-east-1'
	elif configFile.Config.STGSUFF in env:
		sfEnv = 'ciscostage.us-east-1'
	elif configFile.Config.DEVSUFF in env:
		sfEnv = 'ciscodev.us-east-1'
	sql=f"select to_char(parameter_value) sf_username from eds_data_catalog.edw_parameter where parameter_type='{target_db_connection}'"
	sql+=f" and parameter_category='SNOWFLAKE' and environment_name='{env}' and parameter_name='CONNECTION_USER'"
	sf_user_dict=execute_df_qry(conn, sql, reqid, conn)
	sf_user=sf_user_dict["sf_username"][0]
	if sf_user is None or sf_user == '':
		raise Exception("Unable to determine snowflake username to connect")
	step_name="Establishing SF connection"
	log_data(log_file,step_name,0)
	currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
	if currOsUser == "":
		try:
			currOsUser=os.getlogin()
		except Exception as e:
			currOsUser=c_getuser()
			pass
	sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","load_ssbr")
	conn3=open_sf_connection(sfEnv,sf_user,sf_warehouse,sf_role,target_db,target_schema,query_tag=sf_query_tag)
	step_name="Checking warehouse to use"
	log_data(log_file,step_name,0)
	sql=f"select bytes/(1024*1024*1024) byt from {target_db}.information_schema.tables  where table_name='{target_table}' and table_schema='{target_schema}'"
	resultsObj=execute_df_qry(conn3,sql,reqid,conn)
	if len(resultsObj) > 0:
		sftblSize = resultsObj["byt"][0]
	else:
		sql=f"select bytes/(1024*1024*1024) byt from {source_db}.information_schema.tables  where table_name='{source_table}' and table_schema='STG'"
		resultsObj=execute_df_qry(conn3,sql,reqid,conn)
		sftblSize = resultsObj["byt"][0]
	log_data(log_file,f"Target table is {sftblSize} GB",0)
	if merge_type == 'MERGE_ONE2ONE':
		if sftblSize > 50:
			sf_warehouse=sf_warehouse.replace('COMMON_WH','COMMON_EXT3_WH')
		elif sftblSize > 25:
			sf_warehouse=sf_warehouse.replace('COMMON_WH','COMMON_EXT2_WH')
		else:
			sf_warehouse=sf_warehouse.replace('COMMON_WH','COMMON_EXT1_WH')
	else:
		sql=f"select bytes/(1024*1024*1024) byt from {source_db}.information_schema.tables  where table_name='{source_table}' and table_schema='STG'"
		resultsObj=execute_df_qry(conn3,sql,reqid,conn)
		sftblSize = resultsObj["byt"][0]
		if sftblSize > 50:
			sf_warehouse=sf_warehouse.replace('COMMON_WH','COMMON_EXT3_WH')
		elif sftblSize > 25:
			sf_warehouse=sf_warehouse.replace('COMMON_WH','COMMON_EXT2_WH')
		else:
			sf_warehouse=sf_warehouse.replace('COMMON_WH','COMMON_EXT1_WH')

	sql=f"use warehouse {sf_warehouse}"
	execute_qry(conn3,sql,reqid,conn)
	step_name="Updating JCT run status"
	log_data(log_file,step_name,0)
	upd_jct_status('R')
	step_name="Inserting into job stream runs"
	log_data(log_file,step_name,0)
	ins_jct_run_status('Insert')
	step_name=f"Performing merge_type from {stgtbl} to {sstbl}"
	log_data(log_file,step_name,0)
	modify_br_data(merge_type)
	step_name="Updating job stream runs"
	log_data(log_file,step_name,0)
	ins_jct_run_status('Update')
	step_name="Updating job JCT run status"
	log_data(log_file,step_name,0)
	upd_jct_status('C')
	if isinstance(conn3,snowflake.connector.connection.SnowflakeConnection):
		conn3.close()
except Exception as e:
	log_data(err_file,f"Failure at step {step_name}. Please find below exception",1)
	log_data(err_file,str(e),1)
	if sql is not None:
		log_data(err_file,f"Last sql executed was :- {sql}",1)
	print(f"Below exception occurred at step {step_name} while running stg2br/ss jct")
	print(str(e))
	if conn3:
		execute_qry(conn3,'rollback',reqid,conn)
	sys.exit(1)