#!/apps/python/install/bin/python
#########################################################################################################################
# audit_functions.py                                                                                                    #
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Nov-2020                Initial Version                                                 #
#########################################################################################################################
import argparse,pdb
import os, re, string, subprocess, sys
from datetime import datetime
from datetime import timedelta
import time
import faulthandler; faulthandler.enable()
import math
from metadata_handler import *
from operator import itemgetter, attrgetter
from ConfigObject import ConfigObject

def validate_jct(repo_conn,jct,reqid,envType):
	try:
		jct_dict={}
		sql=f"select source_db_connection sdc from edw_job_streams where job_stream_id='{jct}' and active_ind='Y'" 
		jct_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(jct_obj) != 1:
			raise Exception(f"Invalid JCT {jct}")
		sdc = jct_obj["sdc"][0]
		jct_dict["sdc"]=sdc
		sql=f"select distinct parameter_category pc from eds_data_catalog.edw_parameter where parameter_type='{sdc}'"
		type_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(type_obj) != 1:
			raise Exception(f"Could not find source db type")
		db_type = jct_obj["pc"][0]
		jct_dict["db_type"]=db_type
		sql=f"select source_db_connection sdc,job_stream_id js from edw_job_streams where job_stream_id_link='{jct}' and active_ind='Y'"
		brjct_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(brjct_obj) != 1:
			raise Exception(f"Could not locate BR JCT for SRC2STG JCT {jct}")
		tdc = brjct_obj["sdc"][0]
		jct_dict["tdc"]=tdc
		jct_dict["brjct"]=brjct_obj["js"][0]
		sql="select parameter_name pn,to_char(parameter_value) pv from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "'"
		sql+=f" and parameter_category='{db_type}' and parameter_type ='{sdc}'"
		sdc_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(sdc_obj) == 0:
			raise Exception(f"Could not query parameters for object {sdc}")
		for index,row in sdc_obj.iterrows():
			if row["pn"]="SOURCE_LOGIN_PASSWORD":
				sql="SELECT UTL_I18N.RAW_TO_CHAR('"+ row["pv"] +"','AL32UTF8') pv from dual"
				pass_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
				jct_dict[row["pn"]]=pass_obj["pv"]
			else:
				jct_dict[row["pn"]]=row["pv"]
		return(jct_dict)
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred at line {linenu} step {step_name} while downloading data")

def fetch_sf_conn_values(tdc,envType,repo_conn,reqid,brjct):
	try:
		jct_dict={}
		sql="select parameter_name pn,to_char(parameter_value) pv from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "'"
		sql+=f" parameter_type ='{tdc}'"
		sdc_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		if len(sdc_obj) == 0:
			raise Exception(f"Could not query parameters for object {sdc}")
		for index,row in sdc_obj.iterrows():
			if row["pn"]="SOURCE_LOGIN_PASSWORD":
				sql="SELECT UTL_I18N.RAW_TO_CHAR('"+ row["pv"] +"','AL32UTF8') pv from dual"
				pass_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
				jct_dict[row["pn"]]=pass_obj["pv"]
			else:
				jct_dict[row["pn"]]=row["pv"]
		sql=f"select sf_role,sf_warehouse,target_db_name,target_schema,target_table_name from edw_job_streams where job_stream_id='{brjct}'"
		brjct_obj=execute_df_qry(repo_conn, sql, reqid, repo_conn)
		jct_dict["sf_role"]=brjct_obj["sf_role"][0]
		jct_dict["sf_warehouse"]=brjct_obj["sf_warehouse"][0]
		jct_dict["target_db_name"]=brjct_obj["target_db_name"][0]
		jct_dict["target_schema"]=brjct_obj["target_schema"][0]
		jct_dict["target_table_name"]=brjct_obj["target_table_name"][0]
		return(jct_dict)
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred at line {linenu} step {step_name} while downloading data")