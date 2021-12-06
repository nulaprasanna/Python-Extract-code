#!/apps/python/install/bin/python
#########################################################################################################################
# unload_hana_nrt_tbl_worker.py                                                                                         #
# Worker scirpt to extract data from Hana table                                                                         #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          May-2020                Initial version                                                 #
# 1.1           Manick          May-2020                Upload data as and when download completes                      #
# 1.2           Manick          May-2020                Get host list dynamically and cap parallel uploads running      #
#########################################################################################################################

import sys
import csv
import os
import math
from ConfigObject import ConfigObject
from metadata_handler import *
import pdb,random,string
import calendar
from datetime import datetime
import argparse


def upload_hana_data(batch_number):
	global log_file,err_file
	log_file_handle=open(log_file,'a')
	err_file_handle=open(err_file,'a')
	upl_cmd=f"/apps/edwsfdata/python/scripts/DIY_hana_nrt_interim_load.py -b {batch_number}"
	cmnd=subprocess.Popen(upl_cmd,stdout=log_file_handle,stderr=err_file_handle,shell=True)

def unload_hana_data(batch_number,run_id,hana_host_array):
	global reqid,log_file,err_file,srcdbParams,srcPassword,vsrcSchema,tableName,step_name,vsrcQuery,vPath,vFile,REVSRC
	global vDelim,vWhere,vBatch,vInccol,vExttype,vFromdtm,vTodtm,vFromid,vToid,envType,srcBaseschema,srcBasetable,currtime
	try:
		step_name=f"Establishing remote DB connection for batch number {batch_number}"
		hid=run_id%4
		hostName=hana_host_array[hid][0]
		port=hana_host_array[hid][1]
		conn2 = open_hana_connection(hostName,port,srcdbParams['SOURCE_LOGIN'],srcPassword)
		if type(conn2) is int:
			raise Exception ("Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'])
		cursor2=conn2.cursor()
		cursor2.arraysize=5000
		conn = open_oracle_connection(envType,None,True)
		cursor1=conn.cursor()
		printHeader = False # include column headers in each table output
		step_name=f"Retreiving remote connection id for batch {batch_number}"
		log_data(log_file,step_name,0)
		log_data
		cursor2.execute('select CURRENT_CONNECTION from dummy');
		results=cursor2.fetchall()
		for obj in results:
			conn_id=obj[0]
		step_name=f"Updating BTP table for batch {batch_number}"
		log_data(log_file,step_name,0)
		sql=f"update DIY_HANA_NRT_BATCH_LIST set connection_id={conn_id},run_status='R',start_time=sysdate where reqid={reqid} and batch_id={vBatch}"
		sql+=f" and batch_number={batch_number}"
		cursor1.execute(sql)
		cursor1.execute('commit')
		step_name=f"Invoking start publishing procedure for batch {batch_number}"
		log_data(log_file,step_name,0)
		sql=f"call \"EXTRACT\".\"extract.procedures::startPublishing\"('SNFLK','{srcBaseschema}','{srcBasetable}',-1,{batch_number})"
		cursor2.execute(sql)
		cursor2.execute('commit')

		step_name=f"Framing Extract query for batch {batch_number}"
		log_data(log_file,step_name,0)
		csv_file_dest = f"{vPath}{vFile}_{currtime}.{run_id}"
		outputFile = open(csv_file_dest,'w') # 'wb'
		output = csv.writer(outputFile, dialect='excel',delimiter=vDelim,quotechar='"',quoting=csv.QUOTE_ALL)
		#BQRY=f'{vsrcQuery} FROM "{vsrcSchema}"."{tableName}"'
		BQRY=f'{vsrcQuery} FROM {vsrcSchema}.{tableName} where batch_number={batch_number}'
		REVSRC = BQRY
		log_data(log_file,REVSRC,0)
		print("REVSRC is " + REVSRC)
		step_name=f"Running extract query at source for batch {batch_number}"
		log_data(log_file,step_name,0)
		cursor2.execute(REVSRC)
		step_name=f"Writing to csv file for batch {batch_number}"
		log_data(log_file,step_name,0)
		if printHeader: # add column headers if requested
			cols = []
			for col in cursor2.description:
				cols.append(col[0])
			output.writerow(cols)

		for row_data in cursor2: # add table rows
			output.writerow(row_data)

		outputFile.close()
		step_name=f"Updating BTP table for batch {batch_number}"
		log_data(log_file,step_name,0)
		sql=f"update DIY_HANA_NRT_BATCH_LIST set last_update_time=sysdate,run_status='U',flat_file='{csv_file_dest}' "
		sql+=f" where reqid={reqid} and batch_id={vBatch} and batch_number={batch_number}"
		cursor1.execute(sql)
		cursor1.execute('commit')
		step_name=f"Calling procedure to communicate finish publishing for batch {batch_number}"
		log_data(log_file,step_name,0)
		sql=f"call \"EXTRACT\".\"extract.procedures::finishPublishing\"('SNFLK','{srcBaseschema}','{srcBasetable}')"
		cursor2.execute(sql)
		cursor2.execute('commit')

	except Exception as e:
		log_data(err_file,f"Failure at step {step_name}. Please find below exception",1)
		log_data(err_file,str(e),1)
		raise
	finally:
		conn.close()
		conn2.close()

## Main program starts here
pgmVersion='1.0'
parser = argparse.ArgumentParser(description='Script used to pull data from HANA table',
    epilog='Example: python unload_hana_nrt_tbl_worker.py -b batch_number -i runid')
parser.add_argument('-b','--batchNum',required=True,
        help='Batch Number whose data to be extracted')
parser.add_argument('-i','--runid',required=True,
        help='Run id of current extraction')

args=parser.parse_args()
try:
	REVSRC=''
	batch_number=int(args.batchNum)
	run_id=int(args.runid)
	envType=os.environ['RUN_ENVIRONMENT']
	connectionName=os.environ['SOURCE_DB_CONNECTION']
	dbName=os.environ['SOURCE_DB_NAME']
	tableName = os.environ['SOURCE_TABLE_NAME']
	vPath=os.environ['SRC2STG_FF_PATH']
	vFile=os.environ['CURRENT_SRC2STG_FF_NAME']
	vDelim=os.environ['SRC2STG_FF_DELIMITER']
	vsrcQuery=os.environ['SOURCE_QUERY']
	vsrcSchema=os.environ['SOURCE_SCHEMA']
	vWhere=os.environ['WHERE_CLAUSE']
	vBatch=os.environ['BATCH_ID']
	vInccol=os.environ['INCREMENTAL_COLUMN_NAME']
	vExttype=os.environ['EXTRACT_TYPE']
	vFromdtm=os.environ['FROM_EXTRACT_DTM']
	vTodtm=os.environ['TO_EXTRACT_DTM']
	vFromid=os.environ['FROM_EXTRACT_ID']
	vToid=os.environ['TO_EXTRACT_ID']
	reqid=os.environ['REQID']
	srcBaseschema=os.environ['BASE_SCHEMA']
	srcBasetable=os.environ['BASE_TABLE']
	sfEnv=os.environ['SF_ENV']
	os.environ['RUN_ID']=str(run_id)
	vsrcQuery=vsrcQuery + str(vBatch) + " as EDWSF_BATCH_ID "
	currtime=datetime.now().strftime('%Y%m%d%H%M%S')
	step_name="Connecting to Repos DB"
	conn = open_oracle_connection(envType,None,True)
	configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
	concurrent_nrt_batch_count=int(configFile.Config.concurrent_nrt_batch_count)
	cursor1=conn.cursor()
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	step_name=f"Creating BTP Worker log and err file"
	sql=f"select logs_dir from diy_master where reqid={reqid}"
	logs_dir_obj=execute_df_qry(conn, sql, reqid, conn)
	logs_dir=logs_dir_obj["logs_dir"][0]
	if not os.path.exists(os.path.dirname(logs_dir)):
		raise Exception("Unable to locate logs_dir. Please check")
	log_file=f"{logs_dir}/{reqid}_nrtload_{vBatch}_worker{run_id}.log"
	err_file=f"{logs_dir}/{reqid}_nrtload_{vBatch}_worker{run_id}.err"
	log_data(log_file,step_name,0)
	step_name="Fetching source DB params"
	log_data(log_file,step_name,0)
	srcdbParams={}
	sql="select parameter_name,to_char(parameter_value) from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "' and parameter_type ='" + connectionName + "'"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	if len(results) == 0:
		msg="Could not find connection details for connection name - " + connectionName.strip('\n') + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion"
		raise Excepiton (msg)
	for obj in results:
		param=obj[0]
		value=obj[1]
		srcdbParams[param]=value
	#print(srcdbParams)
	step_name="Fetching source db password"
	log_data(log_file,step_name,0)
	sql2="SELECT UTL_I18N.RAW_TO_CHAR('"+ srcdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') from dual"
	cursor1.execute(sql2)
	results=cursor1.fetchall()
	for passobj in results:
		srcPassword=passobj[0]
	step_name="Establishing connection to Hana to retrive individual node names"
	log_data(log_file,step_name,0)
	hostName=srcdbParams['SOURCE_HOST'].split(';')[0].split(':')[0]
	port=srcdbParams['SOURCE_PORT']
	if 'SOURCE_ADDITIONAL_PARAMS' in srcdbParams:
		conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword,srcdbParams['SOURCE_ADDITIONAL_PARAMS'])
	else:
		conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword)
	cursor2=conn2.cursor()
	step_name="Retrieving individual node names"
	log_data(log_file,step_name,0)
	sql="select host, sql_port as port from m_services where service_name = 'indexserver' and detail != 'master'"
	cursor2.execute(sql)
	results=cursor2.fetchall()
	if len(results) == 0:
		msg="Could not find get individual Hana nodes for downloading data.. Hence cannot proceed with ingestion"
		raise Excepiton (msg)
	hana_host_array=results
	unload_hana_data(batch_number,run_id,hana_host_array)
	osPrcmd="ps -ef | grep DIY_hana_nrt_interim_load.py | grep -v grep | wc -l"
	cnt=int(os.popen(osPrcmd).read())
	if cnt <= concurrent_nrt_batch_count :
		upload_hana_data(batch_number)
	else:
		sleep_cnt=0
		while cnt > concurrent_nrt_batch_count and (sleep_cnt < 30):
			log_data(log_file,f"{cnt} interim upload jobs are still in WIP.. Hence waiting for them to finish ",0)
			sleep_cnt+=1
			time.sleep(120)
			cnt=int(os.popen(osPrcmd).read())
		if sleep_cnt >=30:
			log_data(log_file,f"Even after sleeping for 2 hours {cnt} interim upload jobs are still WIP.. Hence giving up now",0)
		else:
			upload_hana_data(batch_number)
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Exception occurred at line {linenu} step {step_name} while extracting hana NRT data - {e}")
	print("Error occurred while running to source query - " + REVSRC)
	sys.exit(7)
