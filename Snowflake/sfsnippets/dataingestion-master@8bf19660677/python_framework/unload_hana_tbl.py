#!/apps/python/install/bin/python
#########################################################################################################################
# unload_hana_tbl.py                                                                                                    #
# Script to extract data from Hana table                                                                                #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Apr-2020                Initial version                                                 #
# 1.1           Manick          Apr-2020                Change to_date to to_timestamp for inc load                     #	
# 1.2           Manick          Sep-2020                Enable upload while download                                    #
# 1.3           Manick          Oct-2020                Introuduce Hana load balancing                                  #
# 1.4           Manick          Nov-2020                Replace double quote to single quote in where clause            #
# 1.5           Manick          Nov-2020                Check proc status only if subprocess submission happenned       #
# 1.6           Manick          Nov-2020                Check error file size for any interim load errors               #
#########################################################################################################################

import sys
import csv
import os,subprocess
import math,decimal,psutil,time
from datetime import datetime
from ConfigObject import ConfigObject
from metadata_handler import *
from pathlib import Path


## Main program starts here
configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
BASE_DIR=configFile.Config.BASE_DIR
talendbase=BASE_DIR + '/talend'
tesbase=talendbase + '/TES'
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
LOGS_DIR=os.environ['LOGS_DIR']
reqid=os.environ['REQID']
jct=os.environ['JOB_STREAM_ID']
try:
	step_name="Connecting to Repos DB"
	conn = open_oracle_connection(envType,None,True)
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	srcdbParams={}
	cursor1=conn.cursor()
	print('Gathering source db connection detais from repository')
	step_name="Fetching source DB params"
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
	sql2="SELECT UTL_I18N.RAW_TO_CHAR('"+ srcdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') from dual"
	cursor1.execute(sql2)
	results=cursor1.fetchall()
	for passobj in results:
		srcPassword=passobj[0]
	step_name="Fetching unsupported Character type"
	sql3 = "select column_name from EDS_DATA_CATALOG.EDW_TABLE_COLUMN a,diy_unsupport_datatypes b  where db_instance_name='" + dbName + "'"
	sql3 = sql3 + " and db_schema_name='" + vsrcSchema.replace('"','') + "' and table_name='" + tableName.replace('"','') + "'"
	sql3 = sql3 + " and environment_name='" + envType + "' and b.source_db_type='HANA' and b.column_datatype=a.data_type"
	cursor1.execute(sql3)
	unsuppRes=cursor1.fetchall()
	step_name="Establising source db connection"
	hostName=srcdbParams['SOURCE_HOST'].split(';')[0].split(':')[0]
	if 'SOURCE_ADDITIONAL_PARAMS' in srcdbParams:
		conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword,srcdbParams['SOURCE_ADDITIONAL_PARAMS'])
	else:
		conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword)
	if type(conn2) is int:
		raise Exception ("Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'])
	cursor2=conn2.cursor()
	cursor2.arraysize=5000
	step_name="Retrieving individual node names"
	sql="select host, sql_port as port from m_services where service_name = 'indexserver' and detail != 'master'"
	cursor2.execute(sql)
	results=cursor2.fetchall()
	if len(results) == 0:
		sql="select host, sql_port as port from m_services where service_name = 'indexserver'"
		cursor2.execute(sql)
		results=cursor2.fetchall()
		if len(results) == 0:
			msg="Could not find get individual Hana nodes for downloading data.. Hence cannot proceed with ingestion"
			raise Excepiton (msg)
	hana_host_array=results
	sql=f"select job_stream_seq_num jsn from edw_job_streams where job_stream_id='{jct}'"
	seqarr=execute_df_qry(conn, sql, reqid, conn)
	job_seq=seqarr["jsn"][0]
	hid=job_seq%len(hana_host_array)
	hostName=hana_host_array[hid][0]
	port=hana_host_array[hid][1]
	conn2 = open_hana_connection(hostName,port,srcdbParams['SOURCE_LOGIN'],srcPassword)
	if type(conn2) is int:
		raise Exception ("Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'] + " to node " + hostName)
	cursor2=conn2.cursor()
	cursor2.arraysize=5000
	printHeader = False # include column headers in each table output

	# output each table content to a separate CSV file
	step_name="Framing Extract query"
	currtime=datetime.now().strftime('%Y%m%d%H%M%S')
	file_cnt=1
	csv_file_dest = f"{vPath}{vFile}_{currtime}_{file_cnt}" 
	outputFile = open(csv_file_dest,'w') # 'wb'
	output = csv.writer(outputFile, dialect='excel',delimiter=vDelim,quotechar='"',quoting=csv.QUOTE_ALL)
	vsrcQuery=vsrcQuery + str(vBatch) + " as EDWSF_BATCH_ID ,"
	SRCQRY=vsrcQuery.rstrip(',') ## strip last comma
	#print ("SRCQRY is " + SRCQRY)
	#BQRY=SRCQRY + " FROM " + vsrcSchema + "." + tableName
	BQRY=f'{SRCQRY} FROM {vsrcSchema}.{tableName}'
	#print ("BQRY is " + BQRY)
	if vWhere == " WHERE ":
		if vExttype == "DATE":
			vWhere = vWhere + vInccol + " > to_timestamp('" + vTodtm + "','YYYY/MM/DD HH24:MI:SS')"	
			REVSRC = BQRY + vWhere
		elif vExttype == "ID":
			vWhere = vWhere + vInccol + " > " + str(vToid)
			REVSRC = BQRY + vWhere
	elif (len(vWhere) > 7):
		REVSRC = BQRY + vWhere.replace("''","'")
	else:
		REVSRC = BQRY
	#print("REVSRC is " + REVSRC)
	step_name="Handling unsupported db types"
	if len(unsuppRes) > 0:
		for unsuppobj in unsuppRes:
			colName=unsuppobj[0]
			revCol="NULL"
			## find replace original clob with changed column name
			#REVSRC = REVSRC.replace(',\"' + colName + '\" AS ' , "," + revCol + " AS ")
			REVSRC = REVSRC.replace('"' + colName + '"' , revCol + " AS " + colName)
	print("REVSRC is " + REVSRC)
	step_name="Running extract query at source"
	cursor2.execute(REVSRC)
	step_name="Writing to csv file"
	if printHeader: # add column headers if requested
			cols = []
			for col in cursor2.description:
					cols.append(col[0])
			output.writerow(cols)

	outf=open(f"{LOGS_DIR}/hana_split_load_{currtime}.log",'w')
	errf=open(f"{LOGS_DIR}/hana_split_load_{currtime}.err",'w')
	touch_file=f"{LOGS_DIR}/{jct}_{currtime}"
	Path(touch_file).touch()
	processes = set()
	line_counter=0
	launched_subproc=False
	for row_data in cursor2: # add table rows
			output.writerow(row_data)
			if line_counter > 10000:
				## check for file size only once in 10K lines for performance improvement
				line_counter=0
				file_size=os.stat(csv_file_dest).st_size
				if file_size > 536870912:
					outputFile.close()
					if file_cnt == 1:
						## launching split load process to start loading while remaining parts are being extracted
						oscmd="/apps/edwsfdata/python/scripts/DIY_orcl_interim_load.py"
						proc=subprocess.Popen([oscmd,'-e',envType,'-r',str(reqid),'-j',jct,'-t',currtime],stdout=outf, stderr=errf)
						pid=proc.pid
						print(f"Snowflake load triggered with pid {pid}")
						processes.add(proc)
						launched_subproc=True
					file_cnt += 1
					if file_cnt> 1:
						if not psutil.pid_exists(pid):
							raise Exception("Interim load process not running. Please check log and retry")
					csv_file_dest = f"{vPath}{vFile}_{currtime}_{file_cnt}" 
					outputFile = open(csv_file_dest,'w')
					output = csv.writer(outputFile, dialect='excel',delimiter=vDelim,quotechar='"',quoting=csv.QUOTE_ALL)
			else:
				line_counter += 1

	outputFile.close()
	if file_cnt == 1:
		file_size=os.stat(csv_file_dest).st_size
		if file_size > 0:
			## launching split load process to start loading while remaining parts are being extracted
			oscmd="/apps/edwsfdata/python/scripts/DIY_orcl_interim_load.py"
			proc=subprocess.Popen([oscmd,'-e',envType,'-r',str(reqid),'-j',jct,'-t',currtime],stdout=outf, stderr=errf)
			pid=proc.pid
			print(f"Snowflake load triggered with pid {pid}")
			processes.add(proc)
			launched_subproc=True
	time.sleep(2) ## sleep 2 seconds so that interim uploader can sense the touched file
	os.remove(touch_file)
	if launched_subproc:
		proc.communicate()
		retcode=proc.returncode
		int_err_file=f"{LOGS_DIR}/hana_split_load_{currtime}.err"
		fsize=os.stat(int_err_file).st_size
		if retcode != 0 or fsize > 0:
			raise Exception(f"Hana interim load failed with return code {retcode}")
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Exception - {e} occurred at line {linenu} step {step_name} while downloading data")
	print("Error occurred while running to source query - " + REVSRC)
	if (os.path.exists(touch_file)):
		os.remove(touch_file)
	sys.exit(7)



