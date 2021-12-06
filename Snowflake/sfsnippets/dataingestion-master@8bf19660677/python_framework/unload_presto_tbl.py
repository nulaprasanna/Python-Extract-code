#!/apps/python/install/bin/python
#########################################################################################################################
# unload_presto_tbl.py                                                                                                  #
# Script to extract data from Presto table                                                                              #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Feb-2021                Initial version                                                 #
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
	cursor1=conn.cursor()
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	step_name="Fetching unsupported Character type"
	sql3 = "select column_name from EDS_DATA_CATALOG.EDW_TABLE_COLUMN a,diy_unsupport_datatypes b  where db_instance_name='" + dbName + "'"
	sql3 = sql3 + " and db_schema_name='" + vsrcSchema.replace('"','') + "' and table_name='" + tableName.replace('"','') + "'"
	sql3 = sql3 + " and environment_name='" + envType + "' and b.source_db_type='PRESTO' and b.column_datatype=a.data_type"
	cursor1.execute(sql3)
	unsuppRes=cursor1.fetchall()
	step_name="Establising source db connection"
	conn2 = open_presto_connection(conn,connectionName,envType,reqid)
	if type(conn2) is int:
		raise Exception ("Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'])
	cursor2=conn2.cursor()
	cursor2.arraysize=5000
	arraysize=5000
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
	BQRY=f'{SRCQRY} FROM {dbName}.{vsrcSchema}.{tableName}'
	#print ("BQRY is " + BQRY)
	if vWhere == " WHERE ":
		if vExttype == "DATE":
			vWhere +=f"cast({vInccol} as timestamp) > to_timestamp('{vTodtm}','yyyy/mm/dd hh24:mi:ss')"	
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

	outf=open(f"{LOGS_DIR}/presto_split_load_{currtime}.log",'w')
	errf=open(f"{LOGS_DIR}/presto_split_load_{currtime}.err",'w')
	touch_file=f"{LOGS_DIR}/{jct}_{currtime}"
	Path(touch_file).touch()
	processes = set()
	line_counter=0
	launched_subproc=False
	is_data=True
	while is_data:
		res=cursor2.fetchmany(arraysize)
		if len(res) == 0:
			is_data=False
		for row_data in res: # add table rows
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
		int_err_file=f"{LOGS_DIR}/presto_split_load_{currtime}.err"
		fsize=os.stat(int_err_file).st_size
		if retcode != 0 or fsize > 0:
			raise Exception(f"Presto interim load failed with return code {retcode}")
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Exception - {e} occurred at line {linenu} step {step_name} while downloading data")
	print("Error occurred while running to source query - " + REVSRC)
	if (os.path.exists(touch_file)):
		os.remove(touch_file)
	sys.exit(7)



