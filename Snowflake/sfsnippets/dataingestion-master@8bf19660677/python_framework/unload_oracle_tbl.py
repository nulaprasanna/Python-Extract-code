#!/apps/python/install/bin/python
import sys,pdb
import csv
import cx_Oracle
import os,subprocess
import math,decimal,psutil,time
from datetime import datetime
from ConfigObject import ConfigObject
from pathlib import Path
from metadata_handler import *

def OutConverter(value):
    if len(value) > 16000000:
        return value[0:16000000]
    else:
        return value

def LobtoStr(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize,outconverter=OutConverter)
    if defaultType == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.NUMBER:
        return cursor.var(decimal.Decimal, arraysize=cursor.arraysize)

## Main program starts here
os.environ['LD_LIBRARY_PATH']="/usr/cisco/packages/oracle/oracle-12.1.0.2/lib"
os.environ['ORACLE_HOME']="/usr/cisco/packages/oracle/oracle-12.1.0.2"
os.environ['TNS_ADMIN']="/apps/edwsfdata/python/network/admin"
configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
BASE_DIR=configFile.Config.BASE_DIR
talendbase=BASE_DIR + '/talend'
envType=os.environ['RUN_ENVIRONMENT']
sourcedbType='ORACLE'
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
ejcparams={}
if len(vWhere.split("'")) > 5:
	vWhere=vWhere.replace("''","'''") ## replace if single quote part of filter and not empty filter like ''
## Connect to repository
try:
	print('Connecting to repository db')
	conn = open_oracle_connection(envType,None,True)
except Exception as e:
	print ("Error occurred while connecting to Repository database")
	print(str(e))
	sys.exit(2)

## Extract target connection details
try:
	srcdbParams={}
	cursor1=conn.cursor()
	print('Gathering source db connection detais from repository')
	sql1="select parameter_name,to_char(parameter_value) from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "' and upper(parameter_category)='" + sourcedbType.upper() + "' and parameter_type ='" + connectionName + "'"
	#print(sql1)
	cursor1.execute(sql1)
	results=cursor1.fetchall()
	if len(results) == 0:
		print ("Could not find connection details for connection name - " + connectionName.strip('\n') + " for DB type - " + sourcedbType + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion")
		sys.exit(3)
	for obj in results:
		param=obj[0]
		value=obj[1]
		srcdbParams[param]=value
	#print(srcdbParams)
except  Exception as e:
	print("Exception occurred while selecting source db connection details")
	print(str(e))
	sys.exit(4)

try:
	sql2="SELECT UTL_I18N.RAW_TO_CHAR('"+ srcdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') from dual"
	cursor1.execute(sql2)
	results=cursor1.fetchall()
	for passobj in results:
		srcPassword=passobj[0]
except Exception as e:
	print ("Exception occurred while decoding source login password")
	print(str(e))
	sys.exit(5)
## Check for any LOB columns
## Check for any VARCHAR columns to overcome ORA-29275: partial multibyte character issue
try:
	sql3 = "select column_name from EDS_DATA_CATALOG.EDW_TABLE_COLUMN where db_instance_name='" + dbName + "'"
	sql3 = sql3 + " and db_schema_name='" + vsrcSchema.replace('"','') + "' and table_name='" + tableName.replace('"','') + "' and data_type='VARCHAR2' "
	sql3 = sql3 + " and environment_name='" + envType + "' and active_flag='A'"
	cursor1.execute(sql3)
	varcharres=cursor1.fetchall()
except Exception as e:
	print ("Exception occurred while checking for existence of VARCHAR2 column")
	print(str(e))
	sys.exit(6)
## Check for any unsupported data types to overcome __str__ returned non-string (type bytes) issue
try:
	sql3 = "select column_name from EDS_DATA_CATALOG.EDW_TABLE_COLUMN a,diy_unsupport_datatypes b  where db_instance_name='" + dbName + "'"
	sql3 = sql3 + " and db_schema_name='" + vsrcSchema.replace('"','') + "' and table_name='" + tableName.replace('"','') + "'"
	sql3 = sql3 + " and environment_name='" + envType + "' and b.source_db_type='ORACLE' and b.column_datatype=a.data_type"
	cursor1.execute(sql3)
	unsuppRes=cursor1.fetchall()
except Exception as e:
	print ("Exception occurred while checking for Unsupported data type")
	print(str(e))
	sys.exit(6)
## Connection to target db
try:
	connstr3 = cx_Oracle.makedsn(srcdbParams['SOURCE_HOST'], srcdbParams['SOURCE_PORT'],service_name=srcdbParams['SOURCE_SERVICE_NAME'])
	#conn3 = cx_Oracle.connect(user=srcdbParams['SOURCE_LOGIN'], password=srcPassword, dsn=connstr3)
	conn3 = cx_Oracle.connect(srcdbParams['SOURCE_LOGIN'], srcPassword, srcdbParams['SOURCE_SERVICE_NAME'])
	cursor3=conn3.cursor()
	cursor3.execute("select value from v$nls_parameters where parameter='NLS_CHARACTERSET'")
	results=cursor3.fetchall()
	for resultsObj in results:
		vCharSet=resultsObj[0]
	if vCharSet == "AL32UTF8":
		srcEncode="UTF-16"
	else:
		srcEncode="UTF-8"
	cursor3.close()
	conn3.close()
except Exception as e:
	print("Error occurred while Verifying source db Characterset " + srcdbParams['SOURCE_HOST'])
	print (str(e))
	sys.exit(6)
try:
	connstr2 = cx_Oracle.makedsn(srcdbParams['SOURCE_HOST'], srcdbParams['SOURCE_PORT'],service_name=srcdbParams['SOURCE_SERVICE_NAME'])
	#conn2 = cx_Oracle.connect(user=srcdbParams['SOURCE_LOGIN'], password=srcPassword, dsn=connstr2,encoding=srcEncode, nencoding="UTF-16")
	conn2 = cx_Oracle.connect(srcdbParams['SOURCE_LOGIN'],srcPassword,srcdbParams['SOURCE_SERVICE_NAME'],encoding=srcEncode, nencoding="UTF-16")
	cursor2=conn2.cursor()
	cursor2.arraysize=5000
	cursor2.outputtypehandler=LobtoStr
except Exception as e:
	print("Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'])
	print (str(e))
	sys.exit(6)

printHeader = False # include column headers in each table output



# output each table content to a separate CSV file
vsrcQuery=vsrcQuery + str(vBatch) + " as EDWSF_BATCH_ID ,"
SRCQRY=vsrcQuery.rstrip(',') ## strip last comma
#print ("SRCQRY is " + SRCQRY)
#BQRY=SRCQRY + " FROM " + vsrcSchema + "." + tableName
BQRY=f'{SRCQRY} FROM {vsrcSchema}.{tableName}'
#print ("BQRY is " + BQRY)
if vWhere == " WHERE ":
	if vExttype == "DATE":
		vWhere = vWhere + vInccol + " > to_date('" + vTodtm + "','YYYY/MM/DD HH24:MI:SS')"	
		REVSRC = BQRY + vWhere
	elif vExttype == "ID":
		vWhere = vWhere + vInccol + " > " + str(vToid)
		REVSRC = BQRY + vWhere
elif (len(vWhere) > 7):
	if vExttype == "DATE":
		if vInccol not in vWhere: 
			vWhere = f"{vWhere} AND {vInccol}  > to_date('{vTodtm}','YYYY/MM/DD HH24:MI:SS')"	
	elif vExttype == "ID":
			if vInccol not in vWhere: 
				vWhere = f"{vWhere} AND {vInccol} > {vToid}"
	REVSRC = BQRY + vWhere
else:
	REVSRC = BQRY
#print("REVSRC is " + REVSRC)
try:
	
	try:
		if len(varcharres) > 0:
			for varcharobj in varcharres:
				colName=varcharobj[0]
				## Change done to take care of ORA-01489: result of string concatenation is too long
				sql4="select count(1) from all_tab_columns where table_name='" + tableName.replace('"','') + "' and owner='" + vsrcSchema.replace('"','') + "' and column_name='" + colName + "' and char_length<4000"
				cursor2.execute(sql4)
				results=cursor2.fetchall()
				varLen=1
				for varLenObj in results:
					varLen=varLenObj[0]
				if varLen == 1:
					revCol=f"rtrim(\"{colName}\" || ' ',' ')"
					#revCol="rtrim(" + colName + " || ' ',' ')"
					## find replace original clob with changed column name
					#REVSRC = REVSRC.replace("," + colName + " " ,"," + revCol + " ")
					REVSRC = REVSRC.replace(',"' + colName + '"' ,',' + revCol + ' ')
	except Exception as e:
		print("Error occurred while framing varchar query")
		print (str(e))
		sys.exit(8)	
	try:
		currtime=datetime.now().strftime('%Y%m%d%H%M%S')
		file_cnt=1
		csv_file_dest = f"{vPath}{vFile}_{currtime}_{file_cnt}" 
		outputFile = open(csv_file_dest,'w') # 'wb'
		output = csv.writer(outputFile, dialect='excel',delimiter=vDelim,quotechar='"',quoting=csv.QUOTE_ALL)
		if len(unsuppRes) > 0:
			for unsuppobj in unsuppRes:
				colName=unsuppobj[0]
				revCol="NULL"
				## find replace original clob with changed column name
				#REVSRC = REVSRC.replace(',\"' + colName + '\" AS ' , "," + revCol + " AS ")
				REVSRC = REVSRC.replace('"' + colName + '"' , revCol + " AS " + colName)
	except Exception as e:
		print("Error occurred while framing unsupported data type query")
		print (str(e))
		sys.exit(8)	
	print("REVSRC is " + REVSRC)
	cursor2.execute(REVSRC)

	if printHeader: # add column headers if requested
			cols = []
			for col in cursor2.description:
					cols.append(col[0])
			output.writerow(cols)
	outf=open(f"{LOGS_DIR}/oracle_split_load_{currtime}.log",'w')
	errf=open(f"{LOGS_DIR}/oracle_split_load_{currtime}.err",'w')
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
						launched_subproc=True
						print(f"Snowflake load triggered with pid {pid}")
						processes.add(proc)
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
			launched_subproc=True
			print(f"Snowflake load triggered with pid {pid}")
			processes.add(proc)
	time.sleep(2) ## sleep 2 seconds so that interim uploader can sense the touched file
	os.remove(touch_file)
	if launched_subproc:
		proc.communicate()
		retcode=proc.returncode
		int_err_file=f"{LOGS_DIR}/oracle_split_load_{currtime}.err"
		fsize=os.stat(int_err_file).st_size
		if retcode != 0 or fsize > 0:
			raise Exception(f"Oracle interim load failed with return code {retcode}")
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Error occurred at line number {linenu} while running source query - {REVSRC}")
	print (str(e))
	if (os.path.exists(touch_file)):
		os.remove(touch_file)
	sys.exit(7)


