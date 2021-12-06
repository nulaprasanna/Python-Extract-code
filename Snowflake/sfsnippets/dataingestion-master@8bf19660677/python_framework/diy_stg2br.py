#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_stg2br.py                                                                                                         #
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Sep-2019                Initial Version                                                 #
# 1.1           Manick          Sep-2019                Insert DIY Master if it doesnt exist on retry for existing JCT  #
# 1.2           Manick          Sep-2019                mkdir if doesnt exist for LOGS_DIR                              #
# 1.3           Manick          Sep-2019                Use domain specific SF Role and sf warehouse                    #
# 1.4           Manick          Sep-2019                Enable mail on success feature                                  #
# 1.5           Manick          Sep-2019                Verify unique column entry piror launching Talend for merge     #
# 1.6           Manick          Sep-2019                Enable to insert attribute3 and attribute4 for stg2br           #
# 1.7           Manick          Sep-2019                Exclude unique key validation for mergeType OVERWRITE           #
# 1.8           Manick          Sep-2019                Grep for talend exception only if talend log was found          #
# 1.9           Manick          Oct-2019                Reuse existing JCT if it is in inactive mode                    #
# 2.0           Manick          Oct-2019                Frame merge type based on SRC2STG.LOAD_TYPE                     #
# 2.1           Manick          Oct-2019                Create BR table and transfer ownership to BM ROLE               #
# 2.2           Manick          Oct-2019                Read base schema/tbl information from jct if its a view/synonym #
# 2.3           Manick          Nov-2019                Use custom role for creating BR table and transfer ownership    #
# 2.4           Manick          NOv-2019                Bug with /dev/null for sending email                            #
# 2.5           Manick          Nov-2019                Grant DML to domain specific role                               #
# 2.6           Manick          Nov-2019                Switch mergetype only if whereclause is null                    #
# 2.7           Manick          Dec-2019                Enhanced email notification to MB team                          #
#                                                       Create BR_VIEW as well                                          #
# 2.8           Manick          Dec-2019                While checking unique key check in unique_key_column table also #
#                                                       Enable view creation for DQ based on refresh time               #
#                                                       Add finally clause to close all connections                     #
# 2.9           Manick          Jan-2020                Recompile BR view only for Prod                                 #
# 2.10          Manick          Jan-2020                Domain specific warehouse and dynamic WH sizing                 #
# 2.11          Manick          Jan-2020                Disable BR_VIEW creation since it pushed to src2stg             #
# 2.12          Manick          Feb-2020                Include EDW_HR_HRR_ETL_SVC for sensitive data ingestion         #
# 2.13          Manick          Feb-2020                Change talend to python for loading ss/br data                  #
# 2.14          Manick          Mar-2020                Update DIY_master to success for standalone stg2br execution    #
# 2.15          Manick          Mar-2020                Enable Append merge type to handle upsert operation             #
# 2.16          Manick          Mar-2020                Look at final db while picking warehoues size                   #
# 2.17          Manick          Apr-2020                Getrid of ejc file                                              #
# 2.18          Manick          May-2020                Dont switch merge type to Overwrite if it is Append and all_data#
# 2.19          Manick          May-2020                DOnt switch merge type for HANANRT tables                       #
# 2.20          Manick          Jun-2020                Changes for Snowflake as source                                 #
# 2.21          Manick          Jul-2020                Bug fix for SF to TD while creating STG2BR JCT                  #
# 2.22          Manick          Aug-2020                Include Data_retention_in_days while creating table             #
# 2.23          Manick          Sep-2020                Integrate with attuinity trail file upload merge                #
# 2.24          Manick          Oct-2020                Change warehouse size buckets as max wh size is large           #
# 2.25          Manick          Nov-2020                Move sf_query_tag block before filter to work for streams       #
# 2.26          Manick          Feb-2021                Validate Exception post call to DDL Generator and handle it     #
# 2.27          Manick          Feb-2021                Enhancement to autopick large wh for initial full load          #
# 2.28          Manick          Apr-2021                Remove changing warehouses inside script                        #
# 2.29          Manick          Jun-2021                New version with additional comments for easy readability       #
#########################################################################################################################
import argparse
import snowflake.connector
import cx_Oracle
import teradata
import os, re, string, subprocess, sys
from datetime import datetime
from datetime import timedelta
import time
import hvac
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from ConfigObject import ConfigObject
import faulthandler; faulthandler.enable()
import math
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import DDL_Generator
from metadata_handler import *
def logdata(logf,msg,printflag):
	currTime=datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
	if printflag == 1:
		print(currTime + " - " + msg)
	logf.write(currTime + " - " + msg + "\n")
	logf.flush()
	os.fsync(logf.fileno())
def validate_params(envType,jobStreamid,emailAdd,destSchema,mergeType,mailonSucc):
	if envType is None:
		print ("Valid environment type is mandatory. Please reinvoke the program and pass DV*/TS*/PRD as environment type")
		sys.exit(1)
	elif ('DV' not in envType and 'TS' not in envType and 'PRD' not in envType):
		print ("Valid environment type is mandatory. Please reinvoke the program and pass DV*/TS*/PRD as environment type")
		sys.exit(2)
	if jobStreamid is None:
		print("Job stream id is mandtory for type jct. Please provide JCT id which is present in repository")
		sys.exit(3)
	if emailAdd is None:
		print ("Email address is mandatory. Please provide the same using -a option (example -a mkumarap@cisco.com)")
		sys.exit(4)
	elif '@cisco.com' not in emailAdd.lower():
		print("Invalid email address. Please pass a valid email address")
	if destSchema is None:
		print("Destination DB cannot be blank. Valid values are SS or BR")
		sys.exit(5)
	if mergeType !='MERGE_ONE2ONE' and mergeType !='OVERWRITE' and mergeType !='MERGE_ACML' and mergeType != 'APPEND':
		print("Invalid mergeType. Valid values are MERGE_ONE2ONE or OVERWRITE or MERGE_ACML or APPEND")
		sys.exit(8)
	if mailonSucc is not None:
		if mailonSucc.upper() != 'YES' and mailonSucc.upper() != 'NO':
			print("Invalid value for -o mail on Success. Valid values are only Yes and No. Please check and retry")
			sys.exit(12)
def set_env():
	os.environ['LD_LIBRARY_PATH']="/usr/cisco/packages/oracle/oracle-12.1.0.2/lib"
	os.environ['ORACLE_HOME']="/usr/cisco/packages/oracle/oracle-12.1.0.2"
	os.environ['RUN_ENVIRONMENT']=envType
def sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1):
	logdata(logFile,"Sending email to " + emailAdd + " with status - " + errMsg,0)
	msg=MIMEMultipart()
	message=errMsg
	body = MIMEText(message)
	msgfrom='sf-smart-ingestion@cisco.com'
	tolist=emailAdd
	msg['Subject'] = Subject
	msg['From']    = msgfrom
	msg['To']      = tolist
	if os.stat(logFile.name).st_size > 0:
		file=logFile.name
		#msg.attach(MIMEText("Logfile.txt"))
		attachment = MIMEBase('application', 'octet-stream')
		attachment.set_payload(open(file, 'rb').read())
		encoders.encode_base64(attachment)
		attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % 'Logfile.txt')
		msg.attach(attachment)

	if os.stat(errFile.name).st_size > 0:
		file=errFile.name
		#msg.attach(MIMEText("Errorfile.txt"))
		attachment = MIMEBase('application', 'octet-stream')
		attachment.set_payload(open(file, 'rb').read())
		encoders.encode_base64(attachment)
		attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % 'Errorfile.txt')
		msg.attach(attachment)
	msg.attach(body)
	s=smtplib.SMTP("outbound.cisco.com")
	s.sendmail(msgfrom,tolist.split(","),msg.as_string())
	s.quit()
	try:
		errMsg=errMsg.replace("'","''")
		inssql="insert into DIY_email_logs (reqid,email_address,subject,errmsg,logfile,errfile,sent_time) values "
		inssql=inssql + "(" + str(reqid) + ",'" + emailAdd + "','" + Subject + "','" + errMsg + "','" + logFile.name + "','" + errFile.name + "',sysdate)"
		cursor1.execute(inssql)
		cursor1.execute('commit')
	except Exception as e:
		logdata(errFile,"Exception encountered while inserting/updating Email Log",1)
		logdata(errFile,str(e),1)
		Subject ="Error: Exception encountered while updating DIY lines"
		errMsg=Subject + "\n\n" + str(e)
		sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)


def sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject):
	logdata(logFile,"Sending email to " + emailAdd + " with status - " + errMsg,0)
	msg=MIMEMultipart()
	message=errMsg
	body = MIMEText(message)
	msgfrom='sf-smart-ingestion@cisco.com'
	tolist=emailAdd
	msg['Subject'] = Subject
	msg['From']    = msgfrom
	msg['To']      = tolist
	if os.stat(logFile.name).st_size > 0:
		file=logFile.name
		#msg.attach(MIMEText("Logfile.txt"))
		attachment = MIMEBase('application', 'octet-stream')
		attachment.set_payload(open(file, 'rb').read())
		encoders.encode_base64(attachment)
		attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % 'Logfile.txt')
		msg.attach(attachment)
	msg.attach(body)
	s=smtplib.SMTP("outbound.cisco.com")
	s.sendmail(msgfrom,tolist.split(","),msg.as_string())
	s.quit()

def invokeSSLoad(envType,jobGroup,brjobStreamId,logDir,mstrlogFile,reqid):
	os.environ["RUN_ENVIRONMENT"]=envType
	#oscmd=Talend_Scr_Path + Talend_Scr
	oscmd="/apps/edwsfdata/python/scripts/DIY_load_ssbr.py"
	osPrcmd="ps -ef | grep " + oscmd + " | grep " +  brjobStreamId + " | grep -v grep | wc -l"
	logdata(mstrlogFile,"Now executing below OS Command",0)
	logdata(mstrlogFile,oscmd + " " + jobGroup + " " + brjobStreamId,0)
	outFile=logDir + brjobStreamId + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".log"
	errFile=logDir + brjobStreamId + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".err"
	outf=open(outFile,'w')
	errf=open(errFile,'w')
	processes = set()
	processes.add(subprocess.Popen([oscmd,'-j',brjobStreamId,'-e',envType,'-r',str(reqid)],stdout=outf,stderr=errf))
	os.wait()
	for p in processes:
		while p.poll() is None:
			cnt=int(os.popen(osPrcmd).read())
			if btchsleep ==0 :
				logdata(mstrlogFile,str(cnt) + " ingestion threads are active for " + monJobstream + " ingestion stream..",1)
				btchsleep=1
			time.sleep(60)
	outf.close()
	errf.close()
def updDIYmstr(reqid,updcol,colval,cursor1,errFile,logFile,emailAdd):
	try:
		if colval =='curr_date':
			updSql="update DIY_master set " + updcol + "=sysdate,last_update_time = sysdate where reqid=" + str(reqid)
		else:
			colval=colval.replace("'","''")
			updSql="update DIY_master set " + updcol + "='" + str(colval) + "',last_update_time = sysdate where reqid=" + str(reqid)
		cursor1.execute(updSql)
		cursor1.execute("commit")
	except Exception as e:
		logdata(errFile,"Exception encountered while updating DIY Master",1)
		logdata(errFile,str(e),1)
		logdata(errFile,"Command executed is -" + updSql,0)
		Subject ="Error: Exception encountered while updating DIY Master"
		errMsg=Subject + "\n\n" + str(e)
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
def insDIYlines(reqid,prephase,currphase,cursor1,errFile,emailAdd):
	try:
		insqry="insert into DIY_lines(reqid,step_name,step_start_time) values "
		insqry=insqry + "(" + str(reqid) + ",'" + currphase + "',sysdate)" 
		cursor1.execute(insqry)
		cursor1.execute("update DIY_lines set step_end_time=sysdate where reqid=" + str(reqid) + " and step_name='" + prephase + "'")
		cursor1.execute("commit")
	except Exception as e:
		logdata(errFile,"Exception encountered while inserting/updating DIY lines",1)
		logdata(errFile,str(e),1)
		Subject ="Error: Exception encountered while updating DIY lines"
		errMsg=Subject + "\n\n" + str(e)
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)



## Main program starts here
pgmVersion='2.29'
params=[]
connstr2=''
ejcparams={}
configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
BASE_DIR=configFile.Config.BASE_DIR
ftime=datetime.now().strftime('%Y%m%d%H%M%S')
reqid=str(datetime.now().strftime('%Y%m%d%H%M%S%f'))
rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
Talend_Base=configFile.Config.Talend_Base
Talend_Scr_Path=Talend_Base + 'scripts/shell/'
Talend_Scr='STG2SS.sh'
Job_Group_Init_Scr=configFile.Config.Job_Group_Init_Scr

retryFlag=False
parser = argparse.ArgumentParser(description='Script to Move data from stg to BR/SS',
	epilog='Example: python DIY_stg2br.py -e env_type -j src2stg_job_stream_id -a email -d dest_schema -m merge_type -o mail_on_success -b customJctId')
parser.add_argument('-e', '--env',required=True,
	help='Envrionment type (TS*/DV*/PRD) of Job that is already ingested')
parser.add_argument('-j','--jobstreamid',required=True,
	help='Corresponding SRC2STG job stream for which you want to perform STG2BR/SS')
parser.add_argument('-a','--email',required=True,
	help='Comma seperated email list to which notification needs to be sent out on ingestion status')
parser.add_argument('-d','--destSchema',required=True,
	help='Destination DB where data needs to be moved/merged. Valid values or BR or SS')
parser.add_argument('-m','--mergeType',
	help='Default merge type is MERGE_ONE2ONE. Pass this parameter if you want to override')
parser.add_argument('-o','--mailOnSucc',
	help='Send an Email on succesful completion. Valid values are Yes/No')
parser.add_argument('-b','--custJct',
	help='Custom JCT ID if you want to have one of your wish.')
args=parser.parse_args()
mergeOverride=False
envType=args.env
jobStreamid=args.jobstreamid
emailAdd=args.email
destSchema=args.destSchema
mergeType=args.mergeType
mailonSucc=args.mailOnSucc
custJct=args.custJct
conn=''
conn3=''
sf_user=None
if mergeType is None:
	mergeType='MERGE_ONE2ONE'
else:
	mergeOverride=True
talendbase=BASE_DIR + '/talend'
tesbase=talendbase + '/TES'
ddlScript='/apps/edwsfdata/python/scripts/DDL_Generator.py'
try:
	if not os.path.exists(os.path.dirname(LOGS_DIR)):
		os.mkdir(LOGS_DIR)
	else:
		rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
		LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
		os.mkdir(LOGS_DIR)
except Exception as e:
	time.sleep(1)
	rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
	LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
	os.mkdir(LOGS_DIR)
	pass
logFile=open(LOGS_DIR + 'main.log','w')
errFile=open(LOGS_DIR + 'main.err','w')
set_env()
logdata(logFile,'Validating Parameters',0)
validate_params(envType,jobStreamid,emailAdd,destSchema,mergeType,mailonSucc)
logdata(logFile,'Connecting to repository db',0)
conn = open_oracle_connection(envType,None,True)
if type(conn) is int:
	logdata(errFile,"Error occurred while connecting to Repository database",1)
	Subject ="Error:" + "Error occurred while connecting to Repository database"
	errMsg=Subject
	sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)
	raise Exception(errMsg)
try:
	cursor1=conn.cursor()
	StepName="Querying JobStream"
	logdata(logFile,"Validating existance of SRC2STG JCT",0)
	sql="Select count(1) from edw_job_streams where job_stream_id='" + jobStreamid + "' and workflow_type in ('SRC2STG','FF2STG')"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	for resultsObj in results:
		edwCnt=resultsObj[0]
	if edwCnt == 0:
		logdata(errFile,"Invalid JCT ID - " + jobStreamid + ". Job Stream " + jobStreamid + " of type SRC2STG could not be located in " + envType + " environment.",1)
		logdata(errFile,sql,0)
		Subject ="Error: Invalid JCT ID - " + jobStreamid + ". Job Stream " + jobStreamid + " of type SRC2STG could not be located in " + envType + " environment."
		errMsg=Subject
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		sys.exit(9)
	StepName="Pulling existing JobStream Info"
	logdata(logFile,"Extracting required data from existing jobStream",0)
	sql="select JOB_GROUP_ID,TARGET_DB_NAME,TARGET_SCHEMA,SF_WAREHOUSE,SF_ROLE,TARGET_DB_CONNECTION,TARGET_TABLE_NAME,JOB_GROUP_ID,RUN_FREQUENCY,"
	sql+="SOURCE_DB_NAME,SOURCE_SCHEMA,SOURCE_TABLE_NAME,EXTRACT_TYPE,JOB_STREAM_DESCRIPTION,where_clause,source_db_connection,target_db_connection "
	sql=sql+ " from EDW_JOB_STREAMS where job_stream_id='" + jobStreamid + "' and workflow_type in ('SRC2STG','FF2STG')"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	for resultsObj in results:
		stgJobGroup=resultsObj[0]
		srcDb=resultsObj[1]
		srcSchema=resultsObj[2]
		sfWhse=resultsObj[3]
		sfRole=resultsObj[4]
		dbConn=resultsObj[5]
		tgtTable=resultsObj[6]
		jobGroup=resultsObj[7]
		runFreq=resultsObj[8]
		dsrcDb=resultsObj[9]
		dsrcSchema=resultsObj[10]
		dsrcTable=resultsObj[11]
		src2stgEtype=resultsObj[12]
		jctDesc=resultsObj[13]
		where_clause=resultsObj[14]
		source_db_conn=resultsObj[15]
		target_db_conn=resultsObj[16]
	if srcDb == 'TDPROD':
		itype='STG2BR'
		brtgtTable=tgtTable.replace("_STREAM","")
	else:
		brtgtTable=tgtTable
		itype='STG2' + destSchema
	StepName="Pulling Source DB Type"
	cursor1.execute(f"select distinct parameter_category from EDS_DATA_CATALOG.EDW_PARAMETER where parameter_type='{source_db_conn}'")
	results=cursor1.fetchall()
	for dbtype_obj in results:
		source_db_type=dbtype_obj[0]
	StepName="Pulling target db type"
	targetdbType=""
	sql=f"select distinct parameter_category from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='{envType}' and parameter_type ='{target_db_conn}'"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	for obj in results:
		targetdbType=obj[0]
	StepName="Pulling SF user"
	sql=f"select to_char(parameter_value) sf_username from eds_data_catalog.edw_parameter a, edw_job_streams b "
	sql+=f" where b.job_stream_Id='{jobStreamid}' and b.active_ind='Y' and b.target_db_connection=a.parameter_type"
	sql+=f" and a.parameter_category='SNOWFLAKE' and a.environment_name='{envType}' and a.parameter_name='CONNECTION_USER'"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	for results_obj in results:
		sf_user=results_obj[0]
	StepName="Checking Jobgroup"
	sql="Select count(*) from edw_job_groups where job_group_id='" + jobGroup + "'"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	for resCntobj in results:
		resCnt=resCntobj[0]
	if resCnt == 0:
		logdata(logFile,"Missing job group - " + jobGroup + " for " + envType + ". Hence creating it now",1)
		sql="insert into edw_job_groups values ('" + jobGroup + "','" + jobGroup + "','Y','P','created through API for ingesting' ,NULL,'Y','A',sysdate,'API',sysdate,'API')"
		cursor1.execute(sql)
		cursor1.execute('commit')
		logdata(logFile,"Now invoking shell script to create directory structure required for ingestion",0)
		resDir=createDirs(jobGroup,envType,Talend_Scr_Path + Job_Group_Init_Scr,LOGS_DIR)
		if resDir != 0:
			raise exception("Creating job group directories on host failed.. Hence aborting ingestion for jobstream " )
	out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'checkpoint').read()
	out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'code').read()
	out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'config/archive').read()
	out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'data/inbox').read()
	out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'data/archive').read()
	out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'logs/archive').read()
	StepName="Checking for existence of SS/BR Jobstream"
	logdata(logFile,"Checking for existence of SS/BR Jobstream",0)
	if destSchema == 'BR':
		if 'ETL' in srcDb:
			destDb=srcDb.replace('ETL','BR')
		else:
			raise Exception ("Invalid source DB - " + srcDb + ". Schema chosen is BR. however source DB is not a ETL DB")
	else:
		destDb=srcDb
	sql="select job_stream_id,run_status,job_group_id,SF_WAREHOUSE,SF_ROLE,MERGE_TYPE,active_ind,target_table_name from edw_job_streams where source_db_connection='" + dbConn + "' and source_db_name='" + srcDb + "' and source_schema='" + srcSchema + "'"
	sql=sql+ " and source_table_name='" + tgtTable + "' and active_ind='Y'"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	if len(results) > 0:
		for resultsObj in results:
			brjobStreamId=resultsObj[0]
			brRunstatus=resultsObj[1]
			jobGroup=resultsObj[2]
			sfWhse=resultsObj[3]
			sfRole=resultsObj[4]
			if not mergeOverride:
				mergeType=resultsObj[5]
			activeInd=resultsObj[6]
			brtgtTable=resultsObj[7]
		if brRunstatus == 'R' and activeInd == 'Y':
			retryFlag=True
		elif brRunstatus == 'C':
			raise Exception("STG2BR/SS Job stream with job stream id - " + brjobStreamId + " exists and is in Completed status. Please re-initialize and retry")
		if activeInd == 'N':
			logdata(logFile,"Since matching STG2SS found but in inactive state, activating and reusing the same",0)
			sql="Update EDW_JOB_STREAMS set active_ind='Y',run_status='P' where job_stream_id='" + brjobStreamId + "'"
			cursor1.execute(sql)
			cursor1.execute('commit')
		if not mergeOverride:
			if mergeType != 'APPEND':
				if src2stgEtype == "ALL_DATA" and where_clause is None:
					mergeType="OVERWRITE"
			else:
				if mergeType != 'APPEND':
					mergeType="MERGE_ONE2ONE"
		cursor1.execute("update EDW_JOB_STREAMS set merge_type='" + mergeType + "' where job_stream_id='" + brjobStreamId + "'")
		cursor1.execute("commit")
	else:
		StepName="Creating new JCT for STG2BR/SS"
		logdata(logFile,"Creating new JCT for STG2BR/SS",0)
		if custJct is not None:
			brjobStreamId=custJct
		else:
			if destSchema in ('BR','SS'):
				brjobStreamId=f'JOB_STG2{destSchema}_{destDb}_{brtgtTable}'
			else:
				brjobStreamId=f'JOB_STG2BR_{destDb}_{brtgtTable}'
		if not mergeOverride:
			if src2stgEtype == "ALL_DATA" and where_clause is None:
				mergeType="OVERWRITE"
			else:
				mergeType="MERGE_ONE2ONE"
		if srcDb == 'TDPROD':
			source_tbl_4_insert=tgtTable
			tgt_tbl_4_insert=brtgtTable
			dest_schema='STG2BR'
		else:
			source_tbl_4_insert=tgtTable
			tgt_tbl_4_insert=tgtTable
			dest_schema=f'STG2{destSchema}'

		sql="INSERT INTO EDW_JOB_STREAMS(JOB_STREAM_ID, JOB_STREAM_NAME, JOB_GROUP_ID, JOB_STREAM_SEQ_NUM, ACTIVE_IND, RUN_STATUS, WORKFLOW_TYPE, "
		sql=sql + "SOURCE_DB_CONNECTION, SOURCE_DB_NAME, SOURCE_SCHEMA, SOURCE_TABLE_NAME, EXTRACT_TYPE, MAX_VALUE_FROM, "
		sql=sql + "TARGET_DB_CONNECTION, TARGET_DB_NAME, TARGET_SCHEMA, TARGET_TABLE_NAME, SF_WAREHOUSE, SF_ROLE, "
		sql=sql + "MERGE_TYPE, JOB_STREAM_ID_LINK, SYSTEM_CALL, JOB_STREAM_DESCRIPTION, RUN_FREQUENCY) values"
		sql=sql + "('" + brjobStreamId + "','" + brjobStreamId + "','" + jobGroup + "',1,'Y','P',"
		sql=sql + "'" + dest_schema + "','" + dbConn + "','" + srcDb + "','" + srcSchema + "','" + source_tbl_4_insert  + "','ALL_DATA','SOURCE',"
		sql=sql + "'" + dbConn + "','" + destDb + "','" + destSchema + "','" + tgt_tbl_4_insert + "','" + sfWhse  + "','" + sfRole + "',"
		sql=sql + "'" + mergeType + "','" + jobStreamid + "','" + brjobStreamId + "','" + brjobStreamId + "','" + runFreq + "')"
		cursor1.execute(sql)
		cursor1.execute('commit')
	StepName="DIY Insert Update"
	#currOsUser=os.getlogin()
	currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
	if currOsUser == "":
		try:
			currOsUser=os.getlogin()
		except Exception as e:
			currOsUser=c_getuser()
			pass
	currHostName=os.uname()[1]
	if not retryFlag:
		retflg='F'
		sql="insert into DIY_master(reqid,Ingestion_type,logs_dir,env_type,job_group,source_db_name,source_schema,source_table,target_schema,"
		sql=sql + "target_table,current_phase ,start_time,retryflag,attribute1,attribute2,attribute3,attribute4,attribute5) values "
		sql=sql + " (" + reqid + ","
		sql=sql + "'" + itype.upper() + "',"
		sql=sql + "'" + LOGS_DIR + "',"
		sql=sql + "'" + envType.upper() + "',"
		sql=sql + "'" + jobGroup + "',"
		sql=sql + "'" + srcDb + "',"
		sql=sql + "'" + srcSchema + "',"
		sql=sql + "'" + tgtTable + "',"
		sql=sql + "'" + destSchema + "',"
		sql=sql + "'" + brtgtTable + "',"
		sql=sql + "'INIT',sysdate,'" + retflg + "','" + currOsUser + "','" + emailAdd + "','" + brjobStreamId + "','" + currHostName + "','" + pgmVersion + "')"
		cursor1.execute(sql)
		cursor1.execute('commit')
	else:
		sql="select max(reqid) from DIY_MASTER where source_table='" + tgtTable + "' and source_db_name='" + srcDb + "' and source_schema='" + srcSchema + "'"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		for maxreqidObj in results:
			reqid=maxreqidObj[0]
		if len(results) == 1 and reqid is not None:
			sql="update DIY_master set logs_dir='" + LOGS_DIR + "',retryflag='T',attribute1='" + currOsUser + "',attribute2='" + emailAdd + "',attribute4='" + currHostName + "',attribute5='" + pgmVersion + "' where reqid=" + str(reqid)
		else:
			sql="insert into DIY_master(reqid,Ingestion_type,logs_dir,env_type,job_group,source_db_name,source_schema,source_table,target_schema,"
			sql=sql + "target_table,current_phase ,start_time,retryflag,attribute1,attribute2,attribute3,attribute4,attribute5) values "
			sql=sql + " (" + reqid + ","
			sql=sql + "'" + itype.upper() + "',"
			sql=sql + "'" + LOGS_DIR + "',"
			sql=sql + "'" + envType.upper() + "',"
			sql=sql + "'" + jobGroup + "',"
			sql=sql + "'" + srcDb + "',"
			sql=sql + "'" + srcSchema + "',"
			sql=sql + "'" + tgtTable + "',"
			sql=sql + "'" + destSchema + "',"
			sql=sql + "'" + brtgtTable + "',"
			sql=sql + "'INIT',sysdate,'" + retflg + "','" + currOsUser + "','" + emailAdd + "','" + brjobStreamId + "','" + currHostName + "','" + pgmVersion + "')"
		cursor1.execute(sql)
		cursor1.execute('commit')
	if 'sf_query_tag' in os.environ:
		sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","STG2BR")
	else:
		sf_query_tag=f"Module=STG2BR;User={currOsUser};Table={srcDb}.{srcSchema}.{tgtTable}"
		os.environ["sf_query_tag"]=sf_query_tag
	if targetdbType == 'SNOWFLAKE':
		insDIYlines(reqid,'NONE','INIT',cursor1,errFile,emailAdd)
		StepName="Connecting to SF"
		logdata(logFile,"Connecting to Snowflake account",0)
		if configFile.Config.PRDSUFF in envType:
			sfEnv = 'cisco.us-east-1'
		elif configFile.Config.STGSUFF in envType:
			sfEnv = 'ciscostage.us-east-1'
		elif configFile.Config.DEVSUFF in envType:
			sfEnv = 'ciscodev.us-east-1'
		else:
			raise Exception('Invalid environment type. Cannot find key for ' + envType )
		if sf_user is None:
			sf_user='EDW_DATALAKE_SVC'
		conn3 = open_sf_connection(sfEnv,sf_user,sfWhse,sfRole,destDb,destSchema,query_tag=sf_query_tag)
		cursor3=conn3.cursor()
		updDIYmstr(reqid,'current_phase','SF_Connectivity_Succesful',cursor1,errFile,logFile,emailAdd)
		insDIYlines(reqid,'INIT','SF_Connectivity_Succesful',cursor1,errFile,emailAdd)
		StepName="Checking base schema and table"
		if 'srcBaseschema' in jctDesc  and 'srcBasetable' in jctDesc:
			#logdata(logFile,"Looks like base schema and base table is different from targets mentioned in JCT",0)
			srcBaseschema=jctDesc.split(',')[0].split('=')[1]
			srcBasetable=jctDesc.split(',')[1].split('=')[1]
			logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
			dsrcSchema=srcBaseschema
			dsrcTable=srcBasetable

		StepName="Checking for existence of base table"
		cursor3.execute('use database ' + destDb)
		sql="select count(1) from information_schema.tables where table_name='" + tgtTable + "' and table_schema='" + destSchema + "'"
		cursor3.execute(sql)
		results=cursor3.fetchall()
		for resultsObj in results:
			sftblCnt=resultsObj[0]
		if sftblCnt == 0:
			StepName="Creating Base table"
			logdata(logFile,"Now creating table for BR/SS before launching ss/br push workflow",0)
			if not os.path.exists(ddlScript):
				raise Exception('Could not locate DDL Generator. Hence not proceeding with STG2BR')
			sql=DDL_Generator.genDDL(dsrcDb,dsrcSchema,dsrcTable,'CREATE')
			if 'Below exception encountered' in sql:
				raise Exception(sql)
			cursor3.execute('use schema ' + destDb + '.' + destSchema)
			if destSchema == 'BR':
				sql=sql.replace(') if not exists',') DATA_RETENTION_TIME_IN_DAYS=30 if not exists')
				cursor3.execute('use role edw_create_table_on_br_role')
				cursor3.execute('use warehouse edw_datalake_wh')
				cursor3.execute('use schema ' + destDb + '.' + destSchema)
			else:
				sql=sql.replace(') if not exists',') DATA_RETENTION_TIME_IN_DAYS=7 if not exists')
			sql=sql.replace('create table ' + dsrcTable,'create table ' + tgtTable)
			create_tbl_sql=sql
			cursor3.execute(sql)
			if destSchema == 'BR':
				logdata(logFile,"Now creating BR view",0)
				sql=f"CREATE OR REPLACE SECURE VIEW {destDb}.BR_VIEW.{tgtTable} COPY GRANTS AS "
				sql+=f" SELECT * FROM {destDb}.BR.{tgtTable} WHERE NVL(EDWSF_SOURCE_DELETED_FLAG,'N')='N'"
				create_view_sql=sql
				cursor3.execute(sql)
				logdata(logFile,"Now transfering ownership of newly created BR table",0)
				stepName="Transfer BR table and view ownership to BM_ROLE per standard"
				cursor3.execute("grant insert,select,update,delete on " + destDb + "." + destSchema + "." + tgtTable + " to " + sfRole)
				cursor3.execute("call EDW_AUDIT_DB.AUDIT.BMOWNERSHIP('" + destDb + "." + destSchema + "." + tgtTable + "')")
				cursor3.execute(f"call EDW_AUDIT_DB.AUDIT.BMOWNERSHIP('{destDb}.BR_VIEW.{tgtTable}')")
				cursor3.execute('use role ' + sfRole)
				cursor3.execute('use warehouse ' + sfWhse)
				lf=open('/tmp/logfile','w')
				lf.write(f"Create table SQL \n {create_tbl_sql} \n Create view SQL \n {create_view_sql}")
				subject=f"New BR Table {destDb }.{destSchema}.{tgtTable}  in {sfEnv} got created"
				msg=f" Source Platform: {source_db_type}\n"
				msg+=f"\n Source Database: {srcDb}\n"
				msg+=f"\n Source Schema: {dsrcSchema}\n"
				msg+=f"\n Source Table: {dsrcTable}\n"
				msg+=f"\n Target Instance: {sfEnv}\n"
				msg+=f"\n Target Database: {destDb}\n"
				msg+=f"\n Target Schema: {destSchema}\n"
				msg+=f"\n Target Table: {tgtTable}\n"
				msg+=f"\n Privileges granted to role: {sfRole}\n"
				msg+=f"\n Executed by: {currOsUser}\n"
				msg+=f"\n Completed on: {datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}\n"
				sendEmail('sf-masterbuild@cisco.com',lf,lf,msg,subject,cursor1)
		StepName="Validating table"
		logdata(logFile,"Now validating the existence of table - " + destDb + "." + destSchema + "." + tgtTable ,0)
		sql="select row_count from information_schema.tables where table_name='" + tgtTable + "' and table_schema='" + destSchema + "'"
		cursor3.execute(sql)
		results=cursor3.fetchall()
		for resultsObj in results:
			stRowCnt=resultsObj[0]
		if len(results) == 0 or stRowCnt is None:
			raise Exception("Issue with table creation. " + destDb + "." + destSchema + "." + tgtTable + " could not be located.")
		updDIYmstr(reqid,'src_rows',str(stRowCnt),cursor1,errFile,logFile,emailAdd)
		StepName="Validating Unique key column"
		logdata(logFile,"Now validating existence of unique key column",0)
		updDIYmstr(reqid,'current_phase','Unique_key_validation',cursor1,errFile,logFile,emailAdd)
		insDIYlines(reqid,'SF_Connectivity_Succesful','Unique_key_validation',cursor1,errFile,emailAdd)
		if mergeType.upper() not in ('OVERWRITE','APPEND'):
			sql=f"select count(1) from EDS_DATA_CATALOG.EDW_UNIQUE_KEY a,EDS_DATA_CATALOG.EDW_UNIQUE_KEY_COLUMN b where a.table_name='{dsrcTable}' and a.environment_name='{envType}'"
			sql=sql + f" and a.db_instance_name='{dsrcDb}' and a.db_schema_name='{dsrcSchema}' and a.active_flag='A' and a.unique_key_name=b.unique_key_name "
			sql=sql + " and a.environment_name=b.environment_name and a.db_instance_name=b.db_instance_name and a.table_name=b.table_name and a.db_schema_name=b.db_schema_name"
			cursor1.execute(sql)
			results=cursor1.fetchall()
			for resultsObj in results:
				uniqCnt=resultsObj[0]
			if uniqCnt == 0:
				raise Exception("Could not locate unique key defined for " + dsrcDb + "." + dsrcSchema + "." + dsrcTable + ". Please check and define before reinvoking")
		StepName="Launching SS/BR push workflow to populate BR/SS table"
		logdata(logFile,"Now launching SS/BR push workflow to populate BR/SS table",0)
		updDIYmstr(reqid,'current_phase','Launch_SSBRPush',cursor1,errFile,logFile,emailAdd)
		insDIYlines(reqid,'Unique_key_validation','Launch_SSBRPush',cursor1,errFile,emailAdd)
		invokeSSLoad(envType,jobGroup,brjobStreamId,LOGS_DIR,logFile,reqid)
		StepName="Validate succesful SS/BR push completion"
		logdata(logFile,"Check for successfull ingestion for jobstream - " + brjobStreamId,0)
		sql="select job_stream_id from edw_job_streams where job_stream_id = '" + brjobStreamId + "' and run_status in ('P','R') and workflow_type !='SRC2STG'"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		if len(results) ==0:
			logdata(logFile,"Successful completion of data ingestion for " + destDb + "." + destSchema + "." + tgtTable,1)
		else:
			logdata(logFile,"Now locating SS/BR push Log to get exact error message",0)
			findCmd=f'ls -rt {LOGS_DIR}/{brjobStreamId}_ssbrload.err | tail -1'
			err_log=os.popen(findCmd).read().rstrip('\n')
			if (len(err_log) > 0):
				merge_error=os.popen('cat ' + err_log).read()
				logdata(errFile,'Please find below the ss/br push error',1)
				logdata(errFile,merge_error,1)
			else:
				logdata(errFile,"Could not locate ss/br push log file",0)
			raise Exception("ss/br push workflow failed for jobstream - " + brjobStreamId)
		updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
		updDIYmstr(reqid,'current_phase','Success',cursor1,errFile,logFile,emailAdd)
		insDIYlines(reqid,'Launch_SSBRPush','Completed_Ingestion',cursor1,errFile,emailAdd)
		StepName="Valdating rowcount"
		sql="select row_count from information_schema.tables where table_name='" + tgtTable + "' and table_schema='" + destSchema + "'"
		cursor3.execute(sql)
		results=cursor3.fetchall()
		for resultsObj in results:
			endRowCnt=resultsObj[0]
		rowsMerged=endRowCnt-stRowCnt
		updDIYmstr(reqid,'tgt_rows',str(endRowCnt),cursor1,errFile,logFile,emailAdd)
		succMsg="Succesfully merged " + str(rowsMerged) + " rows into " + destDb + "." + destSchema + "." + tgtTable
		Subject="Success: Succesfully merged " + str(rowsMerged) + " rows into " + destDb + "." + destSchema + "." + tgtTable
		logdata(logFile,succMsg,1)
		if mailonSucc is not None:
			if mailonSucc.upper() == 'YES':
				sendEmail(emailAdd,logFile,errFile,succMsg,Subject,cursor1)
		stepName="Check and update DIY_MASTER src2stg to success for standalone run"
	else:
		insDIYlines(reqid,'NONE','INIT',cursor1,errFile,emailAdd)
		cmnd=subprocess.Popen("/apps/edwsfdata/python/scripts/DIY_load_from_stream.py -e " + envType + " -j " + brjobStreamId + " -r " + str(reqid),stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		pid=cmnd.pid
		logdata(logFile,f"load_from_stream script launched with pid {pid}",1)
		(output, error) = cmnd.communicate()
		ret_code=cmnd.returncode
		Err=error.decode('ascii')
		Out=output.decode('ascii')
		if ret_code != 0:
			logdata(errFile,f"Exception encountered while loading from stream.\n{Out}\n{Err}",1)
			raise Exception("Exception encountered while running load from stream")
		if Err is not None and len(Err)>0:
			logdata(errFile,"Exception encountered while running load from stream. Please find below the exception \n" + Err,0)
			print(Err)
			raise Exception("Exception encountered while running load from stream")
		else:
			logdata(logFile,"Completed launching load from stream",0)
			updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
			updDIYmstr(reqid,'current_phase','Success',cursor1,errFile,logFile,emailAdd)
			insDIYlines(reqid,'INIT','Completed_Ingestion',cursor1,errFile,emailAdd)
			succMsg=f"Successfully ingested data for JCT - {brjobStreamId}"
			Subject=succMsg
			if mailonSucc is not None:
				if mailonSucc.upper() == 'YES':
					sendEmail(emailAdd,logFile,errFile,succMsg,Subject,cursor1)
			stepName="Check and update DIY_MASTER src2stg to success for standalone run"
	sql=f"select reqid,current_phase from diy_master where current_phase='SRC2STG_Success_But_STG2BR_Fail' and reqid in "
	sql+=f"(select max(reqid) from diy_master a,edw_job_streams b where a.attribute3='{jobStreamid}' and a.attribute3=b.job_stream_id and b.workflow_type in ('SRC2STG','FF2STG'))" 
	cursor1.execute(sql)
	results=cursor1.fetchall()
	if len(results) > 0:
		for results_obj in results:
			reqid_4_upd=results_obj[0]
		logdata(logFile,f"Since this is stand alone call and succeeded updating corresponding SRC2STG in DIY_MASTER ({reqid_4_upd})",0)
		sql=f"update DIY_MASTER set current_phase='Success' where reqid={reqid_4_upd}"
		cursor1.execute(sql)
		cursor1.execute("commit")
	stepName="Reset merge type in JCT"
	cursor1.execute("select extract_type from edw_job_streams where job_stream_id='" + jobStreamid + "'")
	results=cursor1.fetchall()
	for resultsObj in results:
		endEtype=resultsObj[0]
	if mergeType != 'APPEND':
		if endEtype == "ALL_DATA" and where_clause is None and source_db_type != 'HANANRT':
			mergeType="OVERWRITE"
		else:
			mergeType="MERGE_ONE2ONE"
	else:
		if mergeType != 'APPEND':
			mergeType="MERGE_ONE2ONE"
	cursor1.execute("update EDW_JOB_STREAMS set merge_type='" + mergeType + "' where job_stream_id='" + brjobStreamId + "'")
	cursor1.execute("commit")
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	logdata(errFile,f"Error occurred at line number {linenu} during step " + StepName,1)
	logdata(errFile,str(e),1)
	logdata(errFile,sql,0)
	Subject ="Error: Error occurred during step " + StepName
	errMsg=Subject + "\n\n" + str(e)
	updDIYmstr(reqid,'err_msg',errMsg,cursor1,errFile,logFile,emailAdd)
	updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
	sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
finally:
	if conn:
		conn.close()
	if conn3:
		conn3.close()