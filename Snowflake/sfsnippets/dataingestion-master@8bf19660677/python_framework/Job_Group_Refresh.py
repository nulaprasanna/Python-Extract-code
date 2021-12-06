#!/apps/python/install/bin/python
#########################################################################################################################
# Job_Group_Refresh.py                                                                                                  #
# 1.1 insetead of hard coding pick from SRC2STG JCT                                                                     #
# 1.2 Enable restartability for job group refresh and consolidate report on completion for all JCTs on the group        #
# 1.3 Enable DPI_DEBUG to trace oracle sqls from SRC2STG and STG2BR scripts                                             #
# 1.4 Enable retry of jobstreams and exit if either SRC2STG or STG2BR remains unprocessed                               #
# 1.5 Initial retry of job_group_refresh even if src2stg doesnt exist                                                   #
# 1.6 Check for logs file directory existence                                                                           #
# 1.7 Remove -d argument from src2stg                                                                                   #
# 1.8 Return success if nothing to process                                                                              #
# 1.9 Handle special parmeters from special param table                                                                 #
# 1.10Enable more debug to find connect reset message                                                                   #
# 1.11Disable summary email                                                                                             #
# 1.12 New version with additional comments for easy readability                                                        #
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
import pull_refresh_summary
from metadata_handler import *
def logdata(logf,msg,printflag):
	currTime=datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
	if printflag == 1:
		print(currTime + " - " + msg)
	logf.write(currTime + " - " + msg + "\n")
	logf.flush()
	os.fsync(logf.fileno())
## Main Program starts here
parser = argparse.ArgumentParser(description='Script used for refreshing a job group',
    epilog='Example: python DIY.py -j job_group -t Number_of_parallel_threads -a email -e envName')
parser.add_argument('-j', '--jobGroup',required=True,
    help='Job group whose data needs to be refreshed ')
parser.add_argument('-t','--threadCnt',required=True,
	help='Number of parallel threads you wish to use')
parser.add_argument('-a','--emailAdd',required=True,
	help='Email address to notify')
parser.add_argument('-e','--envName',default='DV3',
	help='Environment where you want to trigger ingestion. Default is DV3')
args=parser.parse_args()
jobGroup=args.jobGroup
threadCnt=args.threadCnt
emailAdd=args.emailAdd
envName=args.envName
ejcparams={}
ftime=datetime.now().strftime('%Y%m%d%H%M%S')
configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
ErrFlag=False

try:
	try:
		## Create logs directory to store refresh log
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
	if envName is None or envName == '':
		envName='DV3'
	## Validate passed thread count.. Move this number to config file in next release
	if int(threadCnt) > 50:
		raise Exception ("Invalid thread count. Only a max of 50 threads are allowed")
	## Establish connection to ODS repositor database
	stepName="Connect to repos"
	set_env()
	os.environ['RUN_ENVIRONMENT']=envName
	oscmd="/apps/edwsfdata/python/scripts/DIY_src2stg.py"
	#os.environ['DPI_DEBUG_LEVEL']="16"
	#conn = cx_Oracle.connect(user=ejcparams['EJC_LOGIN'], password=ejcparams['EJC_PASSWORD'], dsn=connstr)
	conn = open_oracle_connection(envName,None,True)
	cursor1 = conn.cursor()
	## Validate passed job group
	stepName="Validating Jobgroup"
	## Regular expression is used while querying job_streams to avoid picking child job threads if any that remains unprocessed from previous failure run
	sql="SELECT job_stream_name,source_db_name,source_schema from EDW_JOB_STREAMS where ((run_status in ('A','P')) or (run_status='R' "
	sql = sql + " and not regexp_like (JOB_STREAM_ID,'_([0-9]{14}.+[0-9]$)'))) "
	sql = sql + " and job_group_id='" + jobGroup +"' and active_ind='Y' and workflow_type='SRC2STG'"
	jobcnt_sql=sql.replace("workflow_type='SRC2STG'","workflow_type in ('SRC2STG','STG2BR','STG2SS','STG2ACML')")
	cursor1.execute(jobcnt_sql)
	jobcnt_res=cursor1.fetchall()
	jobcnt=len(jobcnt_res) ## denotes total number of jobs both SRC2STG and STG2SS/BR
	cursor1.execute(sql)
	jctResults=cursor1.fetchall()
	jctCnt=len(jctResults) ## denotes only SRC2STG job count since that is starting point. STG2BR will internally invoked by SRC2STG workflow.
	## Insert record into DIY_JOB_GROUP_REFRESH table
	insSql="insert into diy_job_group_refresh(reqid,job_group,init_job_stream_count,final_job_stream_count,start_time) values "
	insSql += "(" + str(rftime) + ",'" + jobGroup + "'," + str(jobcnt) + "," + str(jobcnt) + ",sysdate)"
	cursor1.execute(insSql)
	cursor1.execute("commit")
	if jobcnt == 0:
		print ( "Either job group " + jobGroup + " is invalid or no valid jct exist in job group " + jobGroup + " for processing" )
		sys.exit(0)
	if jctCnt > 0:
		outf=open(LOGS_DIR + 'Job_Group_Refresh_' + ftime + '.log','w')
		errf=open(LOGS_DIR + 'Job_Group_Refresh_' + ftime + '.err','w')
		processes = set()
		prcnt=0
		jobStreamarr=[]
		logdata(logFile,"Job group " + jobGroup + " has " + str(jctCnt) + " SRC2SRG JCT for processing.. Processing the same using " + str(threadCnt) + " parallel threads",1)
		for jctObj in jctResults:
			jctName=jctObj[0]
			srcDb=jctObj[1]
			srcSchema=jctObj[2]
			prcnt += 1
			jobStreamarr.append(jctName)
			custParam=''
			## Check for any custom parameter that needs to be used while triggering the job 
			cursor1.execute("select argument_text from DIY_JCT_CUSTOM_PARAMS where job_stream_id='" + jctName + "' and env_type='" + envName + "' and active_ind='Y'")
			results=cursor1.fetchall()
			for resultsObj in results:
				custParam = resultsObj[0]
			if len(custParam) > 0:
				logdata(logFile,"Now launching python framework for job " + jctName + " with custom parameter " + custParam,1)
				## Launch SRC2STG process in back ground
				processes.add(subprocess.Popen([oscmd,'-i','jct','-e',envName,'-a',emailAdd,'-j',jctName,custParam],stdout=outf,stderr=errf))
			else:
				logdata(logFile,"Now launching python framework for job " + jctName,1)
				processes.add(subprocess.Popen([oscmd,'-i','jct','-e',envName,'-a',emailAdd,'-j',jctName],stdout=outf,stderr=errf))
			logdata (logFile,"Launched " + str(prcnt) + " processes out of " + str(jctCnt) + " total process to submit",1)
			## Check number of thread counts passed as parameter vs number of background jobs launched.
			while int(len(processes)) >= int(threadCnt):
				os.wait()
				time.sleep(10) ## 10 seconds sleep before checking for process status
				processes.difference_update(
				 [p for p in processes if p.poll() is not None])
			time.sleep(5) ## Sleeping 5 seconds between each parallel submission
		for p in processes:
			btchsleep=0
			totsleeptime=0
			while p.poll() is None:
				if btchsleep ==0 :
					btchsleep=1
				time.sleep(60)
				totsleeptime+=60
		outf.close()
		errf.close()
	## Check if still any job from the job group remains unprocessed.
	sql="SELECT job_stream_name,source_db_name,source_schema,workflow_type,source_table_name,target_schema from EDW_JOB_STREAMS where ((run_status in ('A','P')) or (run_status='R' "
	sql = sql + " and not regexp_like (JOB_STREAM_ID,'_([0-9]{14}.+[0-9]$)'))) "
	sql = sql + " and job_group_id='" + jobGroup +"' and active_ind='Y' and workflow_type in ('SRC2STG','STG2BR','STG2SS','STG2ACML') order by workflow_type"
	cursor1.execute(sql)
	post_results=cursor1.fetchall()
	jctCnt=len(post_results)
	if len(post_results) > 0:
		logdata(logFile,"Still some job remains unprocessed. Hence processing the same",1)
		outf=open(LOGS_DIR + 'Job_Group_Refresh_retry_' + ftime + '.log','w')
		errf=open(LOGS_DIR + 'Job_Group_Refresh_retry_' + ftime + '.err','w')
		processes = set()
		prcnt=0
		launched_jct=[]
		for row in post_results:
			prcnt += 1
			## Check if retry job is of type SRC2STG or STG2BR. For instance of SRC2STG succeeded but STG2BR alone failed only STG2BR needs to be processed.
			if row[3] == "SRC2STG":
				jctName=row[0]
				processes.add(subprocess.Popen([oscmd,'-i','jct','-e',envName,'-a',emailAdd,'-j',jctName],stdout=outf,stderr=errf))
				logdata (logFile,"Launched " + str(prcnt) + " processes out of " + str(jctCnt) + " total process to submit",1)
				launched_jct.append(jctName)
			else:
				retrycmd="/apps/edwsfdata/python/scripts/DIY_stg2br.py"
				srcdb=row[1]
				srcschema=row[2]
				srctbl=row[4]
				tgtschema=row[5]
				sql2=f"select job_stream_id,source_db_name,source_schema from edw_job_streams where job_group_id='{jobGroup}' and target_db_name='{srcdb}' and target_schema='{srcschema}'"
				sql2+=f" and target_table_name='{srctbl}' and active_ind='Y'"
				cursor1.execute(sql2)
				br_results=cursor1.fetchall()
				br_jct=br_results[0][0]
				if br_jct not in launched_jct:
					processes.add(subprocess.Popen([retrycmd,'-j',br_jct,'-e',envName,'-a',emailAdd,'-d',tgtschema],stdout=outf,stderr=errf))
					logdata (logFile,"Launched " + str(prcnt) + " processes out of " + str(jctCnt) + " total process to submit",1)
				else:
					continue
			while int(len(processes)) >= int(threadCnt):
				os.wait()
				time.sleep(10) ## 10 seconds sleep before checking for process status
				processes.difference_update(
				 [p for p in processes if p.poll() is not None])
			time.sleep(5) ## Sleeping 5 seconds between each parallel submission
		for p in processes:
			btchsleep=0
			totsleeptime=0
			while p.poll() is None:
				if btchsleep ==0 :
					btchsleep=1
				time.sleep(60)
				totsleeptime+=60
		cursor1.execute(sql)
		post_results=cursor1.fetchall()
	postJctCnt=len(post_results)
	## Update DIY_JOB_GROUP_REFRESH with final job stream count which remains unprocessed
	cursor1.execute("update diy_job_group_refresh set final_job_stream_count=" + str(postJctCnt) + ", end_time=sysdate where reqid=" + str(rftime))
	cursor1.execute("commit")
	if postJctCnt != 0:
		logdata(logFile,"Job group " + jobGroup + " had " + str(jctCnt) + " jobs. However only " + str(jctCnt-postJctCnt) + " jobs succeeded. Please check log and retry to reprocess failed jobs",1)
		ErrFlag=True
		#sys.exit(1)
	else:
		logdata(logFile,"Successful refresh of JOb group " + jobGroup + ". All " + str(jctCnt) + " jobs got completed succesfully",1)
	#stepName="Pulling refresh summary"
	#pull_refresh_summary.pull_job_group_refresh_summary(jobGroup,envName,LOGS_DIR,rftime)
	#if os.stat(LOGS_DIR + 'out.html').st_size > 0:
	#	stepName="Sending Email"
	#	pull_refresh_summary.sendHtmEmail(emailAdd,LOGS_DIR + 'out.html',"Job group refresh summary for - " + jobGroup,jobGroup)
	#else:
	#	logdata(logFile,"HTML report generation for job group refresh - " + jobGroup + " failed. Please check",1)
	stepName="Closing files"
	logFile.close()
	errFile.close()
	if ErrFlag:
		sys.exit(99)
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	logdata (errFile,f"Error encountered at line {linenu} during {stepName}.. Exception message given below",1)
	logdata(errFile,str(e),1)
	errFile.close()
	sys.exit(98)
		

