#!/apps/python/install/bin/python
#########################################################################################################################
# DIY_src2stg.py                                                                                                        #
# Script to perform Data Ingestion for Snowflake                                                                        #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Apr-2019                Initial Version                                                 #
# 1.1           Manick          May-2019                Revised split logic                                             #
# 1.2           Manick          May-2019                Add Trim while extracting filter from TD                        #
# 1.3           Manick          May-2019                Derive base table from 1-1 view for TD Data dictionary queries  #
# 1.4           Manick          May-2019                Enable JCT entry and invoke ingestion in parallel               #
# 1.5           Manick          May-2019                Write JCT entires into a file before execution                  #
# 1.6           Manick          May-2019                Insert generated JCT sql file                                   #
# 1.7           Manick          May-2019                Enable Runlog                                                   #
# 1.8           Manick          May-2019                Invoke Talend for ingestion                                     #
#                                                       Append datetime to master record for easy reprocessing          #
# 1.9           Manick          May-2019                Bug fix with incremental logic if record is huge                #
# 1.10          Manick          May-2019                Check for job group existing and create if not                  #
# 1.11          Manick          May-2019                Option to override thread count                                 #
# 1.12          Manick          May-2019                Bug fix to use large table split columns only as int & date     #
# 1.13          Manick          May-2019                Integrate SF code to merge & create stage table from slices     #
# 1.14          Manick          May-2019                Convert all hard coded params to read from config file          #
# 1.15          Manick          May-2019                Bug fix for incremental ingestion from JCT                      #
# 1.16          Manick          May-2019                Implement logic to update incremental column details            #
# 1.17          Manick          May-2019                Bug fix for incremental column update                           #
# 1.18          Manick          May-2019                Change dbc.indices to indicesv bug fix                          #
# 1.19          Manick          May-2019                Bug fix for large table with no index                           #
# 2.0           Manick          May-2019                Include Oracle logic                                            #
# 2.1           Manick          May-2019                Fix Missing logic for retry since upd inc is failing            #
# 2.2           Manick          May-2019                Bug fix with exception inside index column loop                 #
#                                                       Bug fix with long running oracle index identification query     #
#                                                       Flush logfile content to see then and their                     #
# 2.3           Manick          May-2019                Enable thread count logic to restrict number of parallel proc   #
# 2.4           Manick          May-2019                Fix bug with retry flag for multi threaded runs                 #
# 2.5           Manick          May-2019                Bug fix on updating last extract date for Oracle                #
# 2.6           Manick          May-2019                Fix bug with process submission in parallel                     #
# 2.7           Manick          May-2019                Change SF connections to use token from keeeper                 #
# 2.8           Manick          May-2019                Check and abort if metadata doesnt exist                        #
# 2.9           Manick          May-2019                Enable metadata collection for Oracle & TD db                   #
# 3.0           Manick          May-2019                Impl. of revolutionary logic on oracle splitting logic          #
# 3.1           Manick          May-2019                Treat both part & non-part oracle table same due to bug         #
# 3.2           Manick          May-2019                Bug fix with missing quote for date based sliced tables         #
# 3.3           Manick          May-2019                Bug fix with overalapping sequence post de-norm                 #
# 3.4           Manick          May-2019                Bug fix with missing schema in SF during merge process          #
# 3.5           Manick          May-2019                Remove whitespaces from input file                              #
# 3.6           Manick          May-2019                Fix bug with date addition while creating buckets               #
# 3.7           Manick          May-2019                Bug fix with reading source count from flat file type conver    #
# 3.8           Manick          May-2019                Bug fix with meta data collection invocation script             #
# 3.9           Manick          May-2019                Bug fix with timestamp range missing paranthesis                #
# 3.10          Manick          May-2019                Bug fix while checking completed jobs by removing %             #
# 3.11          Manick          May-2019                Bug fix to include Decimal type for TD                          #
# 3.12          Manick          May-2019                Bug fix for existing table with null for incremental            #
# 3.13          Manick          May-2019                Bug fix with data type while finding oracle indiex              #
# 3.14          Manick          May-2019                Bug fix when filter exists for history load                     #
# 3.15          Manick          May-2019                Bug fix when filter has split then filter is coming twice in JCT#
# 3.16          Manick          May-2019                Bug fix with thread count parameter override                    #
# 3.17          Manick          Jun-2019                Introduce split count to override program defined threadcount   #
# 3.18          Manick          Jun-2019                Bug fix with number of pieces on retry failed job               #
# 3.19          Manick          Jun-2019                Bug fix when range value comes as null                          #
# 3.20          Manick          Jun-2019                Bug fix to collect metadata against TD Tbl and not view         #
# 3.21          Manick          Jun-2019                Change in logic to handle oracle table with LOB columns         #
# 3.22          Manick          Jun-2019                Change JCT status at the end of ingestion                       #
# 3.23          Manick          Jun-2019                Add status code A while checking retrying existing JCT          #
# 3.24          Manick          Jun-2019                Re-use existing JCT entries for subsequent large ingestions     #
# 3.25          Manick          Jun-2019                Bug while upd incremental column for TD                         #
# 3.26          Manick          Jun-2019                Update JCT Inc only if it is null                               #
# 3.27          Manick          Jun-2019                Modify mount point check to consider thread and split count     #
# 3.28          Manick          Jun-2019                Bug with trim function while using timestamp columne            #
# 3.29          Manick          Jun-2019                Disable special processing for LOB since it is fixed at Talend  #
# 3.30          Manick          Jun-2019                Bug fix with threadcount for date based ingestion               #
# 3.31          Manick          Jun-2019                Option to override split column                                 #
# 3.32          Manick          Jun-2019                Update incremental from child to master only if child exists    #
# 3.33          Manick          Jun-2019                Bug fix on wrong record picked up for reprocessing              #
# 3.34          Manick          Jun-2019                Bug fix with job stream monitoring                              #
# 3.35          Manick          Jul-2019                Leave master JCT record with status code C so TES wont fail     #
# 3.36          Manick          Jul-2019                Update script to read SF token from config file                 #
# 3.37          Manick          Jul-2019                Include tbl paritition while finding tbl size to fix float error#
# 3.38          Manick          Aug-2019                Remove additional date filter in case of oracle for consistency #
# 4.0           Manick          Aug-2019                Modification to include master log, tblsize and tpt             #
# 4.1           Manick          Aug-2019                Capture exception inside SF merge exception process             #
# 4.2           Manick          Aug-2019                Include retry for child with 'P' and update job_group           #
# 4.3           Manick          Aug-2019                Bug fixed with elapsed days for TD with 7453 error              #
# 4.4           Manick          Aug-2019                Bug fix with job stream check post talend                       #
# 4.5           Manick          Aug-2019                Ping Oracle db connection inside invoketalend to keep active    #
# 4.6           Manick          Aug-2019                Get os user who submitted the job and modify thread count for TD#
# 4.7           Manick          Aug-2019                Bug fix with looping when issue encountered during min/max      #
# 4.8           Manick          Aug-2019                Bug fix with date data type split for TD wrt format             #
# 4.9           Manick          Aug-2019                Update existing DIY_Master for retry                            #
# 4.10          Manick          Aug-2019                While querying JCT check for workflow type                      #		
# 4.11          Manick          Aug-2019                On retry check nopieces based on active flag                    #
#                                                       Include exception while logging err_msg in DIY_master           #
#                                                       Convert count(*) to bigint incase of TD                         #
# 4.12          Manick          Aug-2019                Bug on incremental ingestion of large batch remove unwanted and #
# 4.13          Manick          Aug-2019                Add incremental column as part of JCT where after split         #
# 4.14          Manick          Aug-2019                Scan for ACTIVE_IND = Y before querying JCT for table ingestion #
# 4.15          Manick          Aug-2019                Add -j to trigger ingestion based on job stream id              #
#                                                       Bug fix to include 'A' while checking for retry                 #
#                                                       Bug fix to handle None when source oracel tbl has no stats      #
# 4.16          Manick          Aug-2019                Bug fix with noPieces on retry of a single thread job           #
# 4.17          Manick          Sep-2019                Enable email notification                                       #
# 4.18          Manick          Sep-2019                Capture email address and audit of emails                       #
# 4.19          Manick          Sep-2019                Bug with multiple jcts for incremental                          #
# 4.20          Manick          Sep-2019                Use temp job group during ingestion and reset once it is done   #
# 4.21          Manick          Sep-2019                Use qualify to get latest stats in case of TD for rowcount      #
# 4.22          Manick          Sep-2019                Add additional argument to push data directly to SS/BR          #
# 4.23          Manick          Sep-2019                Bug with split logic with thread override                       #
# 4.24          Manick          Sep-2019                Enable varchar split for oracle if unique and use non-null cols #
# 4.25          Manick          Sep-2019                Disable job group only if it is history load                    #
# 4.26          Manick          Sep-2019                Bug fix with existing job group for single thread ingestion     #
# 4.27          Manick          Sep-2019                mkdir if doesnt exist for LOGS_DIR                              #
# 4.28          Manick          Sep-2019                Bug fix with source column format type for date                 #
# 4.29          Manick          Sep-2019                Capture role as a parameter and use that role for SF ingestion  #
# 4.30          Manick          Sep-2019                Bug with ijobGroup      for job retry                           #
# 4.31          Manick          Sep-2019                Enable Varchar field in split column search for TD              #
# 4.32          Manick          Sep-2019                Bug fix on locating talend log file                             #
# 4.33          Manick          Sep-2019                While checking metadata check if datatype is not null           #
# 4.34          Manick          Sep-2019                Read source rows from table                                     #
# 4.35          Manick          Sep-2019                Launch STG2BR added in DIY master and lines                     #
# 4.36          Manick          Sep-2019                Enabled TIMESTAMP for oracle inc column and format to get data  #
# 4.37          Manick          Sep-2019                Exclude warehouse derivation for REPLICATION_POC_WH             #
# 4.38          Manick          Sep-2019                Bug with finding FATAL error from Talend exception              #
# 4.39          Manick          Sep-2019                Handle Varchar index column to make it distinct if collides     #
# 4.40          Manick          Sep-2019                Validate incremental column name & value and send msg if null   #
# 4.41          Manick          Sep-2019                bug fix with thread/split cnt and only non null cmn for split   #
# 4.42          Manick          Sep-2019                Bug fix with talend log between FF2SfSTG and TDSrc2FF/OraSrc2FF #
#                                                       Fix split step entry only for threadcount > 1                   #
# 4.43          Manick          Sep-2019                Bug fix with EDW_DATALAKE_WH while deriving WH from ROLE        #
# 4.44          Manick          Sep-2019                Add new logic to check nulls manually for oracle for split      #
#                                                       filter for varchar missing on > chunk_treshold block            #
# 4.45          Manick          Oct-2019                Add active_ind='Y' while querying child stream for compeltion   #
# 4.46          Manick          Oct-2019                Throttle down TD parallel sessions based on table size          #
#                                                       change variable name threadcount to splitcount for ease         #
# 4.47          Manick          Oct-2019                Bug fix with srcIndcol logic for checking nulls                 #
# 4.48          Manick          Oct-2019                Enable parallelism while pulling count and inc data for oracle  #
# 4.49          Manick          Oct-2019                Check and write on runnumber file only if is not opened         #
# 4.50          Manick          Oct-2019                Add additonal quotes if split column is varchar and has quote   #
#                                                       Enable parallelism while pulling count if stats are missing     #
#                                                       Force UTF8 while making oracle connections                      #
# 4.51          Manick          Oct-2019                Diabling deactiving master job stream since failing during retry#
#                                                       To handle enhanced tpt header issue check and rename column     #
#                                                       EDWSF_BATCH_ID as applicable                                    #
# 4.52          Manick          Oct-2019                Reuse child streams which are in Inactive status as well        #
# 4.53          Manick          Oct-2019                Use utf-16 encoding to support NLS character instead of utf-8   #
# 4.54          Manick          Oct-2019                Remove TRIM if it is VARCHAR during split to fix the bug        #
# 4.55          Manick          Oct-2019                Pass merge type based on load type to STG2BR                    #
# 4.56          Manick          Oct-2019                If where cluase has quote append another quote                  #
# 4.57          Manick          Oct-2019                Integration of DIY_METADATA     and force metadata collection   #
# 4.58          Manick          Oct-2019                Modify oracle split logic which check non-nulls selectivity     #
# 4.59          Manick          Oct-2019                Enable ingestion from oracle view if it is 1-1                  #
# 4.60          Manick          Oct-2019                Update job description in JCT for handling merge from view      #
# 4.61          Manick          Nov-2019                Collect metadata failing if passing a TD view                   #
# 4.62          Manick          Nov-2019                Merge type validation and change                                #
# 4.63          Manick          Nov-2019                Replace Popen with subprocess while invoking stg2br to fix bug  #
# 4.64          Manick          Nov-2019                Bug fix while checking view for base table and owner            #
# 4.65          Manick          Nov-2019                Validate srcNumrows                                             #
# 4.66          Manick          Nov-2019                Bug fix the logic for checking table split column post view enab#
# 4.67          Manick          Nov-2019                Move view validation outside scope as function                  #
# 4.68          Manick          Nov-2019                Enable oracle view ingestion thorugh exception process          #
# 4.69          Manick          Nov-2019                Force single thread if index cannot found and size < 100 GB     #
# 4.70          Manick          Nov-2019                Ignore if SF has more chunks than needed while merging          #
# 4.71          Manick          Nov-2019                Bug fix with view check for 1-1 with filters                    #
# 4.72          Manick          Nov-2019                Bug fix when synonym found in validate view section             #
# 4.73          Manick          Nov-2019                Mark JCT success incase source has 0 records                    #
# 4.74          Manick          Nov-2019                Switch base schema and tbl to actual for metadata calls to fix  #
#                                                       Synonym issue wehre data is populated as NULL in target         #
# 4.75          Manick          Nov-2019                Bug with validating view source db type case sensitivity        #
# 4.76          Manick          Dec-2019                If no PK found dont switch load and mergetype                   #
#                                                       Switch mergetype only if whereclause is null                    #
# 4.77          Manick          Dec-2019                Integration fo source deletes                                   #
# 4.78          Manick          Dec-2019                Bug fix with ascii value for varchar based split                #
# 4.79          Manick          Dec-2019                SF connection expires before checking source delete valid id    #
#                                                       Create zero record tables if split is inefficient               #
#                                                       Preserve incremental value and pull only first time             #
# 4.80          Manick          Dec-2019                Bug with zero record table creation in SF due to number as var  #
# 4.81          Manick          Dec-2019                Use domain specific role and not edw_datalake_role              #
# 4.82          Manick          Dec-2019                Skip rowcount for view based ingestion                          #
# 4.83          Manick          Dec-2019                Pick different sized domain specific warehouse                  #
# 4.84          Manick          Dec-2019                PreMaValue inside loop for TD ingestion                         #
# 4.85          Manick          Dec-2019                Change to_date to to_timestamp for TD Incremental column        #
# 4.86          Manick          Dec-2019                Integration of DIY soft delete                                  #
# 4.87          Manick          Dec-2019                Pass reqid to soft del and insert with A if inc col present     #
# 4.88          Manick          Dec-2019                Bug with number having same min value which adds 1              #
# 4.89          Manick          Jan-2020                Improvise TD split logic                                        #
# 4.90          Manick          Jan-2020                Pull source count from table for ALL_DATA for audit purpose     #
#                                                       Update unique key not found for inc col with no pk              #
#                                                       use different location for odbc ini file                        #
# 4.91          Manick          Jan-2020                Timestamp column format for TD split incorrect                  #
#                                                       Disable STG2BR as well if merge key notfound                    #
# 4.92          Manick          Jan-2020                Push BR_VIEW recompilation post soft delete                     #
#                                                       Change teradata teradatasql                                     #
# 4.93          Manick          Jan-2020                Call soft delete if it is never ran for a JCT for audit         #
# 4.94          Manick          Jan-2020                Add LOCK ROW FOR ACCESS  for TD for read consistency            #
# 4.95          Manick          Jan-2020                Fix with float type split on minmax array                       #
# 4.96          Manick          Feb-2020                Enable usage of different SF user                               #
# 4.97          Manick          Feb-2020                Disable -d option and auto derive SF schema                     #
# 4.98          Manick          Feb-2020                Bug fix while determining dest schema in TD                     #
#                                                       Dont run soft delete if original ingestion is ALL_DATA          #
# 4.99          Manick          Feb-2020                Change BR_VIEW refresh procedure                                #
# 5.0           Manick          Feb-2020                Bug fix with unique_key_not_found error for filtered_all_data   #
# 5.1           Manick          Feb-2020                Change timestamp to fixed format for TD while splitting         #
# 5.2           Manick          Feb-2020                Restart failed job with JCT verb fails if stg2br not exists     #
# 5.3           Manick          Feb-2020                surcIndColtype in clause causing bug                            #
# 5.4           Manick          Feb-2020                Bug fix with min-max same value for number data type            #
#                                                       Enable NumberToDecimal function to avoid rounding of large dec  #

#                                                       Do not perform  full load of large table if not on excep list   #
#                                                       For large table check index on incremental pull column          #
#                                                       Enable filtered date/id to handle tv table duplicate issue      #
# 5.5           Manick          Feb-2020                Switch conection for oracle to LDAP                             #
# 5.6           Manick          Mar-2020                Outstanding bug with noPieces issue and missing quote on upd    #
# 5.7           Manick          Mar-2020                only one ing at time for a tbl. upd ext type on child threads   #
# 5.8           Manick          Mar-2020                Launch soft delete for all zero rec ingestion to upd audit dash #
#                                                       Enable split and load for view if override column exists        #
# 5.9           Manick          Mar-2020                Enable Append/Upsert type load                                  #
# 5.10          Manick          Mar-2020                Pull source count for audit before launching extraction         #
# 5.11          Manick          Mar-2020                Read from partition if enabled                                  #
# 5.12          Manick          Mar-2020                Disable to_date to use index while querying inc column          #
# 5.13          Manick          Mar-2020                Consider back_time_sec while extracting last extract dtm        #
# 5.14          Manick          Mar-2020                Consider not null per stats when splitting td table             #
# 5.15          Manick          Mar-2020                Disable renaming stage table since it is unnecessary overhead   #
# 5.16          Manick          Apr-2020                Bug fix with while pulling id type inc col for oracle           #
# 5.17          Manick          Apr-2020                Enable and print PID's for background processes                 #
# 6.0           Manick          Apr-2020                Disable Talend                                                  #
# 7.0           Manick          Apr-2020                Enable Hana                                                     #
# 7.1           Manick          Apr-2020                Use View instead of Table for Hana since view has limited cols  #
# 7.2           Manick          May-2020                Use VT for TD Inc pull to reduce CPU usage and use NUSI if avl  #
# 7.3           Manick          May-2020                Include Query banding to segregate load .                       #
#                                                       Change volatile table to in clause based on index presence      #
#                                                       Include SZ (timestamp with timezone as valid inc column for TD) #
# 7.4           Manick          May-2020                Duplicate child JCTS results in submitting both at same time    #
# 7.5           Manick          May-2020                Enable HANA NRT loads                                           #
# 7.6           Manick          May-2020                Dont get srcrows from pending batches. get from DIY_master      #
# 7.7           Manick          May-2020                Use date array if exists for incremental which gets splits      #
# 7.8           Manick          Jun-2020                Validate recompile BR view result                               #
# 8.0           Manick          May-2020                Enable Snowflake                                                #
# 8.1           Manick          May-2020                Get rid of using snowflake.connector                            #
# 8.2           Manick          Jun-2020                Check for thread count along with split count from DIY_Master   #
# 8.3           Manick          Jul-2020                Disable show stream validation since stream will always be on vw#
# 8.4           Manick          Jul-2020                Use WH from JCT table for stream based workflow                 #
# 8.5           Manick          Jul-2020                Handle column name starting with number for HANA                #
# 8.6           Manick          Jul-2020                Convert Split logic to new Python module                        #
# 8.7           Manick          Jul-2020                Handle JCT with ALL_DATA having 0 rows at source                #
# 8.8           Manick          Jul-2020                Issue when splitter could not find not null column for splitting#
# 8.9           Manick          Aug-2020                Fix issue with querying information schema results in 090030    #
# 8.10          Manick          Sep-2020                Bug fix which deletes all records for HANANRT when no btp       #
# 8.11          Manick          Sep-2020                Bug fix with split filter condition for filtered load type jct  #
# 8.12          Manick          Sep-2020                Integration of flat file upload from streams                    #
# 8.13          Manick          Sep-2020                Fix for source ind coltype while merge and restart if merge fail#
#                                                       Handle Information schema query error through auto retry        #
# 8.14          Manick          Sep-2020                Create JCT's and table even if source has 0 records             #
#                                                       Handle if merge fails after succesfull completion               #
# 8.15          Manick          Sep-2020                Enable split and load for Oracle views                          #
# 8.16          Manick          Oct-2020                Adjust bucket ranges and go with max 20 due to parallel load    #
# 8.17          Manick          Oct-2020                Introuduce -m verb to override merge type                       #
# 8.18          Manick          Nov-2020                Fix if single quote comes as part of splitted data escape it    #
# 8.19          Manick          Nov-2020                Fix to truncate milliseconds while updating to_extract_dtm jct  #
#                                                       Fix for filtered load if filter and split has common column     #
# 8.20          Manick          Jan-2021                Explicit close calls for SF connection                          #
# 8.21          Manick          Jan-2021                Introduce back_time_sec logic for ID typed load as well         #
# 8.22          Manick          May-2021                Incorporate inline comments for easy readability
# 8.23          Harkamal        Jun-2021                Enable procedure EDW_RECOMP_BR_VIEW_WITH_MODE for pre and post
#########################################################################################################################
import argparse,pdb
import snowflake.connector
import cx_Oracle
import teradatasql
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
import smtplib,decimal
from metadata_handler import *
from operator import itemgetter, attrgetter
from splitter import diy_split
import DDL_Generator

## At times querying information schema resuts in 90030 error which will eventually gets resolved upon retrying.
## Hence this method keeps retrying to query information schema in loop for 10 times before giving up
## Information schema is queried to get number of split table created as part of ingestion process and to validate if all child threads got completed scuccesfully.

def query_information_schema(cursor3,sql,logFile):
	succ=False
	attempt=0
	while not succ:
		try:
			cursor3.execute(sql)
			results=cursor3.fetchall()
			succ=True
			return(results)
		except Exception as e:
			err_number=e.errno
			sql_state=e.sqlstate
			err_msg=e.msg
			qid=e.sfqid
			if err_number == 90030:
				logdata(logFile,f" Hit error # 90030 - {err_msg} error for query id {qid}.. Hence sleeping 10 seconds before retrying",0)
				time.sleep(10)
				attempt += 1
			else:
				succ=True
				logdata(logFile,f"sql {sql} resulted in {err_number}-{sql_state}-{err_msg}-{qid}",0)
				return(f"{err_number}-{sql_state}-{err_msg}-{qid}")
			if attempt >= 10:
				succ=True
				logdata(logFile,f"sql {sql} failed to succeed even after 10 attempts.. hence giving up",0)
				return(f"sql {sql} failed to succeed even after 10 attempts.. hence giving up")

## Incase if table is very large and table is partitioned by date then framework will rely on querying latest partition to query the incremental column value. #
## This is applicable only for tables ingested from Oracle database. For those jobs for which Parition needs to be used for querying incremental value identifying #
## partition column is a manual process. Once identified same has to be entered into DIY_TAB_PARTITIONS table. Framework will use that table to query current #
## partition which will be uesd to pull the incremental column value #

def query_partition(cursor1,cursor2,sourceDb,sourceSchema,sourceTable,logFile,errFile):
	try:
		part_col_found=False
		sql=f"select column_name from diy_tab_partitions where db_name='{sourceDb}' and schema_name='{sourceSchema}' and table_name='{sourceTable}' and enabled_flag='Y'"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		if len(results) == 1:
			for results_obj in results:
				part_col_name=results_obj[0]
			sql=f"select column_name from ALL_PART_KEY_COLUMNS where name='{sourceTable}' and owner='{sourceSchema}' and object_type='TABLE'"
			results=cursor2.execute(sql).fetchall()
			if len(results) == 1:
				for results_obj in results:
					part_column=results_obj[0]
				if part_column == part_col_name:
					sql=f"select DATA_TYPE from all_tab_columns where column_name='{part_column}' and table_name='{sourceTable}' and owner='{sourceSchema}'"
					results=cursor2.execute(sql)
					for results_obj in results:
						part_column_data_type=results_obj[0]
					sql=f"select partition_name,high_value from all_tab_partitions where table_name='{sourceTable}' and table_owner='{sourceSchema}'"
					results=cursor2.execute(sql)
					res_lis=[dict((cursor2.description[i][0], value) for i, value in enumerate(row)) for row in cursor2.fetchall()] ## fetch entire output in a list
					for obj in res_lis:
						part_name=obj['PARTITION_NAME']
						high_value=obj['HIGH_VALUE']
						if part_column_data_type == "DATE":
							cursor2.execute(f"select sysdate-{high_value} from dual")
							results=cursor2.fetchall()
							for results_obj in results:
								delta=results_obj[0]
							if delta < 0:
								part_col_found=True
								break
					if part_col_found:
						logdata(logFile,f"Determined current partition for {sourceDb}.{sourceSchema}.{sourceTable} as {part_name}",0)
						return part_name
					else:
						logdata(logFile,f"Could not determine current partition for {sourceDb}.{sourceSchema}.{sourceTable}",0)
						return -1
				else:
					logdata(logFile,f"Data dictionary returned partition column as {part_column} where as DIY has it as {part_col_name}",0)
					return -1
			else:
				logdata(logFile,f"Could not query soure db data dictionary for {sourceDb}.{sourceSchema}.{sourceTable}",0)
				return -1
		else:
			logdata(logFile,f"No entry found in diy_tab_partitions for {sourceDb}.{sourceSchema}.{sourceTable}",0)
			return -1
	except Exception as e:
		logdata(logFile,f"Below exception encountered while determining partition for {sourceDb}.{sourceSchema}.{sourceTable}",0)
		logdata(logFile,str(e),1)
		return -1

## This is used by cx_Oracle Python module to return numeric data type as Python's decimal data type #

def NumberToDecimal(cursor, name, defaultType, size, precision, scale):
        if defaultType == cx_Oracle.NUMBER:
                return cursor.var(decimal.Decimal, arraysize=cursor.arraysize)

## This method just logs the data into log file along with timestamp and flush the memory so that content can be visible before closing file handle

def logdata(logf,msg,printflag):
	currTime=datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
	if printflag == 1:
		print(currTime + " - " + msg)
	logf.write(currTime + " - " + msg + "\n")
	logf.flush()
	os.fsync(logf.fileno())

## This method validates the input parameters that are passed to the program

def validate_params(itype,pfile,tableName,envType,tCnt,jobStreamid,emailAdd,mailonSucc):
	if (itype.lower() != 'db' and itype.lower() != 'file' and itype.lower() != 'table' and itype.lower() != 'jct'):
		print ("Invalid value for argument -t. It has to be either db (if source is another database) or file (if source is flat file) or table (previously ingested table) or jct")
		sys.exit(1)
	if (itype.lower() == 'db'):
		exists=os.path.isfile(pfile)
		if not exists:
			print ("Invalid parameter file - " + pfile + ". Please check for the existence of parameter file and reinvoke the program")
			sys.exit(2)
	if(itype.lower() == 'table'):
		if tableName is None:
			print ("tableName is mandtory. Please provide tableName for which data was previously ingested")
			sys.exit(4)
		if envType is None:
			print ("Environment type (TS*/DV*/PRD) is mandatory. Please provide the environment type for table - " + tableName + " for which data was previously ingested")
			sys.exit(5)
		if tableName.count('.') <2:
			print ("Table name - " + tableName + " needs to be given in db.schema.tablename format. Please check and retry")
			sys.exit(6)
	if(itype.lower() == 'jct'):
		if jobStreamid is None:
			print("Job stream id is mandtory for type jct. Please provide JCT id which is present in repository")
			sys.exit(8)
		if envType is None:
			print ("Environment type (TS*/DV*/PRD) is mandatory. Please provide the environment type")
			sys.exit(9)
	if tCnt is not None:
		try:
			tCnt=int(tCnt)
		except:
			print("Invalid value for threadcount - " + tCnt + ". Thread count needs to be a integer if you want to override. Please check and retry")
			sys.exit(7)
	if emailAdd is None:
		print ("Email address is mandatory. Please provide the same using -a option (example -a mkumarap@cisco.com)")
		sys.exit(10)
	if mailonSucc is not None:
		if mailonSucc.upper() != 'YES' and mailonSucc.upper() != 'NO':
			print("Invalid value for -o mail on Success. Valid values are only Yes and No. Please check and retry")
			sys.exit(12)

## This method sets environment variables to make connection to Oracle database.

def set_env():
	os.environ['LD_LIBRARY_PATH']="/usr/cisco/packages/oracle/oracle-12.1.0.2/lib"
	os.environ['ORACLE_HOME']="/usr/cisco/packages/oracle/oracle-12.1.0.2"
	os.environ['TNS_ADMIN']="/apps/edwsfdata/python/network/admin"
	#os.environ['ODBCINI']='/apps/edwsfdata/python/scripts/odbc/.odbc.ini'

## This method calls DIY_load_stg.py which is primarily responsible for pulling data from source db and writing into CSV file.
## Incase if job involves multiple child threads (due to split and load) then launch them in parallel until max thread count specified while invoking program.

def invokeExtract(envType,Data_Ext_Scr,reqid,jobGroup,jobStreamarr,jobStreammstr,logDir,mstrlogFile,Thread_count,tblSize,cursor1,sourceSchema,sourceTable,baseTable,baseSchema,sourcedbType):
	os.environ["RUN_ENVIRONMENT"]=envType
	#logFile=logDir + jobStream + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".log"
	#oscmd=Talend_Scr_Path + Talend_Scr + " " + jobGroup + " " + jobStream + " >> " + logFile + " 2>&1 &"
	oscmd=Data_Ext_Scr
	monJobstream=jobStreamarr[0][:-2]
	osPrcmd="ps -ef | grep " + Data_Ext_Scr + " | grep " +  monJobstream + " | grep -v grep | wc -l"
	processes = set()
	pids=set()
	prcnt=0
	##jobStreamarr holds list of all child jobs (parent job only if no split) that needs to be triggered in parallel to download data.
	for loopcnt in range(len(jobStreamarr)):
		prcnt += 1
		currJobstr=jobStreamarr[loopcnt]
		logdata(mstrlogFile,"Now executing below OS Command",0)
		if sourcedbType.upper() == 'HANA':
			logdata(mstrlogFile,oscmd + " -e " + envType + " -r " + str(reqid) + " -j " + currJobstr + " -s " + str(tblSize) + " -t " + sourceTable + " -b " + sourceSchema,0)
		else:
			logdata(mstrlogFile,oscmd + " -e " + envType + " -r " + str(reqid) + " -j " + currJobstr + " -s " + str(tblSize) + " -t " + baseTable + " -b " + baseSchema,0)
		outFile=logDir + currJobstr + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".log"
		errFile=logDir + currJobstr + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".err"
		outf=open(outFile,'w')
		errf=open(errFile,'w')
		## For Hana pass the view new to extract from view and for all other DB types pass correspond base table.
		if sourcedbType.upper() == 'HANA':
			proc=subprocess.Popen([oscmd,'-e',envType,'-r',str(reqid),'-j',currJobstr,'-s',str(tblSize),'-t',sourceTable,'-b',sourceSchema],stdout=outf,stderr=errf)
		else:
			proc=subprocess.Popen([oscmd,'-e',envType,'-r',str(reqid),'-j',currJobstr,'-s',str(tblSize),'-t',baseTable,'-b',baseSchema],stdout=outf,stderr=errf)
		pid=proc.pid
		pids.add(pid)
		pid_list=""
		processes.add(proc)
		#processes.add(subprocess.Popen([oscmd,jobGroup,currJobstr,str(tblSize),baseTable,baseSchema],stdout=outf,stderr=errf))
		if prcnt == 1:
			prst='1st'
		elif prcnt == 2:
			prst='2nd'
		elif prcnt == 3:
			prst='3rd'
		else:
			prst=f"{prcnt}th"
		print ("Launched " + prst + " process with pid " + str(pid) + " out of " + str(len(jobStreamarr)) + " total process to submit")
		curr_pid=os.getpid()
		logFlag=True
		## Check how many child process has been submitted vs thread count specified while invoking the program.
		while len(processes) >= Thread_count:
			for pid_obj in pids:
				pid_list+=f"{pid_obj}|"
			pid_list=pid_list[:-1]
			#osPrcmd=f"ps -ef | egrep '{pid_list}' | grep {Data_Ext_Scr} | grep -v grep | wc -l"
			#osPrcmd=f"ps -ef | awk '{print $2}' | grep {curr_pid}  | grep -v defunct | grep -v grep | wc -l"
			osPrcmd="ps -ef | grep -v defunct | grep -v grep | awk '{print $3}' | grep " + str(curr_pid) + " | wc -l"
			cnt=int(os.popen(osPrcmd).read())
			#cnt=cnt-1 ## ignore parent pid
			if logFlag:
				logdata(mstrlogFile,str(cnt) + " ingestion threads are active for " + monJobstream + " ingestion stream..",1)
				logFlag=False
			os.wait()
			time.sleep(10) ## 10 seconds sleep before checking for process status
			cursor1.execute("select sysdate from dual"); ## Just to keep the oracle connective live for very long running ingestions
			processes.difference_update(
             [p for p in processes if p.poll() is not None])
		time.sleep(5) ## Sleeping 5 seconds between each parallel submission
	for p in processes:
		btchsleep=0
		totsleeptime=0
		#osPrcmd=f"ps -ef | egrep '{pid_list}' | grep {Data_Ext_Scr} | grep -v grep | wc -l"
		#osPrcmd=f"ps -ef | awk '{print $2}' | grep {curr_pid}  | grep -v defunct | grep -v grep | wc -l"
		osPrcmd="ps -ef | grep -v defunct | grep -v grep | awk '{print $3}' | grep " + str(curr_pid) + " | wc -l"
		while p.poll() is None:
			cnt=int(os.popen(osPrcmd).read())
			#cnt=cnt-1 ## ignore parent pid
			if btchsleep ==0 :
				logdata(mstrlogFile,str(cnt) + " ingestion threads are active for " + monJobstream + " ingestion stream..",1)
				btchsleep=1
			time.sleep(10)
			cursor1.execute("select sysdate from dual"); ## Just to keep the oracle connective live for very long running ingestions
			totsleeptime+=10
			if totsleeptime >= cnt*60:
				totsleeptime=0
				btchsleep=0
			#sleeptime=cnt*60
			#time.sleep(sleeptime)
			#p.wait()
	outf.close()
	errf.close()

## This method used to establish connection to Snowflake. This is just wrapper to invoke open_sf_connection from metadata module

def connect_snowflake(dbName,schemaName,logFile,errFile,envType,cursor1,emailAdd,sfRole,sfWh,sf_user):
	global sf_query_tag,sfEnv
	if configFile.Config.PRDSUFF in envType:
		sfEnv = 'cisco.us-east-1'
	elif configFile.Config.STGSUFF in envType:
		sfEnv = 'ciscostage.us-east-1'
	elif configFile.Config.DEVSUFF in envType:
		sfEnv = 'ciscodev.us-east-1'
	else:
		logdata(errFile,"Invalid envtype.. May be change in config.. Check and fix",1)
		errMsg="Invalid envtype.. May be change in config.. Check and fix"
		Subject="Error:" + errMsg
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		return -1
	try:
		conn3 = open_sf_connection(sfEnv,sf_user,sfWh,sfRole,dbName,schemaName,query_tag=sf_query_tag)
		return conn3
	except  Exception as e:
		colval="Exception occurred while establing SF connection to " + sfEnv + " account as " + sf_user + ". Please check."
		updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
		updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
		logdata(errFile,colval,1)
		logdata(errFile,str(e),1)
		errMsg=colval
		Subject="Error:" + colval
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		return -1

## This method is used to merge the smaller chunks to single stage table if multiple splits were used for data load.

def mergeSFstgtable(dbName,schemaName,tblName,noPieces,logFile,errFile,envType,srcNumrows,cursor1,emailAdd,sfRole,sfWh,reqid,surcIndColtype,surcIndColFormat,sf_user,jobStream):
	logdata(logFile,"Now starting the SF merge process",0)
	conn3=connect_snowflake(dbName,schemaName,logFile,errFile,envType,cursor1,emailAdd,sfRole,sfWh,sf_user)
	if type(conn3) is int:
		logdata(logFile,"Exception encoutnered while connection to snowflake",1)
		return -1
	else:
		cursor3=conn3.cursor()
	if noPieces == 1:
		try:
			logdata(logFile,"Stage table is single table.. Nothing to merge.. Hence Proceeding with count check",0)
			cursor3.execute("select count(*) from " + dbName + "." + schemaName + "." + tblName)
			results=cursor3.fetchall()
			for resultsObj in results:
				sfCnt = resultsObj[0]
			logdata(logFile,"Succesfully ingested " + str(sfCnt) + " rows into " + dbName + "." + schemaName + "." + tblName + " table.",1)
			#return(sfCnt)
		except Exception as e:
			colval="Exception occurred while checking target table rowcount."
			updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
			updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
			logdata(errFile,colval,1)
			logdata(errFile,str(e),1)
			errMsg=colval
			Subject="Error:" + colval
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			conn3.close()
			return -1
	else:
		try:
			sql=f"select substr(split_column,instr(split_column,'~')+1) split_col_data_type from diy_master where reqid={reqid}"
			cursor1.execute(sql)
			results=cursor1.fetchall()
			for coltype_obj in results:
				surcIndColtype=coltype_obj[0]
			logdata(logFile,"Checking for any zero record chunks",0)
			## At times chunks became zero record due to change in source since it got split. In such cases check and create empty table for corresponding chunk
			## This is done as framework validates total number of splits vs total number of chunk tables.
			if surcIndColtype in ['NUMBER','I','I8','I1','I2','D']:
				minval_col="to_number(min_value)"
				maxval_col="to_number(max_value)"
			elif surcIndColtype in ['DATE','TS','TZ','SZ']:
				minval_col="to_date(nullif(substr(min_value,1,19),' IS NULL '),'MM/DD/YYYY HH24:MI:SS')"
				maxval_col="to_date(substr(max_value,1,19),'MM/DD/YYYY HH24:MI:SS')"
			elif surcIndColtype =='DA':
				minval_col=f"to_date(min_value,'MM/DD/YYYY')"
				maxval_col=f"to_date(max_value,'MM/DD/YYYY')"
			else:
				minval_col="min_value"
				maxval_col="max_value"
			sql=f"select min(split_id) from diy_splits where reqid={reqid} and {minval_col}<={maxval_col}"
			cursor1.execute(sql)
			results=cursor1.fetchall()
			for non_zero_tab_obj in results:
				non_zero_split_id=non_zero_tab_obj[0]
			sql=f"select split_id from diy_splits where reqid={reqid} and {minval_col}>={maxval_col}"
			cursor1.execute(sql)
			results=cursor1.fetchall()
			for zero_tab_obj in results:
				split_id=zero_tab_obj[0]
				logdata(logFile,f"Found split id {split_id} as 0 record table.. Hence creating the same",0)
				cursor3.execute(f"create  table if not exists {tblName}_{split_id} as  select * from {tblName}_{non_zero_split_id} where 1=2 ;")
			sql="select count(*) from " + dbName + ".information_schema.tables where regexp_like(table_name,'" + tblName + "_[0-9]{1,}')  and table_schema='STG'  and table_type='BASE TABLE' and created>current_date-7";
			results=query_information_schema(cursor3,sql,logFile)
			if type(results) is str:
				raise Exception(results)
			for resultsObj in results:
				sfChunkCnt = resultsObj[0]
			## Check number of splits vs number of chunk tables in SF.
			if sfChunkCnt < noPieces :
				raise Exception ("Snowflake stage has only " + str(sfChunkCnt) + " chunks while it is suppose to have " + str(noPieces))
			logdata(logFile,"Stage table has " + str(noPieces) + " chunks.. Hence proceeding towards merging the same..",0)
			## Below piece of code is to check available warehouse sizes and pick appropriate sized warehouse for merge to optimize usage cost
			cursor3.execute("show warehouses like '" + sfWh + "'")
			cursor3.execute("select $4 whsize from table(result_scan(last_query_id()))")
			results=cursor3.fetchall()
			for resultsObj in results:
				sfwhSize = resultsObj[0]
			logdata(logFile,"Warehouse " + sfWh + " is originally of size " + sfwhSize,0)
			sql="select bytes/(1024*1024*1024) from " + dbName + ".information_schema.tables  where table_name like '" + tblName + "_1' and table_schema='STG' and table_type='BASE TABLE'"
			results=query_information_schema(cursor3,sql,logFile)
			if type(results) is str:
				raise Exception(results)
			for resultsObj in results:
				sftblChunkSize = resultsObj[0]
			sftblSize=sftblChunkSize*noPieces*10 ## add 10x for compression
			if sftblChunkSize >= 50:
				wh_2_use=sfWh.replace('COMMON_WH','COMMON_EXT3_WH')
			elif sftblChunkSize >= 25:
				wh_2_use=sfWh.replace('COMMON_WH','COMMON_EXT2_WH')
			else:
				wh_2_use=sfWh.replace('COMMON_WH','COMMON_EXT1_WH')
			cursor3.execute(f"use warehouse {wh_2_use}")
			logdata(logFile,"Now proceeding with target table merge",1)
			## Stage table will be everytime drop and create
			cursor3.execute("Create or replace table " + tblName + " as select * from " + tblName + "_1 where 1=2")
			cursor3.execute("alter session set autocommit=false")
			for i in range(noPieces):
				cursor3.execute("select count(*) from " + tblName + "_" + str(i))
				results=cursor3.fetchall()
				for cntObj in results:
					rowCnt=cntObj[0]
				cursor1.execute("update DIY_splits set row_count=" + str(rowCnt) + " where reqid=" + str(reqid) + " and split_id=" + str(i))
				cursor1.execute("commit")
				cursor3.execute("insert into " + tblName + " (select * from " + tblName + "_" + str(i) + ")")
			cursor3.execute("commit")
			logdata(logFile,"Succesfully merged all pieces into target table " + tblName,1)
			cursor3.execute("select count(*) from " + dbName + "." + schemaName + "." + tblName)
			results=cursor3.fetchall()
			for resultsObj in results:
				sfCnt = resultsObj[0]
			## If snowflake count exceeds or equals to source count, then drop the chunks table.. if not keep it for further RCA
			logdata(logFile,"Succesfully ingested " + str(sfCnt) + " rows into " + dbName + "." + schemaName + "." + tblName + " table.",1)
			if sfCnt >= srcNumrows:
				logdata(logFile,"Since snowflake rowcount matches or exceeds source count proceeding with dropping chunk tables",0)
				for i in range(noPieces):
					cursor3.execute("drop table " + tblName + "_" + str(i))
			#if conn3:
			#	conn3.close()
			#return(sfCnt)
		except Exception as e:
			cursor3.execute("rollback")
			colval="Exception occurred while merging target table." 
			logdata(logFile,"Reverting back status of master JCT",0)
			sql=f"update edw_job_streams set run_status='A' where job_stream_id='{jobStream}'"
			execute_qry(conn, sql, reqid, conn)
			sql=f"select job_stream_id jsi from diy_src2stg_checkpoint where reqid={reqid}"
			jct_obj=execute_df_qry(conn, sql, reqid, conn)
			child_jct=jct_obj["jsi"][0]
			sql=f"update edw_job_streams set run_status='R' where job_stream_id='{child_jct}'"
			execute_qry(conn, sql, reqid, conn)
			sql=f"update diy_src2stg_checkpoint set current_Phase='Archive_Files' where job_stream_id='{child_jct}' and reqid={reqid}"
			execute_qry(conn, sql, reqid, conn)
			execute_qry(conn, "commit", reqid, conn)
			updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
			updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
			logdata(errFile,colval,1)
			logdata(errFile,str(e),1)
			errMsg=colval
			Subject="Error:" + colval
			sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			conn3.close()
			return -1
	if conn3:
		conn3.close()
	return(sfCnt)

## Below method is used to pull latest value for incremental column specifically for TD tables.
	
def updTdInccol(sourceSchema,sourceTable,cursor2,jobStream,incColname,cursor1,logFile,errFile):
	logdata(logFile,"Now checking the data type for incremental column in source db",0)
	try:
		sql19="LOCK ROW FOR ACCESS select decode(trim(columntype),'DA','DATE','I','ID','I8','ID','I1','ID','I2','ID','TS','DATE','TZ','DATEZ','SZ','DATEZ','INVALID') from "
		sql19=sql19+"dbc.columnsv where databasename='" + sourceSchema + "' and tablename='" + sourceTable + "' and columnname='" + incColname + "'"
		cursor2.execute(sql19)
		results=cursor2.fetchall()
		for incdTypeobj in results:
			incdType=incdTypeobj[0]
		if incdType == 'INVALID':
			logdata(errFile,"Incremental column specified is neither a date data type nor a ID data type. Hence cannot update incremental column details",1)
			return -1
		try:
			logdata(logFile,"Incremental column identified is of data type - " + incdType + ".Now extracting latest value from source",0)
			if incdType == 'DATEZ':
				sql20="LOCK ROW FOR ACCESS  select cast(max(" + incColname + ") as timestamp(0)) from " + sourceSchema + "." + sourceTable
				incdType = 'DATE'
			else:
				sql20="LOCK ROW FOR ACCESS  select max(" + incColname + ") from " + sourceSchema + "." + sourceTable
			cursor2.execute(sql20)
			results=cursor2.fetchall()
			for resIncvalueobj in results:
				resIncvalue=resIncvalueobj[0]
			logdata(logFile,"Max value of " + incColname + " in table " + sourceSchema + "." + sourceTable + " is " + str(resIncvalue),0)
			return(incdType + "~" + str(resIncvalue))
		except Exception as e:
			logdata(errFile,"Exception occurred while pull incremental column value.",1)
			logdata(errFile,str(e),1)
			return -1
	except Exception as e:
		logdata(errFile,"Exception occurred while checking incremental column data type.",1)
		logdata(errFile,str(e),1)
		return -1

## Below method is used to pull latest value for incremental column specifically for Hana tables.

def updHanaInccol(sourceSchema,sourceTable,cursor2,jobStream,incColname,cursor1,logFile,errFile,obj_type):
	logdata(logFile,"Now checking the data type for incremental column in source db",0)
	try:
		sql19="select map(trim(data_type_name),'DATE','DATE','TIMESTAMP','DATE','SECONDDATE','DATE','INTEGER','ID','BIGINT','ID','SMALLINT','ID','TINYINT','ID','INVALID') from "
		if obj_type == 'TABLE':
			sql19=sql19+"SYS.TABLE_COLUMNS where schema_name='" + sourceSchema + "' and table_name='" + sourceTable + "' and column_name='" + incColname + "'"
		else:
			sql19=sql19+"SYS.VIEW_COLUMNS where schema_name='" + sourceSchema + "' and view_name='" + sourceTable + "' and column_name='" + incColname + "'"
		cursor2.execute(sql19)
		results=cursor2.fetchall()
		for incdTypeobj in results:
			incdType=incdTypeobj[0]
		if incdType == 'INVALID':
			logdata(errFile,"Incremental column specified is neither a date data type nor a ID data type. Hence cannot update incremental column details",1)
			return -1
		try:
			logdata(logFile,"Incremental column identified is of data type - " + incdType + ".Now extracting latest value from source",0)
			#sql20=f'select max({incColname}) from {sourceSchema}."{sourceTable}"'
			if incdType == 'DATE':
				sql20=f'select to_seconddate(to_varchar(max({incColname}), \'MM/DD/YYYY HH24:MI:SS\'),\'MM/DD/YYYY HH24:MI:SS\') from {sourceSchema}."{sourceTable}"'
			elif incdType == 'ID':
				sql20=f'select max({incColname}) from {sourceSchema}."{sourceTable}"'
			else:
				logdata(errFile,"Incremental column specified is neither a date data type nor a ID data type. Hence cannot update incremental column details",1)
				return -1
			cursor2.execute(sql20)
			results=cursor2.fetchall()
			for resIncvalueobj in results:
				resIncvalue=resIncvalueobj[0]
			logdata(logFile,"Max value of " + incColname + " in table " + sourceSchema + "." + sourceTable + " is " + str(resIncvalue),0)
			return(incdType + "~" + str(resIncvalue))
		except Exception as e:
			logdata(errFile,"Exception occurred while pull incremental column value.",1)
			logdata(errFile,str(e),1)
			return -1
	except Exception as e:
		logdata(errFile,"Exception occurred while checking incremental column data type.",1)
		logdata(errFile,str(e),1)
		return -1

## Below method is used to pull latest value for incremental column specifically for Oracle tables.

def updOracleInccol(sourceSchema,sourceTable,cursor2,jobStream,incColname,cursor1,logFile,errFile,src_tbl_2_query):
	logdata(logFile,"Now checking the data type for incremental column in source db",0)
	try:
		sql19="select decode(data_type,'NUMBER','ID','DATE','DATE','TIMESTAMP(3)','DATE','TIMESTAMP(6)','DATE','INVALID') from all_tab_columns where owner='" + sourceSchema + "' and table_name='" + sourceTable + "' and column_name='" + incColname + "'"
		cursor2.execute(sql19)
		results=cursor2.fetchall()
		if len(results) == 0:
			logdata(errFile,"Incremental column " + incColname + " specified is invalid. Please check and fix",1)
			return -1
		for incdTypeobj in results:
			incdType=incdTypeobj[0]
		if incdType != 'DATE' and incdType !='ID':
			logdata(errFile,"Incremental column specified is neither a date data type nor a ID data type. Hence cannot update incremental column details",1)
			return -1
		try:
			logdata(logFile,"Incremental column identified is of data type - " + incdType + ".Now extracting latest value from source",0)
			if incdType == 'DATE':
				sql20="select to_date(to_char(aa.tdate,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') from "
				sql20+= " ( select /*+ parallel (16) */ max(" + incColname + ") as tdate from " + src_tbl_2_query  + " a ) aa"
			else:
				sql20= "select /*+ parallel (16) */ max(" + incColname + ") as tdate from " + src_tbl_2_query  + " a "
			cursor2.execute(sql20)
			results=cursor2.fetchall()
			for resIncvalueobj in results:
				resIncvalue=resIncvalueobj[0]
			logdata(logFile,"Max value of " + incColname + " in table " + sourceSchema + "." + sourceTable + " is " + str(resIncvalue),0)
			return(incdType + "~" + str(resIncvalue))
		except Exception as e:
			logdata(errFile,"Exception occurred while pull incremental column value.",1)
			logdata(errFile,str(e),1)
			return -1
	except Exception as e:
		logdata(errFile,"Exception occurred while checking incremental column data type.",1)
		logdata(errFile,str(e),1)
		return -1

## Whenever a new table is getting ingested for very first time associated meta data needs to be collected and stored in catalog.
## DIY_metadata is a seperate stand alone script used for collecting metadata. This method is just wrapper to invoke the same.

def collectMetadata(envType,sourceDb,sourceSchema,sourceTable,logFile,errFile,sourcedbType,connectionName,logDir):
	logdata(logFile,"Now launching metadata collection module",0)
	try:
		Meta_Scr_2_Run="/apps/edwsfdata/python/scripts/DIY_metadata.py"
		if '/' in sourceTable:
			table_2_use=sourceTable.split('/')[1]
		else:
			table_2_use=sourceTable
		outMetaFile=logDir + "metadata_" + table_2_use + datetime.now().strftime('%Y%m%d%H%M%S') + ".log"
		errMetaFile=logDir + "metadata_" + table_2_use + datetime.now().strftime('%Y%m%d%H%M%S') + ".err"
		outf=open(outMetaFile,'w')
		errf=open(errMetaFile,'w')
		res=subprocess.Popen([Meta_Scr_2_Run,"-e",envType,"-d",sourceDb,"-s",sourceSchema,"-t",sourceTable,"-c",connectionName],stdout=outf,stderr=errf)
		res.wait()
		rc=res.returncode
		outf.close()
		errf.close()
		return rc
	except Exception as e:
		logdata(errFile,"Exception encountered while launching process for metadata collection",1)
		logdata(errFile,str(e),1)
		errf.write("Exception encountered while launching process for metadata collection - " + str(e))
		outf.close()
		errf.close()
		return -1

## Throughout the process mutiple attempts are required to update DIY_Master table. This method is invoked to perform such updates.

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

## Throughout the process mutiple lines are inserted into DIY_LINES table. This method is invoked to perform such inserts.

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

## This is obsolete procedure. This needs to be switched with metadata_handler.send_email

def sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1):
	logdata(logFile,"Sending email to " + emailAdd + " with status - " + errMsg,0)
	msg=MIMEMultipart()
	message=errMsg
	body = MIMEText(message)
	msgfrom='sf-smart-ingestiony@cisco.com'
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
		inssql="insert into DIY_email_logs (reqid,email_address,subject,errmsg,logfile,errfile,sent_time) values "
		errMsg=errMsg.replace("'","''")
		Subject=Subject.replace("'","''")
		inssql=inssql + "(" + str(reqid) + ",'" + emailAdd + "','" + Subject + "','" + errMsg + "','" + logFile.name + "','" + errFile.name + "',sysdate)"
		cursor1.execute(inssql)
		cursor1.execute('commit')
	except Exception as e:
		logdata(errFile,"Exception encountered while inserting/updating Email Log",1)
		logdata(errFile,str(e),1)
		Subject ="Error: Exception encountered while updating DIY lines"
		errMsg=Subject + "\n\n" + str(e)
		sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)

## This is obsolete procedure. This needs to be switched with metadata_handler.send_email

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

## Below method is used to validate the existence of metadata. If catalog doesnt have associated metadata collectmetadata method is invoked to collect the same.

def validateMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,logDir):
	logdata(logFile,"Now validating metadata to make sure all source table columns exists in repository",0)
	if sourcedbType.upper() == "TERADATA":
		## Incase of TD, if 1-1 view is passed as source object collect metadata for corresponding base table.
		try:
			if 'VWDB' in sourceSchema.upper():
				logdata(logFile,"Pulling 3NF for vw schema",0)
				sql13 = " select distinct db_schema_name from EDS_DATA_CATALOG.EDW_TD_VWDB_MAPPING where view_schema_name='" + sourceSchema + "'"
				cursor1.execute(sql13)
				results=cursor1.fetchall()
				for vwcheckobj in results:
					sourceSchema=vwcheckobj[0]
				logdata(logFile,"Found base schema as " + sourceSchema,0)
		except Exception as e:
			#print ("Error occurred while checking base table from 1-1 view")
			logdata(errFile,"Error occurred while checking table schema from view schema",1)
			#print(str(e))
			logdata(errFile,str(e),1)
			return -1
	try:
		## Check for metadata existence.
		sql1="select count(*) from EDS_DATA_CATALOG.EDW_TABLE_COLUMN where db_instance_name='" + sourceDb + "' and db_schema_name='" +  sourceSchema
		sql1=sql1+ "' and table_name='" + sourceTable + "' and environment_name='" + envType + "' and data_type is not null"
		cursor1.execute(sql1)
		results=cursor1.fetchall()
		for resMetobj in results:
			resMetcnt=resMetobj[0]
		## Collect metadata if it doesnt exist.
		if resMetcnt == 0:
			#return -1
			logdata(logFile,"Now attempting to collect metadata for table - " + sourceTable,0)
			rc=collectMetadata(envType,sourceDb,sourceSchema,sourceTable,logFile,errFile,sourcedbType,connectionName,logDir)
			if rc != 0:
				raise Exception("Issue encountered while collecting metadata. Please check logfile")
			logdata(logFile,"Post collecting metadata checking for metadata in repos",0)
			try:
				cursor1.execute(sql1)
				results=cursor1.fetchall()
				for resMetobj in results:
					resMetcnt=resMetobj[0]
				if resMetcnt == 0:
					logdata(errFile,"Even though attempted to collect metadata, collect metadata failed.. Please check and take action",1)
					return -1
				else:
					return resMetcnt
			except Exception as e:
				logdata(errFile,"Exception encountered while verifying metadata existence for table post collection- " + sourceTable,1)
				logdata(errFile,"SQL query ran was " + sql1,1)
				logdata(errFile,str(e),1)
				return -1
		else:
			return resMetcnt
	except Exception as e:
		logdata(errFile,"Exception encountered while verifying metadata existence for table pre collection - " + sourceTable,1)
		logdata(errFile,"SQL query ran was " + sql1,1)
		logdata(errFile,str(e),1)
		return -1

## Below method is used to validate if requested ingestion is from view.. Incase of TD, only 1-1 view is supported. Complex views aren't supported.
## Incase of Oracle views are not supported out of box. But if we receive request for Oracle views, then check if it is 1-1 based on definition
## If not check if view is approved exceptionally (such exceptions are recorded in  edw_view_ingestion table
## For Hana all views are supported. For HANA NRT only views are supported for ingestion
## For Snowflake check if streams are enabled as that is only supported method for incremental loads.

def validate_view(envType,sourceDb,dbType,cursor2,sourceSchema,sourceTable,emailAdd,logFile,errFile,cursor1):
	try:
		global excepView,obj_type
		global sourceType
		if dbType.upper() == 'TERADATA':
			sql13 = " exec PERFMETRICSDB.BASEVIEWCHECK ('" + sourceSchema + "','" + sourceTable + "')"
			cursor2.execute(sql13)
			results=cursor2.fetchall()
			for vwcheckobj in results:
				tblName=vwcheckobj[0]
			if tblName == '-2':
				logdata(errFile,"Source Teradata object - " + sourceSchema + "." + sourceTable + " doesnt exist.. Hence cannot be ingested",1)
				Subject ="Error: Source Teradata object - " + sourceSchema + "." + sourceTable + " doesnt exist.. Hence cannot be ingested"
				errMsg=Subject
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				return -1
			elif tblName == '-1':
				#print ("Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested")
				logdata(errFile,"Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested",1)
				Subject ="Error: Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested"
				errMsg=Subject
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				return -1
			else:
				objArray = tblName.split(".")
				srcBaseschema = objArray[0]
				srcBasetable = objArray[1]
				logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
				return f'{srcBaseschema}~{srcBasetable}'
		elif dbType.upper() == 'ORACLE':
			logdata(logFile,"Checking if source object exists in database",0)
			sql13=f"select count(1) from all_objects where object_name='{sourceTable}' and owner='{sourceSchema}'"
			cursor2.execute(sql13)
			results=cursor2.fetchall()
			for objCntobj in results:
				objCnt=objCntobj[0]
			if objCnt == 0:
				sql13="select user from dual"
				cursor2.execute(sql13)
				results=cursor2.fetchall()
				for usrObj in results:
					currentUser=usrObj[0]
				Subject=f"Either source object {sourceSchema}.{sourceTable} doesn't exist or user {currentUser} dont have access. Hence cannot ingest. Please check and retry"
				errMsg=Subject
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				logdata(logFile,Subject,1)
				return -1
			logdata(logFile,"Checking if passed object is view",0)
			sql13 = "select count(1) from all_tables where table_name='" + sourceTable + "' and owner='" + sourceSchema + "'"
			cursor2.execute(sql13)
			results=cursor2.fetchall()
			for vwcheckobj in results:
				tblCnt=vwcheckobj[0]
			if tblCnt == 0:
				#print ("Source Teradata object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested")
				## 4.59 change starts here
				TblFound=False
				logdata(logFile,"Checking if passed object is Synonym",0)
				sql13 = " select b.owner,b.object_name,b.object_type from all_synonyms a,all_objects b where "
				sql13 += " a.synonym_name='" + sourceTable + "' and a.owner='" + sourceSchema + "' and a.table_name=b.object_name and a.table_owner=b.owner and b.object_type in ('TABLE','VIEW')"
				cursor2.execute(sql13)
				sourceType=''
				results=cursor2.fetchall()
				for synObj in results:
					sourceOwner=synObj[0]
					sourceName=synObj[1]
					sourceType=synObj[2]
				if sourceType == 'TABLE':
					srcBaseschema=sourceOwner
					srcBasetable=sourceName
					TblFound=True
				elif  sourceType =='VIEW':
					viewOwner=sourceOwner
					viewName=sourceName
				if sourceType == '':
					logdata(logFile,"Checking if passed object is view",0)
					sql13 = "select owner,view_name from all_views where view_name='" + sourceTable + "' and owner='" + sourceSchema + "'"
					cursor2.execute(sql13)
					results=cursor2.fetchall()
					for viewObj in results:
						viewOwner=viewObj[0]
						viewName=viewObj[1]
						sourceType='VIEW'
				if sourceType == 'VIEW':
					logdata(logFile,"Now checking if view " + viewOwner + "." + viewName + " exists in exception list",0)
					sql13 = "select count(1) from edw_view_ingestion where environment_type='" + envType + "' and view_name='" + viewName
					sql13 += "' and db_name='" + sourceDb + "' and schema_name='" + viewOwner + "' and active_flag='Y'"
					cursor1.execute(sql13)
					results=cursor1.fetchall()
					for resultsObj in results:
						excepCnt=resultsObj[0]
					if excepCnt > 0:
						TblFound=True
						excepView=True
						srcBaseschema = sourceSchema
						srcBasetable = sourceTable
						logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
						return f'{srcBaseschema}~{srcBasetable}'
					else:
						logdata(logFile,"Now checking if view " + viewOwner + "." + viewName + " is 1-1",0)
						sql13 = "select referenced_owner,referenced_name from  all_dependencies where name='" + viewName + "' and owner='" + viewOwner + "'"
						sql13 += " and type='VIEW'  and dependency_type='HARD'"
						cursor2.execute(sql13)
						results=cursor2.fetchall()
						vwTblCnt=len(results)
						if vwTblCnt == 1:
							for vwCntObj in results:
								srcBaseschema=vwCntObj[0]
								srcBasetable=vwCntObj[1]
								TblFound=True
						else:
							logdata(errFile,"Source  Oracle object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested",1)
							Subject="Source  Oracle object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested"
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							return -1
				if sourceType == 'VIEW' and not excepView :
					logdata(logFile,"Now checking if view " + viewOwner + "." + viewName + " has all columns as table",0)
					sql13 += " select column_name,data_type from all_tab_columns where table_name='" + srcBasetable + "' and owner='" + srcBaseschema + "'"
					sql13 += " minus " 
					sql13 = "select column_name,data_type from all_tab_columns where table_name='" + sourceTable + "' and owner='" + sourceSchema + "'"
					cursor2.execute(sql13)
					results=cursor2.fetchall()
					vwColCnt=len(results)
					if vwColCnt > 0:
						logdata(errFile,"Source  Oracle object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested",1)
						Subject="Source  Oracle object - " + sourceSchema + "." + sourceTable + " is not 1-1 view.. Hence cannot be ingested"
						errMsg=Subject
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						return -1
				if not TblFound:
					logdata(errFile,"Source Oracle object - " + sourceSchema + "." + sourceTable + " is not a base table.. Hence cannot be ingested",1)
					Subject ="Error: Source Oracle object - " + sourceSchema + "." + sourceTable + " is not a base table.. Hence cannot be ingested"
					errMsg=Subject
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					return -1
				else:
					logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
					return f'{srcBaseschema}~{srcBasetable}'
			else:
				sourceType = 'TABLE'
				srcBaseschema = sourceSchema
				srcBasetable = sourceTable
				logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
				return f'{srcBaseschema}~{srcBasetable}'
		elif dbType.upper() == 'HANA':
			logdata(logFile,f"Checking if passed object {sourceSchema}.{sourceTable} is view",0)
			obj_type=''
			sql=f"select object_type from sys.objects where object_name='{sourceTable}' and schema_name='{sourceSchema}'"
			cursor2.execute(sql)
			results=cursor2.fetchall()
			for results_obj in results:
				obj_type=results_obj[0]
			if obj_type not in ('TABLE','VIEW'):
				raise Exception (f"Passed object {sourceSchema}.{sourceTable} is neither table nor view")
			if obj_type == 'TABLE':
				return f'{sourceSchema}~{sourceTable}'
			elif obj_type == 'VIEW':
				#sql=f"select base_schema_name,base_object_name from SYS.object_dependencies where dependent_object_name = '{sourceTable}'"
				#sql+=f" and dependent_schema_name='{sourceSchema}' and base_object_type='TABLE' and base_schema_name !='_SYS_BIC'"
				#cursor2.execute(sql)
				#results=cursor2.fetchall()
				#if len(results) != 1:
				#	raise Exception (f"Passed object {sourceSchema}.{sourceTable} is not 1-1 View")
				#else:
				#	for results_obj in results:
				#		srcBaseschema=results_obj[0]
				#		srcBasetable=results_obj[1]
				#return f'{srcBaseschema}~{srcBasetable}'
				return f'{sourceSchema}~{sourceTable}'
		elif dbType.upper() == 'HANANRT':
			logdata(logFile,f"Checking if passed object {sourceSchema}.{sourceTable} is view",0)
			obj_type=''
			sql=f"select object_type from sys.objects where object_name='{sourceTable}' and schema_name='{sourceSchema}'"
			cursor2.execute(sql)
			results=cursor2.fetchall()
			for results_obj in results:
				obj_type=results_obj[0]
			if obj_type not in ('VIEW'):
				raise Exception (f"Passed object {sourceSchema}.{sourceTable} is not view. HANANRT supports only view")
			else:
				sql=f"select base_schema_name,base_object_name from SYS.object_dependencies where dependent_object_name = '{sourceTable}'"
				sql+=f" and dependent_schema_name='{sourceSchema}' and base_object_type='TABLE' and base_schema_name not like '%SYS%'"
				sql+=f" AND   BASE_OBJECT_NAME <> 'BATCHES_TO_PUBLISH'"
				cursor2.execute(sql)
				results=cursor2.fetchall()
				if len(results) != 1:
					raise Exception (f"Passed object {sourceSchema}.{sourceTable} is not 1-1 View")
				else:
					for results_obj in results:
						srcBaseschema=results_obj[0]
						srcBasetable=results_obj[1]
				return f'{srcBaseschema}~{srcBasetable}'
		elif dbType.upper() == 'SNOWFLAKE':
			logdata(logFile,f"Checking if streams enabled for passed object {sourceDb}.{sourceSchema}.{sourceTable}",0)
			obj_type=""
			sql=f"show tables like '{sourceTable}' in schema {sourceDb}.{sourceSchema}"
			cursor2.execute(sql)
			results=cursor2.fetchall()
			if len(results) == 1:
				obj_type='TABLE'
			else:
				sql=sql.replace("tables","views")
				cursor2.execute(sql)
				results=cursor2.fetchall()
				if len(results) == 0:
					logdata(logFile,f"Passed Snowflake object {sourceDb}.{sourceSchema}.{sourceTable} is neither a table nor a view.. Hence cannot proceed further",1)
					raise Exception("Invalid object passed")
				else:
					obj_type='VIEW'
			cursor2.execute("show streams")
			sql=f"select count(1) cnt from table(result_scan(last_query_id())) where $7='{sourceDb}.{sourceSchema}.{sourceTable}'"
			cursor2.execute(sql)
			results=cursor2.fetchall()
			if len(results) != 1:
				raise Exception(f"Streams not enabled for {sourceDb}.{sourceSchema}.{sourceTable}. Hence cannot proceed with ingestion")
			return f'{sourceSchema}~{sourceTable}'
	except Exception as e:
		logdata(errFile,"Exception encountered while checking passed object is table or view - " + sourceTable,1)
		logdata(errFile,str(e),1)
		return -1

## Check if source table doesnt have any incremental data to process since last pull.
## In such cases just mark corresponding corresponding JCT's (both SRC2STG and STG2BR as completed)
## If thisis first request and JCT's aren't present then create them for future use. Also create correspoing SS/BR table in SF.
## At the end invoke soft delete (only if it is for PROD) to tie out the audits between source and SF.

def zeroRecIngestion(jobStream,reqid,cursor1,errFile,logFile,emailAdd,envType,LOGS_DIR,cursor3,sourcedbType,ltype):
	global jobGroup,connectionName,sourceDb,sourceSchema,sourceTable,whereClause,incColname,targetDb,targetSchema,targetTable,sfWh,destSchema
	global sfRole,srcBasetable,srcBaseschema,sfEnv,currOsUser
	logdata(logFile,"Now marking SRC2STG and STG2BR workflows as completed since no record to process",1)
	sql=f"select count(*) from edw_job_streams where job_stream_id='{jobStream}'"
	cursor1.execute(sql)
	results=cursor1.fetchall()
	for resultsObj in results:
		jctCnt=resultsObj[0]
	if jctCnt == 0:
		logdata(logFile,f"Job stream - {jobStream} doesnt exist on database.. Hence creating the same for future ingestion",0)
		#whereClauseIns=whereClause.replace("'","''").replace("''''","''")
		whereClauseIns=whereClause.replace("'","''")
		if len(incColname) > 0 and 'NRT' not in connectionName:
			source_del_flag='A'
		else:
			source_del_flag='N'
		InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
		InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
		InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
		InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by,source_deleted_flag)"
		InsQry += " values "
		InsQry += "('" + jobStream + "','" + jobStream + "','" + jobGroup + "',1,'Y','C','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
		InsQry += "','" + sourceTable + "','" + whereClauseIns + "','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
		InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable  + "','" + sfWh + "','" + sfRole + "',NULL"
		InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API','" + source_del_flag + "')"
		cursor1.execute(InsQry)
		logdata(logFile,f"Creating target table",0)
		os.environ['RUN_ENVIRONMENT']=envType
		sql=DDL_Generator.genDDL(sourceDb,srcBaseschema,srcBasetable,'CREATE')
		#print(sql)
		if destSchema == 'BR':
			if 'ETL' in targetDb:
				destDb=targetDb.replace('ETL','BR')
			else:
				raise Exception ("Invalid source DB - " + targetDb + ". Schema chosen is BR. however source DB is not a ETL DB")
		else:
			destDb=targetDb
		conn3=connect_snowflake(targetDb,targetSchema,logFile,errFile,envType,cursor1,emailAdd,sfRole,sfWh,sf_user)
		cursor3=conn3.cursor()
		cursor3.execute('use schema ' + destDb + '.' + destSchema)
		if destSchema == 'BR':
			sql=sql.replace(') if not exists',') DATA_RETENTION_TIME_IN_DAYS=30 if not exists')
			cursor3.execute('use role edw_create_table_on_br_role')
			cursor3.execute('use warehouse edw_datalake_wh')
			cursor3.execute('use schema ' + destDb + '.' + destSchema)
		else:
			sql=sql.replace(') if not exists',') DATA_RETENTION_TIME_IN_DAYS=7 if not exists')
		sql=sql.replace('create table ' + srcBasetable,'create table ' + targetTable)
		create_tbl_sql=sql
		cursor3.execute(sql)
		#print(sql)
		if destSchema == 'BR':
			logdata(logFile,"Now creating BR view",0)
			sql=f"CREATE OR REPLACE SECURE VIEW {destDb}.BR_VIEW.{targetTable} COPY GRANTS AS "
			sql+=f" SELECT * FROM {destDb}.BR.{targetTable} WHERE NVL(EDWSF_SOURCE_DELETED_FLAG,'N')='N'"
			create_view_sql=sql
			cursor3.execute(sql)
			logdata(logFile,"Now transfering ownership of newly created BR table",0)
			stepName="Transfer BR table and view ownership to BM_ROLE per standard"
			cursor3.execute("grant insert,select,update,delete on " + destDb + "." + destSchema + "." + targetTable + " to " + sfRole)
			cursor3.execute("call EDW_AUDIT_DB.AUDIT.BMOWNERSHIP('" + destDb + "." + destSchema + "." + targetTable + "')")
			cursor3.execute(f"call EDW_AUDIT_DB.AUDIT.BMOWNERSHIP('{destDb}.BR_VIEW.{targetTable}')")
			cursor3.execute('use role ' + sfRole)
			cursor3.execute('use warehouse ' + sfWh)
			lf=open('/tmp/logfile','w')
			lf.write(f"Create table SQL \n {create_tbl_sql} \n Create view SQL \n {create_view_sql}")
			subject=f"New BR Table {destDb }.{destSchema}.{targetTable}  in {sfEnv} got created"
			msg=f"\n Source Database: {sourceDb}\n"
			msg+=f"\n Source Schema: {sourceSchema}\n"
			msg+=f"\n Source Table: {sourceTable}\n"
			msg+=f"\n Target Instance: {sfEnv}\n"
			msg+=f"\n Target Database: {destDb}\n"
			msg+=f"\n Target Schema: {destSchema}\n"
			msg+=f"\n Target Table: {targetTable}\n"
			msg+=f"\n Privileges granted to role: {sfRole}\n"
			msg+=f"\n Executed by: {currOsUser}\n"
			msg+=f"\n Completed on: {datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}\n"
			sendEmail('sf-masterbuild@cisco.com',lf,lf,msg,subject,cursor1)
			conn3.close()

	else:
		cursor1.execute(f"Update edw_job_streams set run_status='C' where job_stream_id='{jobStream}'")
		logdata(logFile,"Now pulling corresponding STG2BR to mark complete",0)
		sql=f"select TARGET_DB_NAME,TARGET_SCHEMA,TARGET_DB_CONNECTION,TARGET_TABLE_NAME,EXTRACT_TYPE from  EDW_JOB_STREAMS where job_stream_id='{jobStream}'"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		for resultsObj in results:
			targetDb=resultsObj[0]
			targetSchema=resultsObj[1]
			targetConn=resultsObj[2]
			targetTbl=resultsObj[3]
			load_type=resultsObj[4]
		#sql=f"select job_stream_id from edw_job_streams where source_db_connection='{targetConn}' and source_db_name='{targetDb}' and source_schema='{targetSchema}' and source_table_name='{targetTbl}' and active_ind='Y'"
		sql=f"select job_stream_id,TARGET_DB_NAME,TARGET_SCHEMA,TARGET_TABLE_NAME,EXTRACT_TYPE from edw_job_streams where job_stream_id_link='{jobStream}' and active_ind='Y'"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		if len(results) > 0:
			for resultsObj in results:
				brJobStream=resultsObj[0]
				brdbName=resultsObj[1]
				brdbSchema=resultsObj[2]
				brtbl=resultsObj[3]
				eType=resultsObj[4]
				logdata(logFile,f"Found corresponding BR Jobstream as {brJobStream}. Hence marking it as completed as well",1)
				cursor1.execute(f"Update edw_job_streams set run_status='C' where job_stream_id='{brJobStream}'")
			cursor1.execute("Commit")
			if ltype == 'ALL_DATA' and sourcedbType != 'HANANRT':
				logdata(logFile,"Since its full load and source has 0 rows deleting all rows from tgt db",1)
				conn3=connect_snowflake(targetDb,targetSchema,logFile,errFile,envType,cursor1,emailAdd,sfRole,sfWh,sf_user)
				cursor3=conn3.cursor()
				cursor3.execute(f'delete from {brdbName}.{brdbSchema}."{brtbl}"')
				conn3.close()
		else:
			logdata(logFile,f"Could not find corresponding STG2BR entry.",1)

		updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
		updDIYmstr(reqid,'current_phase','Success',cursor1,errFile,logFile,emailAdd)
		insDIYlines(reqid,'Invoking_Data_Load','Completed_Ingestion',cursor1,errFile,emailAdd)
	if envType == 'PRD':
		#logdata(logFile,"Checking if record exists in audit. If not launching soft delete",1)
		logdata(logFile,"Launching soft delete to update audit dashboard",1)
		#sql=f"select source_db_name,source_schema,source_table_name from edw_job_streams where job_stream_id='{jobStream}'"
		#cursor1.execute(sql)
		#results=cursor1.fetchall()
		#for results_obj in results:
		#	source_db=results_obj[0]
		#	source_schema=results_obj[1]
		#	source_tbl=results_obj[2]
		#if len(results_obj) > 0:
		#	sql=f"select count(1) from EDW_INGESTION_DATA_AUDIT where source_table='{source_tbl}' and source_db='{source_db}' and source_schema='{source_schema}'"
		#	cursor1.execute(sql)
		#	results=cursor1.fetchall()
		#	for results_obj in results:
		#		aud_count=results_obj[0]
		#	if aud_count == 0:
		#		logdata(logFile,f"Since no record exist in audit for {source_tbl} launching soft delete now",0)
		del_enable_count=0
		sql=f"select count(1) from edw_job_streams where job_stream_id='{jobStream}' and (extract_type <> 'ALL_DATA' or source_db_connection='HANANRT_PRD') and source_deleted_flag='A' and active_ind='Y'"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		for del_flag_obj in results:
			del_enable_count=del_flag_obj[0]
		if del_enable_count == 1 and (ltype != 'ALL_DATA' or sourcedbType.upper() == 'HANANRT'):
			cmnd=subprocess.Popen(f"/apps/edwsfdata/python/scripts/DIY_soft_delete.py -e {envType} -l {LOGS_DIR} -j {jobStream} -r {reqid}" ,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
			pid=cmnd.pid
			logdata(logFile,f"Soft delete script launched with pid {pid}",1)
			(output, error) = cmnd.communicate()
			Err=error.decode('ascii')
			Out=output.decode('ascii')
			if (Err is not None and len(Err)>0) or ('exception occurred' in Out):
				logdata(errFile,"Exception encountered while launching soft deletes. Please find below the exception \n" + Err,0)
				print(Err)
				source_delete='FAIL'
			else:
				logdata(logFile,"Completed launching soft deletes. Please find below the output \n" + Out,0)
				print(Out)
				if "Error occurred " in Out:
					logdata(errFile,"Exception encountered while launching soft deletes. Please find below the exception \n" + Err,0)
		else:
			logdata(logFile,f"Not proceeding with soft delete for {jobStream} as source delete flag is not set to A",0)

## Below methiod is used to find out final destination (target) schema in Snowflake. It is wrapper to validate and invoke determine_dest_schema method.

def find_dest_schema(jct,cursor1,logFile,errFile,envType,sourceDb,sourceSchema,sourceTable,sourcedbType,cursor2):
	try:
		logdata(logFile,"Now pulling existing dest schema",0)
		sql=f"select target_schema from edw_job_streams where job_stream_id_link='{jct}' and active_ind='Y'"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		if len(results) == 0:
			logdata(logFile,f"Could not detect SF schema through sql {sql}. Hence attempting to determine the same",0)
			target_schema=determine_dest_schema(envType,sourceDb,sourceSchema,sourceTable,sourcedbType,cursor1,cursor2,logFile,errFile,target_schema_name)
			return target_schema
		else:
			for results_obj in results:
				target_schema=results_obj[0]
		return target_schema
	except Exception as e:
		logdata(errFile,"Exception encountered while checking snowflake target schema for jct - " + jct,1)
		logdata(errFile,sql,1)
		return -1 

## Check if any exception for default rule recorded in TARGET_SCHEMA_OVERRIDE table. If yes use the same.
## If not if source is oracle or hana go with SS, if it is TD go with BR
## if it is Snowflake or presto check if target schema is passed while invoking the JCT and use the same accordingly. If not go with SS

def determine_dest_schema(envType,sourceDb,sourceSchema,sourceTable,sourcedbType,cursor1,cursor2,logFile,errFile,target_schema_name):
	try:
		target_schema=None
		logdata(logFile,"Now determining SF target schema",0)
		sql=f"select target_schema from eds_acl.target_schema_override where environment_name='{envType}' and db_instance_name='{sourceDb}'"
		sql+=f" and db_schema_name='{sourceSchema}' and table_name='{sourceTable}' and active_ind='Y'"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		if len(results) > 0:
			logdata(logFile,f"Since override data exists for {sourceDb}.{sourceSchema}.{sourceTable} leveraging the same",0)
			for results_obj in results:
				target_schema=results_obj[0]
		else:
			if sourcedbType in ('ORACLE','HANA','HANANRT'):
				target_schema='SS'
			elif sourcedbType == 'TERADATA':
				sql=f"select count(1) from dbc.databasesv where databasename=regexp_replace('{sourceSchema}','VWDB','DB') and regexp_instr(ownername,'E3NF')>0"
				cursor2.execute(sql)
				results=cursor2.fetchall()
				for results_obj in results:
					nf_count=results_obj[0]
				if nf_count == 1:
					target_schema='BR'
				else:
					target_schema='SS'
			elif sourcedbType == 'SNOWFLAKE':
				if target_schema_name is None or target_schema_name == '':
					return -1
				else:
					target_schema=target_schema_name
		if target_schema is None:
			return -1
		else:
			logdata(logFile,f"Determined target schema as {target_schema}",0)
			return target_schema
	except Exception as e:
		logdata(errFile,"Exception encountered while determining snowflake target schema ",1)
		logdata(errFile,str(e),1)
		logdata(errFile,sql,1)
		return -1 

## By default framework supports full load (truncate and load) only for tables which has < 1 million rows.
## If a table has > 1M rows then check if such load is approved exceptionally. Such exceptions are recorded in EDW_LARGE_TBL_INGESTION table.

def validate_large_table_ingestion(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile):
	try:
		logdata(logFile,"Now validating large table ingestion in exception list",0)
		sql = f"select count(1) from edw_large_tbl_ingestion where environment_type='{envType}' and table_name='{sourceTable}'"
		sql += f" and db_name='{sourceDb}' and schema_name='{sourceSchema}' and active_flag='Y'"
		cursor1.execute(sql)
		results=cursor1.fetchall()
		for resultsObj in results:
			excepCnt=resultsObj[0]
		if excepCnt > 0:
			logdata(logFile,f"Though table {sourceDb}.{sourceSchema}.{sourceTable} is large, it exists in exception list. Hence proceeding with ingestion",0)
			return 0
		else:
			logdata(logFile,f"Table {sourceDb}.{sourceSchema}.{sourceTable} is large and not part of exception list. Hence not proceeding with ingestion",1)
			return -1
	except Exception as e:
		logdata(errFile,"Exception encountered while mining exception list for large table ingestion ",1)
		logdata(errFile,str(e),1)
		logdata(errFile,sql,1)
		return -1 

## For oracle loads check if incremental column has index if source table has more than 10 Million rows.
## This check is done only as part of intial load when a incremental load is requested for very first time.

def validate_incremental_column_index(envType,sourceDb,sourceSchema,sourceTable,cursor2,logFile,errFile,incColname):
	try:
		logdata(logFile,"Now checking existence of Index on incremental column",0)
		sql=f"select count(1) cnt from all_Ind_columns where table_name='{sourceTable}' and table_owner='{sourceSchema}' and column_name='{incColname}' and column_position=1"
		cursor2.execute(sql)
		results=cursor2.fetchall()
		for resultsObj in results:
			indCnt=resultsObj[0]
		if indCnt > 0:
			logdata(logFile,f"Index exists on incremental column. Hence proceeding with ingestion",0)
			return 0
		else:
			logdata(logFile,f"Table {sourceDb}.{sourceSchema}.{sourceTable} is large and do not have index on inc column {incColname}. Hence JCT's will be disabled",1)
			return -1
	except Exception as e:
		logdata(errFile,"Exception encountered while checking index on inc column for large table ingestion ",1)
		logdata(errFile,str(e),1)
		logdata(errFile,sql,1)
		return -1 


#
## Main program starts here
#
## Version number is recorded in DIY_MASTER table to see which version is used for which ingestion job. 
pgmVersion='8.23'
params=[]
excepView=False
ejcparams={}
## config file which stores all framework config information
configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
BASE_DIR=configFile.Config.BASE_DIR
ftime=datetime.now().strftime('%Y%m%d%H%M%S')
reqid=str(ftime)
rftime=datetime.now().strftime('%Y%m%d%H%M%S%f')
## Directory where data files will be stored
LOGS_DIR=configFile.Config.LOGS_DIR + rftime + '/'
## Script which is used to extract data from source db to flat file
Data_Ext_Scr=configFile.Config.data_extract_scr
## Analyze threshold to check the source stats... If stats are older than the threshold then they wont consider
Anal_Threshold=int(configFile.Config.Anal_Threshold)
## Number of rows which tells whether to split and load for TD
Split_Threshold=int(configFile.Config.Split_Threshold)
## Number of rows which tells whether to split and load for oracle
Split_Orcl_Threshold=int(configFile.Config.Split_Orcl_Threshold)
## Threshold to determine number of splits
Chunk_Orcl_Threshold=int(configFile.Config.Chunk_Orcl_Threshold)
## Number of parallel threads to pull data in parallel
Thread_count=int(configFile.Config.Thread_count)
## Obsolete not in use
Split_Lob_Threshold=int(configFile.Config.Split_Lob_Threshold)
## Obsolete not in use
Thread_Lob_Count=int(configFile.Config.Thread_Lob_Count)
## Obsolete not in use
dist_cnt_limit=int(configFile.Config.Dist_Buck_Limit)
retryFlag='FALSE'
jctCnt=0
tblSize=1
split4upd=1
surcIndColFormat=''
parser = argparse.ArgumentParser(description='Script to Peform data ingestion into Snowflake',
    epilog='Example: python DIY_src2stg.py -i ingestion_type (db/file/table/jct) -f param_file -t sourcedb.schema.tablename -e env_type -c thread_count -s split_count -n split_column -j job_stream_id -a email -r sf_role_to_use -o mail_on_success -b customJctId -u snowflakeUser -k targetSchema -g targetDbconn -m mergeType')
parser.add_argument('-i', '--type',required=True,
    help='What is the source for ingestion. Another database or a flat file or already ingested table ')
parser.add_argument('-f','--file',
	help='Full name of parameter file with required details for ingestion')
parser.add_argument('-t','--table',
	help='Name of already ingested sourcedb.schema.tablename')
parser.add_argument('-e','--env',
	help='Envrionment type (TS*/DV*/PRD) of Job that is already ingested')
parser.add_argument('-c','--threadcnt',
	help='Thread count if you want to override program settings')
parser.add_argument('-s','--splitcnt',
	help='Split count if you want to override program settings')
parser.add_argument('-n','--splitcmn',
	help='Split column if you want to override program settings')
parser.add_argument('-j','--jobstreamid',
	help='Existing job stream id which you want to ingest')
parser.add_argument('-a','--email',required=True,
	help='Comma seperated email list to which notification needs to be sent out on ingestion status')
parser.add_argument('-r','--role',
	help='Snowflake role to use. By default existing role from JCT will be used, incase of new JCT EDW_DATALAKE_ROLE will be used')
parser.add_argument('-o','--mailOnSucc',
	help='Send an Email on succesful completion. Valid values are Yes/No')
parser.add_argument('-b','--custJct',
	help='Custom JCT ID if you want to have one of your wish.')
parser.add_argument('-u','--snowflakeUser',
	help='Username for connecting to Snowflake environment.')
parser.add_argument('-k','--targetSchema',
	help='Target Schema. Applicable only for ingestion from Snowflake to other platforms')
parser.add_argument('-g','--targetDbconn',
	help='Target DB Connection details. Applicable only for ingestion from Snowflake to other platforms')
parser.add_argument('-m','--mergeType',
	help='Merge Type if default ones needs to be overwritten for some reason. Valid values are OVERWRITE, APPEND and MERGE_ONE2ONE')
args=parser.parse_args()
itype=args.type
pfile=args.file
tableName=args.table
envType=args.env
if args.threadcnt is not None:
	tCnt=int(args.threadcnt)
else:
	tCnt=args.threadcnt
if args.splitcnt is not None:
	sCnt=int(args.splitcnt)
else:
	sCnt=args.splitcnt
if args.splitcmn is not None:
	sCmn=args.splitcmn.strip()
else:
	sCmn=args.splitcmn
jobStreamid=args.jobstreamid
emailAdd=args.email
sfRole=args.role
sfRoleParam=args.role
mailonSucc=args.mailOnSucc
custJct=args.custJct
sf_user=args.snowflakeUser
if sf_user is None:
	sf_user='EDW_DATALAKE_SVC'
target_schema_name=args.targetSchema
target_db_conn=args.targetDbconn
mType=args.mergeType
if mType is None:
	mType='MERGE_ONE2ONE'
	merge_override=False
else:
	if mType not in ('MERGE_ONE2ONE','OVERWRITE','APPEND'):
		print('Invalid parameter value passed for merge type (-m). Please check and retry')
		sys.exit(1)
	merge_override=True
sfWh=None
surcIndColtype=''
conn3=''
MERGE_KEY_NOT_FOUND=False
workflow_type=''
## Creation of logs dir and log file
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
## First time when ingestion happens it will belong to INGESTION_GROUP. At the end it will move to original job group
ijobGroup='INGESTION_GROUP'
logdata(logFile,'Validating Parameters',0)
validate_params(itype,pfile,tableName,envType,tCnt,jobStreamid,emailAdd,mailonSucc)
## Check for thread count override
if tCnt is None:
	tCnt=Thread_count
else:
	Thread_count=int(tCnt)
## Ingestion can be invoked for a specific table by passing table name as parameter.. But this is possible for only already ingested table and setup exists.
## All metadata required for ingestion are built into an array and parameters are read from array for processing.
if itype.lower() == 'table':
	logdata(logFile,'Ingesting data for existing JCT table',0)
	logdata(logFile,'Connecting to repository db',0)
	conn = open_oracle_connection(envType,None,True)
	if type(conn) is int:
		#print ("Error occurred while connecting to Repository database")
		logdata(errFile,"Error occurred while connecting to Repository database",1)
		#print(str(e))
		logdata(errFile,str(e),1)
		Subject ="Error:" + "Error occurred while connecting to Repository database"
		errMsg=Subject + "\n\n" + str(e)
		sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)
	tblDict=tableName.split('.')
	cursor1=conn.cursor()
	logdata(logFile,'Validating existing JCT entry',0)
	sql = "SELECT '" + envType + "',job_group_id,parameter_category,source_db_name,source_schema,source_table_name,target_db_name,"
	sql = sql + "target_schema,target_table_name,source_db_connection,incremental_column_Name,nvl(where_clause,''),job_stream_id,sf_role,sf_warehouse from ( SELECT * FROM EDW_JOB_STREAMS a,EDS_DATA_CATALOG.EDW_PARAMETER b where a.workflow_type='SRC2STG' "
	sql = sql + " and a.source_db_name='" + tblDict[0] + "' and a.source_schema='" + tblDict[1] + "' and a.source_table_name='" + tblDict[2] + "' and a.active_ind='Y'"
	sql = sql + " and a.source_db_connection=b.parameter_type and b.parameter_name='SOURCE_LOGIN' and b.environment_name=decode(replace(sys_context('userenv','current_schema'),'EJC',''),'','PRD',replace(sys_context('userenv','current_schema'),'EJC','')) "
	sql = sql + " order by length(job_stream_id)) where rownum=1"
	#print ("SQL is - " + sql)
	cursor1.execute(sql)
	results=cursor1.fetchall()
	if len(results) != 1:
		#print ("Invalid source table - " + tableName + " for ingestion. Existing JCT entry with this table not found in repository. Please check and retry")
		logdata(errFile,"Invalid source table - " + tableName + " for ingestion. Either JCT entry with this table not found or multiple entries found in repository. Please check and retry",1)
		Subject ="Error: Invalid source table - " + tableName + " for ingestion. Either JCT entry with this table not found or multiple entries found in repository. Please check and retry"
		errMsg=Subject
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		sys.exit(9)
	else:
		#print ("length of resultset is " + str(len(results)))
		#i=0
		logdata(logFile,'Building object array',0)
		for resObj in results:
			#print("resobj is " + str(resObj))
			paramarr=''
			paramarr=resObj[0]
			paramarr = paramarr + '~' + resObj[1]
			paramarr = paramarr + '~' + resObj[2]
			paramarr = paramarr + '~' + resObj[3]
			paramarr = paramarr + '~' + resObj[4]
			paramarr = paramarr + '~' + resObj[5]
			paramarr = paramarr + '~' + resObj[6]
			paramarr = paramarr + '~' + resObj[7]
			paramarr = paramarr + '~' + resObj[8]
			paramarr = paramarr + '~' + resObj[9]
			if resObj[10] == None:
				paramarr = paramarr + '~'
			else:
				paramarr = paramarr + '~' + resObj[10]
			if resObj[11] == None:
				paramarr = paramarr + '~'
			else:
				paramarr = paramarr + '~' + resObj[11]
			params.append(paramarr.split('~'))
			jobStreamid=resObj[12]
			if sfRole is None:
				sfRole=resObj[13]
			sfWh=resObj[14]
			#i+=i
		#print(params)
		#sys.exit(0)
# End of indentation for ingestion for existing table		
## This block will be used if existing JCT is passed as parameter.
## All scheduled jobs will fall under this category.
if itype.lower() == 'jct':
	logdata(logFile,'Ingesting data for existing JCT entry',0)
	logdata(logFile,'Connecting to repository db',0)
	conn = open_oracle_connection(envType,None,True)
	if type(conn) is int:
		#print ("Error occurred while connecting to Repository database")
		logdata(errFile,"Error occurred while connecting to Repository database",1)
		#print(str(e))
		logdata(errFile,str(e),1)
		Subject ="Error: Error occurred while connecting to Repository database"
		errMsg=Subject + "\n\n" + str(e)
		sendEmailwc(emailAdd,logFile,errFile,errMsg,Subject)
	#tblDict=tableName.split('.')
	cursor1=conn.cursor()
	logdata(logFile,'Validating existing JCT entry',0)
	sql = "SELECT '" + envType + "',job_group_id,parameter_category dbtype,source_db_name,source_schema,source_table_name,target_db_name,"
	sql = sql + "target_schema,target_table_name,source_db_connection,incremental_column_Name,nvl(where_clause,''),sf_role,sf_warehouse,"
	sql = sql+ "target_db_connection,workflow_type from ( SELECT * FROM EDW_JOB_STREAMS a ,EDS_DATA_CATALOG.EDW_PARAMETER b where a.workflow_type in ('SRC2STG','FF2STG') "
	sql = sql + " and a.active_ind='Y' and a.job_stream_id='" + jobStreamid + "'"
	sql = sql + " and a.source_db_connection=b.parameter_type and b.parameter_name='SOURCE_LOGIN' and b.environment_name=decode(replace(sys_context('userenv','current_schema'),'EJC',''),'','PRD',replace(sys_context('userenv','current_schema'),'EJC','')))"
	#print ("SQL is - " + sql)
	cursor1.execute(sql)
	results=cursor1.fetchall()
	if len(results) != 1:
		#print ("Invalid source table - " + tableName + " for ingestion. Existing JCT entry with this table not found in repository. Please check and retry")
		logdata(errFile,"Invalid JCT - " + jobStreamid + " for ingestion. Either JCT entry not found in repository or not in active status. Please check and retry",1)
		Subject ="Error: Invalid JCT - " + jobStreamid + " for ingestion. Either JCT entry not found in repository or not in active status. Please check and retry"
		errMsg=Subject
		sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		sys.exit(9)
	else:
		#print ("length of resultset is " + str(len(results)))
		#i=0
		logdata(logFile,'Building object array',0)
		for resObj in results:
			#print("resobj is " + str(resObj))
			paramarr=''
			paramarr=resObj[0]
			paramarr = paramarr + '~' + resObj[1]
			paramarr = paramarr + '~' + resObj[2]
			paramarr = paramarr + '~' + resObj[3]
			paramarr = paramarr + '~' + resObj[4]
			paramarr = paramarr + '~' + resObj[5]
			paramarr = paramarr + '~' + resObj[6]
			paramarr = paramarr + '~' + resObj[7]
			paramarr = paramarr + '~' + resObj[8]
			paramarr = paramarr + '~' + resObj[9]
			if resObj[10] == None:
				paramarr = paramarr + '~'
			else:
				paramarr = paramarr + '~' + resObj[10]
			if resObj[11] == None:
				paramarr = paramarr + '~'
			else:
				paramarr = paramarr + '~' + resObj[11]
			params.append(paramarr.split('~'))
			if sfRole is None:
				sfRole=resObj[12]
			sfWh=resObj[13]
			target_db_conn=resObj[14]
			workflow_type=resObj[15]
			#i+=i
		#print(params)
		#sys.exit(0)
# End of indentation for ingestion for existing job stream		
if itype.lower() == 'db':
	logdata(logFile,'Ingesting data from source database',0)
	vfile=open(pfile,"r")
	for line in vfile:
		if not line.strip().startswith("#"):
			if len(line.strip())>0:
				params.append(line.split('~'))
	vfile.close()
	logdata(logFile,'Building object array',0)
	for i in range(len(params)):
		for j  in range(len(params[i])-2):
			if params[i][j]=="":
				#print ("Found null value in parameter file.. Please check and rerun ingestion for line number - " + str(i+1) + " in the input file.. Proceeding with next one")
				logdata(errFile,"Found null value in parameter file.. Please check and rerun ingestion for line number - " + str(i+1) + " in the input file.. Proceeding with next one",1)
				Subject ="Error: Found null value in parameter file.. Please check and rerun ingestion for line number - " + str(i+1) + " in the input file.. Proceeding with next one"
				errMsg=Subject
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				sys.exit(8)
	#if sfRole is None:
	#	sfRole='EDW_DATALAKE_ROLE'
	#	sfWh='EDW_DATALAKE_WH'
	#elif sfWh is None:
	#	whSuffix=sfRole.replace('EDW_','').replace('_ETL_ROLE','')
	#	sfWh='EDW_I_' + whSuffix + '_WH'
# End of indentation for ingestion from source db
#currOsUser=os.getlogin()
## Check the current user to login into DIY_MASTER. At times below function doesnt return current o/s user 
## To handle that different method is created inside metadata handler and same is used to get the current o/s user
## This is not the sudo'ed user (which always will be edwadm), but the parent user who accessed the host.
currOsUser=os.popen("who am i | awk '{print $1}'").read().strip("\n")
if currOsUser == "":
	try:
		currOsUser=os.getlogin()
	except Exception as e:
		currOsUser=c_getuser()
		pass
## Query tag to be used while connecting to Snowflake.
sf_query_tag=f"Module=SRC2STG;User={currOsUser}"
## This workflow is created to process flat files, but currently not in use.
if workflow_type == 'FF2STG':
	reqid=str(datetime.now().strftime('%Y%m%d%H%M%S%f'))
	sql=f"select extract_type et,workflow_type wt,src2stg_ff_path sfp,run_status rs from edw_job_streams where job_stream_id='{jobStreamid}'"
	jct_obj=execute_df_qry(conn,sql,reqid,conn)
	if len(jct_obj) == 0:
		logdata(errFile,"Invalid JCT {jobStreamid}. Please check and retry",1)
		sys.exit(1)
	eType=jct_obj["et"][0]
	wType=jct_obj["wt"][0]
	sfp=jct_obj["sfp"][0]
	run_status=jct_obj["rs"][0]
	if eType != 'STREAM':
		logdata(errFile,f"Invalid Extract type {eType} for jct {jobStreamid}. Please check and retry",1)
		sys.exit(1)
	if wType != 'FF2STG':
		logdata(errFile,f"Invalid Workflow type {wType} for jct {jobStreamid}. Please check and retry",1)
		sys.exit(1)
	if not os.path.exists(os.path.dirname(sfp)):
		logdata(errFile,f"Invalid Data file directory {sfp} for jct {jobStreamid}. Please check and retry",1)
		sys.exit(1)
	if run_status == 'C':
		logdata(errFile,f"Job stream {jobStreamid} is in completed status. Please reinitialize and retry",1)
		sys.exit(0)
	elif run_status == 'R':
		sql=f"select max(reqid) reqid from diy_master where attribute3='{jobStreamid}'"
		reqid_obj=execute_df_qry(conn,sql,reqid,conn)
		reqid=reqid_obj["reqid"][0]
		sql=f"select logs_dir ld from diy_master where reqid={reqid}"
		ld_obj=execute_df_qry(conn,sql,reqid,conn)
		LOGS_DIR=ld_obj["ld"][0]
	cmnd=subprocess.Popen(f"/users/edwadm/mkumarap/DIY_FF2STG.py -e {envType} -j {jobStreamid} -a {emailAdd} -r {reqid} -l {LOGS_DIR} " ,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	pid=cmnd.pid
	print(f"Flat file upload script launched with pid {pid}")
	(output, error) = cmnd.communicate()
	Err=error.decode('ascii')
	Out=output.decode('ascii')
	if (Err is not None and len(Err)>0) or ('Error occurred' in Out):
		logdata(errFile,f"Exception encountered while launching flat file upload. Please find below the exception \n" + Err,1)
	else:
		logdata(logFile,f"Completed launching flat file upload. Please find below the output \n" + Out,1)
		logdata(logFile,f"Now launching stg2br script to push data from stg to ss/br DIY_stg2br.py -e {envType} -j {jobStreamid} -a {emailAdd} -d SS -m MERGE_ONE2ONE" ,1)
		cmnd=subprocess.Popen(f"/apps/edwsfdata/python/scripts/DIY_stg2br.py -e {envType} -j {jobStreamid} -a {emailAdd} -d SS -m MERGE_ONE2ONE",stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
		pid=cmnd.pid
		logdata(logFile,f"stg2br script launched with pid {pid}",1)
		(output, error) = cmnd.communicate()
		Err=error.decode('ascii')
		Out=output.decode('ascii')
		if Err is not None and len(Err)>0:
			logdata(errFile,"Exception encountered while launching stg2br. Please find below the exception \n" + Err,0)
			print(Err)
		else:
			logdata(logFile,"Completed launching stg2br. Please find below the output \n" + Out,0)
			print(Out)


if itype.lower() == 'db' or itype.lower() == 'table' or (itype.lower()=='jct' and workflow_type != 'FF2STG'):
	logdata(logFile,'Inside main loop',0)
	tblCntvar=0
	eType='ALL_DATA'
	incColname=None
	init_thread_count=Thread_count
	for i in range(len(params)):
		try:
			reqid=str(datetime.now().strftime('%Y%m%d%H%M%S%f'))
			envType=params[i][0]
			SuccFlag=False
			stg2brFail=False
			wfType='SRC2STG'
			existFlag='FALSE'
			retryFlag='FALSE'
			disableJct=False
			spaceCheck=False
			aud_num_rows=0
			part_name=-1
			Thread_count=init_thread_count
			lastUpdColFlag=False
			if sCnt is None:
				splitcount=1
				threadoverride=False
			else:
				try:
					splitcount=int(sCnt)
				except:
					print("Invalid value for Splitcount - " + sCnt + ". Split count needs to be a integer if you want to override. Please check and retry")
					sys.exit(8)
				threadoverride=True
			stageMnt='/apps/edwsfdata/' + envType
			jobGroup=params[i][1].strip()
			sourcedbType=params[i][2].strip()
			sourceDb=params[i][3].strip()
			sourceSchema=params[i][4].strip()
			sourceTable=params[i][5].strip()
			targetDb=params[i][6].strip()
			targetSchema=params[i][7].strip()
			targetTable=params[i][8].strip()
			connectionName=params[i][9].strip()
			incColname=params[i][10].strip()
			whereClause=params[i][11].strip('\n')
			sf_query_tag=f"Module=SRC2STG;User={currOsUser};Table={targetDb}.{targetSchema}.{targetTable}"
			os.environ["sf_query_tag"]=sf_query_tag
			## By default Framework decides the value for JCT based on standards... Just incase if it needs to be overridden JCT name can be passed using -b verb
			if jobStreamid is None:
				if custJct is not None:
					jobStream=custJct
				else:
					if '/' in sourceTable:
						table_2_use=sourceTable.split('/')[1]
					else:
						table_2_use=sourceTable
					jobStream = 'JOB_' + wfType + '_' + sourceDb + '_' + table_2_use
			else:
				jobStream=jobStreamid
			#if sourcedbType.upper() == 'ORACLE':
			#	Talend_Scr='SRC2STG_ORCL.sh'
			#elif sourcedbType.upper() == 'TERADATA':
			#	Talend_Scr='SRC2STG_TPT.sh'
			logdata(logFile,'Processing ingestion for - ' + jobStream,0)
			#logdata(errFile,'Processing ingestion for - ' + jobStream,0)
			## 4.81 change
			if itype.lower() == 'db' and sfRoleParam is None:
				## Determine Snowflake role and warehouse based on Target datbase name
				sfRole=targetDb.replace('ETL_DB','COMMON_ETL_ROLE').replace('_DV3','').replace('_TS3','').replace('_TSA','')
				if sfRole == targetDb:
					sfRole='EDW_DATALAKE_ROLE'
					sfWh='EDW_DATALAKE_WH'
				else:
					whSuffix=sfRole.replace('EDW_','').replace('_ETL_ROLE','')
					sfWh='EDW_I_' + whSuffix + '_WH'
			elif sfRole is None:
				sfRole=targetDb.replace('ETL_DB','COMMON_ETL_ROLE').replace('_DV3','').replace('_TS3','')
				if sfRole == targetDb:
					sfRole='EDW_DATALAKE_ROLE'
					sfWh='EDW_DATALAKE_WH'
				else:
					whSuffix=sfRole.replace('EDW_','').replace('_ETL_ROLE','')
					sfWh='EDW_I_' + whSuffix + '_WH'
			else:
				if sfWh != 'EDW_DATALAKE_WH' and sfWh != 'REPLICATION_POC_WH' and sfRole != 'EDW_DATALAKE_ROLE':
					whSuffix=sfRole.replace('EDW_','').replace('_ETL_ROLE','')
					sfWh='EDW_I_' + whSuffix + '_WH'
			if itype.lower() == 'db':
				logdata(logFile,"Now triggering data ingestion for " + sourceDb + "." + sourceSchema + "." + sourceTable,1)
				## Open connection to repository DB
				conn = open_oracle_connection(envType,None,True)
				if type(conn) is int:
					#print ("Error occurred while connecting to Repository database")
					logdata(errFile,"Error occurred while connecting to Repository database",1)
					#print(str(e))
					logdata(errFile,str(e),1)
					Subject ="Error: Error occurred while connecting to Repository database"
					errMsg=Subject + "\n\n" + str(e)
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			try:
				srcdbParams={}
				tgtdbParams={}
				cursor1=conn.cursor()
				## Read source database connection creds from repository db
				logdata(logFile,'Gathering source db connection detais from repository',0)
				sql1="select parameter_name,to_char(parameter_value) from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "' and upper(parameter_category)='" + sourcedbType.upper() + "' and parameter_type ='" + connectionName + "'"
				#print(sql1)
				cursor1.execute(sql1)
				results=cursor1.fetchall()
				if len(results) == 0:
					#print ("Could not find connection details for connection name - " + connectionName.strip('\n') + " for DB type - " + sourcedbType + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion")
					logdata(errFile,"Could not find connection details for connection name - " + connectionName.strip('\n') + " for DB type - " + sourcedbType + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion",1)
					Subject ="Error: Could not find connection details for connection name - " + connectionName.strip('\n') + " for DB type - " + sourcedbType + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion"
					errMsg=Subject
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
				for obj in results:
					param=obj[0]
					value=obj[1]
					srcdbParams[param]=value
				#print(srcdbParams)
			except  Exception as e:
				#print("Exception occurred while selecting source db connection details")
				logdata(errFile,"Exception occurred while selecting source db connection details",1)
				#print(str(e))
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while selecting source db connection details"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			try:
				if sourcedbType != 'SNOWFLAKE':
					## Gather Snowflake user name from JCT to establish connection to Snowflake.
					logdata(logFile,"Gathering snowflake user to connect",0)
					sql1=f"select count(1) from edw_job_streams where job_stream_Id='{jobStream}' and active_ind='Y'"
					cursor1.execute(sql1)
					results=cursor1.fetchall()
					for results_obj in results:
						jobstream_cnt=results_obj[0]
					if len(results) > 0:
						sql1=f"select to_char(parameter_value) sf_username from eds_data_catalog.edw_parameter a, edw_job_streams b "
						sql1+=f" where b.job_stream_Id='{jobStreamid}' and b.active_ind='Y' and b.target_db_connection=a.parameter_type"
						sql1+=f" and a.parameter_category='SNOWFLAKE' and a.environment_name='{envType}' and a.parameter_name='CONNECTION_USER'"
						cursor1.execute(sql1)
						results=cursor1.fetchall()
						for results_obj in results:
							sf_user=results_obj[0]
					logdata(logFile,f"Obtained snowflake user as {sf_user}",0)
				else:
					## If not (TD being only other option today) gather connection details from repository
					if target_db_conn is None or target_db_conn == '':
						raise Exception("Target DB connection is mandatory for which destination is not Snowflake.")
					logdata(logFile,"Gathering target db connection details",0)
					targetdbType=""
					sql=f"select distinct parameter_category from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='{envType}' and parameter_type ='{target_db_conn}'"
					cursor1.execute(sql)
					results=cursor1.fetchall()
					for obj in results:
						targetdbType=obj[0]
					sql=f"select parameter_name,to_char(parameter_value) from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='{envType}'"
					sql+=f" and upper(parameter_category)='{targetdbType}' and parameter_type ='{target_db_conn}'"
					cursor1.execute(sql)
					results=cursor1.fetchall()
					if len(results) == 0:
						#print ("Could not find connection details for connection name - " + connectionName.strip('\n') + " for DB type - " + sourcedbType + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion")
						logdata(errFile,"Could not find connection details for connection name - " + connectionName.strip('\n')  + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion",1)
						Subject ="Error: Could not find connection details for connection name - " + connectionName.strip('\n')  + " for env " + envType + " in Repository database.. Hence cannot proceed with ingestion"
						errMsg=Subject
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
					for obj in results:
						param=obj[0]
						value=obj[1]
						tgtdbParams[param]=value
					sql2="SELECT UTL_I18N.RAW_TO_CHAR('"+ tgtdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') from dual"
					cursor1.execute(sql2)
					results=cursor1.fetchall()
					for passobj in results:
						tgtPassword=passobj[0]
			except  Exception as e:
				logdata(errFile,"Exception occurred while gathering snowflake connection user",1)
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while gathering snowflake connection user"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)

			try:
				## Decode source database password
				sql2="SELECT UTL_I18N.RAW_TO_CHAR('"+ srcdbParams['SOURCE_LOGIN_PASSWORD'] +"','AL32UTF8') from dual"
				cursor1.execute(sql2)
				results=cursor1.fetchall()
				for passobj in results:
					srcPassword=passobj[0]
			except Exception as e:
				#print ("Exception occurred while decoding source login password")
				logdata(errFile,"Exception occurred while decoding source login password",1)
				#print(str(e))
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while decoding source login password"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			try:
				## Check if provided job group already exists. If not set it up by creating directory and inserting a record into edw_job_groups table.
				logdata(logFile,"Checking for existing JOb group entry for Job group - " + jobGroup,0)
				sql17="Select count(*) from edw_job_groups where job_group_id='" + jobGroup + "'"
				cursor1.execute(sql17)
				results=cursor1.fetchall()
				for resCntobj in results:
					resCnt=resCntobj[0]
				if resCnt == 0:
					logdata(logFile,"Missing job group - " + jobGroup + " for " + envType + ". Hence creating it now",1)
					sql18="insert into edw_job_groups values ('" + jobGroup + "','" + jobGroup + "','Y','P','created through API for ingesting " + jobStream + "',NULL,'Y','A',sysdate,'API',sysdate,'API')"
					cursor1.execute(sql18)
					cursor1.execute('commit')
				out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + jobGroup + '/' + 'data/inbox').read()
			except Exception as e:
				logdata(errFile,"Exception occurred while making new job group entry.. Hence aborting ingestion for jobstream " + jobStream,1)
				#print(str(e))
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while making new job group entry.. Hence aborting ingestion for jobstream " + jobStream
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			try:
				## Check for existence of Ingestion group.
				logdata(logFile,"Checking for existing Ingestion JOb group entry for Job group - " + ijobGroup,0)
				sql17="Select count(*) from edw_job_groups where job_group_id='" + ijobGroup + "'"
				cursor1.execute(sql17)
				results=cursor1.fetchall()
				for resCntobj in results:
					resCnt=resCntobj[0]
				if resCnt == 0:
					logdata(logFile,"Missing job group - " + ijobGroup + " for " + envType + ". Hence creating it now",1)
					sql18="insert into edw_job_groups values ('" + ijobGroup + "','" + ijobGroup + "','Y','P','created through API',NULL,'Y','A',sysdate,'API',sysdate,'API')"
					cursor1.execute(sql18)
					cursor1.execute('commit')
				out=os.popen('mkdir -p /users/edwadm/' + envType + '/' + ijobGroup + '/' + 'data/inbox').read()
			except Exception as e:
				logdata(errFile,"Exception occurred while making ingestion job group entry.. Hence aborting ingestion for jobstream " + jobStream,1)
				#print(str(e))
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while making ingestion job group entry.. Hence aborting ingestion for jobstream " + jobStream
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			try:
				## Validate if any other ingestion for same table is already in progress. This is done by comparing end_time in DIY_MASTER table.
				logdata(logFile,f"Checking if currently ingestion is in progress for table - {sourceDb}.{sourceSchema}.{sourceTable}" ,0)
				sql3="select reqid,end_time from("
				sql3+=" select reqid,row_number() over (partition by source_db_name,source_schema,source_table order by start_time desc) as row_num,end_time from diy_master"
				sql3+=f" where source_table='{sourceTable}' and source_db_name='{sourceDb}' and source_schema='{sourceSchema}') where row_num=1"
				cursor1.execute(sql3)
				results=cursor1.fetchall()
				for pend_time_obj in results:
					preq_id=pend_time_obj[0]
					pend_time=pend_time_obj[1]
				if len(results) > 0:
					if pend_time == '' or pend_time is None:
						Subject=f"Looks like ingestion is in progress for table - {sourceDb}.{sourceSchema}.{sourceTable}. If not cleanup last triggered attempt and retry"
						logdata(errFile,Subject,1)
						sendEmail(emailAdd,logFile,errFile,Subject,Subject,cursor1)
						continue
				## Check if it is a retry job
				logdata(logFile,"Checking for running JCT entry with job_stream_id - " + jobStream,0)
				#sql3="SELECT count(*) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "%' and RUN_STATUS in ('R')"
				sql3="SELECT count(*) from EDW_JOB_STREAMS where (JOB_STREAM_ID like '"   + jobStream + "' and RUN_STATUS in ('R','A'))"
				sql3=sql3 + " or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and RUN_STATUS in ('R','P')) and workflow_type='SRC2STG' and active_ind='Y'"
				cursor1.execute(sql3)
				results=cursor1.fetchall()
				for cntObj in results:
					jobCnt=cntObj[0]
				if jobCnt > 0:
					#print("Job control entry - " + jobStream + " already exists on repository under running status.. Hence retrying the jobs now")
					logdata(logFile,"Job control entry - " + jobStream + " already exists on repository under running status.. Hence retrying the jobs now",1)
					retryFlag='TRUE'
					sql33="select max(reqid) from DIY_MASTER where source_table='" + sourceTable + "' and source_db_name='" + sourceDb + "' and source_schema='" + sourceSchema + "'"
					cursor1.execute(sql33)
					results=cursor1.fetchall()
					for maxreqidObj in results:
						reqid=maxreqidObj[0]
					if len(results) == 0 or reqid is None:
						reqid=str(datetime.now().strftime('%Y%m%d%H%M%S%f'))
						retflg='F'
						currstat='INIT'
						#currOsUser=os.getlogin()
						currHostName=os.uname()[1]
						if splitcount == 1:
							tCnt=1
						sqlins="insert into DIY_master(reqid,Ingestion_type,logs_dir,param_file,env_type,job_group,source_db_name,source_db_type,source_schema,source_table,target_schema,"
						sqlins=sqlins + "target_table,Thread_count,split_count,current_phase ,start_time,retryflag,attribute1,attribute2,attribute3,attribute4,attribute5) values "
						sqlins=sqlins + " (" + reqid + ","
						sqlins=sqlins + "'" + itype.upper() + "',"
						sqlins=sqlins + "'" + LOGS_DIR + "',"
						if pfile is not None:
							sqlins=sqlins + "'" + pfile + "',"
						else:
							sqlins=sqlins + " NULL,"
						sqlins=sqlins + "'" + envType.upper() + "',"
						sqlins=sqlins + "'" + jobGroup + "',"
						sqlins=sqlins + "'" + sourceDb + "',"
						sqlins=sqlins + "'" + sourcedbType + "',"
						sqlins=sqlins + "'" + sourceSchema + "',"
						sqlins=sqlins + "'" + sourceTable + "',"
						sqlins=sqlins + "'" + targetSchema + "',"
						sqlins=sqlins + "'" + targetTable + "',"
						sqlins=sqlins + str(tCnt) + ","
						sqlins=sqlins + str(splitcount) + ","
						#sqlins=sqlins + "'" + currstat + "',sysdate,'" + retflg + "')"
						sqlins=sqlins + "'" + currstat + "',sysdate,'" + retflg + "','" + currOsUser + "','" + emailAdd + "','" + jobStream + "','" + currHostName + "','" + pgmVersion + "')"
						try:
							cursor1.execute(sqlins)
							cursor1.execute('commit')
						except Exception as e:
							logdata(errFile,"Exception occurred while inserting DIY master record",1)
							logdata(errFile,sqlins,0)
							logdata(errFile,str(e),1)
							Subject ="Error: Exception occurred while inserting DIY master record"
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						insDIYlines(reqid,'NONE','INIT',cursor1,errFile,emailAdd)
			except  Exception as e:
				#print ("Exception occurred while checking job control run status")
				logdata(errFile,"Exception occurred while checking job control run status",1)
				#print(str(e))
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while checking job control run status"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			## If source is not Snowflake, target has to be Snowflake. Hence establish connection to Snowflake. If not establish connection to respective target platform
			if sourcedbType != 'SNOWFLAKE':
				conn3=connect_snowflake(targetDb,targetSchema,logFile,errFile,envType,cursor1,emailAdd,sfRole,sfWh,sf_user)
				if type(conn3) == int:
					raise Exception("Exception encountered while establing SF connection. Please check")
			else:
				if targetdbType == 'TERADATA':
					conn3=teradatasql.connect(host=tgtdbParams['SOURCE_HOST'],user=tgtdbParams['SOURCE_LOGIN'], password=tgtPassword)
				else:
					raise Exception("Unsupported Target db Type. Currently Teradata is only supported db type")
			cursor3=conn3.cursor()
			if type(conn3) is int:
				logdata(logFile,"Exception encoutnered while connection to snowflake",1)
				continue
			else:
				conn3.close()
			## Update existing DIY_MASTER entry if it is a retry job and if not create a new record into DIY_MASTER table
			if retryFlag=="TRUE":
				retflg='T'
				currstat='Split_Phase'
				#currOsUser=os.getlogin()
				currHostName=os.uname()[1]
				if splitcount == 1:
					tCnt=1
				sqlupd="update DIY_master set logs_dir='" + LOGS_DIR + "',job_group='" + jobGroup + "',target_schema='" + targetSchema + "',target_table='" + targetTable + "'"
				sqlupd=sqlupd + ",Thread_count=" + str(tCnt)  + ",retryflag='T',attribute1='" + currOsUser + "',err_msg=NULL,attribute2='" + emailAdd + "'"
				sqlupd=sqlupd + ",attribute4='" + currHostName + "',attribute5='" + pgmVersion + "' where reqid=" + str(reqid)
				try:
					cursor1.execute(sqlupd)
					cursor1.execute('commit')
				except Exception as e:
					logdata(errFile,"Exception occurred while updating DIY master record",1)
					logdata(errFile,sqlupd,0)
					logdata(errFile,str(e),1)
					Subject ="Error: Exception occurred while updating DIY master record"
					errMsg=Subject + "\n\n" + str(e)
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
			else:
				retflg='F'
				currstat='INIT'
				#currOsUser=os.getlogin()
				currHostName=os.uname()[1]
				if splitcount == 1:
					tCnt=1
				sqlins="insert into DIY_master(reqid,Ingestion_type,logs_dir,param_file,env_type,job_group,source_db_name,source_db_type,source_schema,source_table,target_schema,"
				sqlins=sqlins + "target_table,Thread_count,split_count,current_phase ,start_time,retryflag,attribute1,attribute2,attribute3,attribute4,attribute5) values "
				sqlins=sqlins + " (" + reqid + ","
				sqlins=sqlins + "'" + itype.upper() + "',"
				sqlins=sqlins + "'" + LOGS_DIR + "',"
				if pfile is not None:
					sqlins=sqlins + "'" + pfile + "',"
				else:
					sqlins=sqlins + " NULL,"
				sqlins=sqlins + "'" + envType.upper() + "',"
				sqlins=sqlins + "'" + jobGroup + "',"
				sqlins=sqlins + "'" + sourceDb + "',"
				sqlins=sqlins + "'" + sourcedbType + "',"
				sqlins=sqlins + "'" + sourceSchema + "',"
				sqlins=sqlins + "'" + sourceTable + "',"
				sqlins=sqlins + "'" + targetSchema + "',"
				sqlins=sqlins + "'" + targetTable + "',"
				sqlins=sqlins + str(tCnt) + ","
				sqlins=sqlins + str(splitcount) + ","
				#sqlins=sqlins + "'" + currstat + "',sysdate,'" + retflg + "')"
				sqlins=sqlins + "'" + currstat + "',sysdate,'" + retflg + "','" + currOsUser + "','" + emailAdd + "','" + jobStream + "','" + currHostName + "','" + pgmVersion + "')"
				try:
					cursor1.execute(sqlins)
					cursor1.execute('commit')
				except Exception as e:
					logdata(errFile,"Exception occurred while inserting DIY master record",1)
					logdata(errFile,sqlins,0)
					logdata(errFile,str(e),1)
					Subject ="Error: Exception occurred while inserting DIY master record"
					errMsg=Subject + "\n\n" + str(e)
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
				## Insert a  new record into DIY_LINES table with INIT status
				insDIYlines(reqid,'NONE','INIT',cursor1,errFile,emailAdd)
			try:
				## Auto dereive/adjust MERGE_TYPE based on extract type. If extract type is ALL_DATA and merge type is not APPEND then switch merge type to OVERWRITE even if JCT says MERGE_ONE2ONE
				## Similarly if extract type is DATE/ID and merge type is not APPEND switch merge type to MERGE_ONE2ONE. For some reason if this rule needs to be overridden use -m verb
				logdata(logFile,"Checking whether extraction is History or incremental",0)
				sql4="SELECT distinct nvl(EXTRACT_TYPE,'ALL_DATA') from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and workflow_type='SRC2STG'"
				cursor1.execute(sql4)
				results=cursor1.fetchall()
				if len(results) > 0:
					for etypeObj in results:
						eType=etypeObj[0]
					if not merge_override:
						if eType == 'ALL_DATA' and len(whereClause) == 0:
							sql=f"select merge_type from edw_job_streams where job_stream_id_link='{jobStream}' and active_ind='Y'"
							cursor1.execute(sql)
							results2=cursor1.fetchall()
							for results2_obj in results2:
								mType=results2_obj[0]
							if mType != 'APPEND' and sourcedbType !='HANANRT':
								mType='OVERWRITE'
						else:
							sql=f"select merge_type from edw_job_streams where job_stream_id_link='{jobStream}' and active_ind='Y'"
							cursor1.execute(sql)
							results2=cursor1.fetchall()
							for results2_obj in results2:
								mType=results2_obj[0]
							if mType != 'APPEND':
								mType='MERGE_ONE2ONE'
							if len(results2) == 0:
								mType='MERGE_ONE2ONE'
				else:
					eType='ALL_DATA'
					if not merge_override:
						mType='OVERWRITE'
			except  Exception as e:
				colval="Exception occurred while checking job control extract type"
				updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				logdata(errFile,colval,1)
				#print ("Exception occurred while checking job control extract type")
				logdata(errFile,sql4,0)
				logdata(errFile,str(e),1)
				Subject ="Error: Exception occurred while checking job control extract type"
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
				#print(str(e))
			## If not retry then validate the RUN_STATUS column in EDW_JOB_STREAMS. If it is "P" or "A" then it is good to proceed with ingestion
			## If it is "C" probably it didnt got re-initialized. Hence it is needs to be initialized before attempting to process.
			if retryFlag !='TRUE':
				try:
					logdata(logFile,"Checking for pending JCT entry with job_stream_id - " + jobStream,0)
					sql3="SELECT count(*) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and RUN_STATUS in ('P','A') and workflow_type='SRC2STG' and active_ind='Y'"
					cursor1.execute(sql3)
					results=cursor1.fetchall()
					for cntObj in results:
						jobCnt=cntObj[0]
					if jobCnt > 0:
						#print("Job control entry - " + jobStream + " already exists on repository under running status.. Hence retrying the jobs now")
						logdata(logFile,"Job control entry - " + jobStream + " already exists on repository under Pending status.. Hence triggering ingestion now",1)
						existFlag='TRUE'
						ijobGroup=jobGroup
				except  Exception as e:
					#print ("Exception occurred while checking job control run status")
					logdata(errFile,"Exception occurred while checking job control pending status",1)
					logdata(errFile,sql3,0)
					#print(str(e))
					logdata(errFile,str(e),1)
					Subject ="Error: Exception occurred while checking job control pending status"
					errMsg=Subject + "\n\n" + str(e)
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
				try:
					if jobCnt == 0:
						logdata(logFile,"Checking if existing job stream - " + jobStream + " is in completed status",0)
						sql3="SELECT count(*) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and RUN_STATUS='C' and workflow_type='SRC2STG'"
						cursor1.execute(sql3)
						results=cursor1.fetchall()
						for resultsObj in results:
							jctCnt = resultsObj[0]
						if jctCnt > 0:
							#print("Job control entry - " + jobStream + " already exists on repository under Completed status.. Please re-initialize the job and retry")
							logdata(logFile,"Job control entry - " + jobStream + " already exists on repository under Completed status.. Please re-initialize the job and retry",1)
							continue
				except  Exception as e:
					logdata(errFile,"Exception occurred while checking job control completion status",1)
					logdata(errFile,sql3,0)
					#print ("Exception occurred while checking job control completion status")
					logdata(errFile,str(e),1)
					Subject ="Error:Exception occurred while checking job control completion status"
					errMsg=Subject + "\n\n" + str(e)
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
					#print(str(e))
			try:
				## Depending on source db platform establish connection to source database.
				logdata(logFile,f"Establishing connection to source db",0)
				if sourcedbType.upper() == 'ORACLE':
					conn2 = cx_Oracle.connect(srcdbParams['SOURCE_LOGIN'], srcPassword, srcdbParams['SOURCE_SERVICE_NAME'],encoding="UTF-16", nencoding="UTF-16")
				elif sourcedbType.upper() == 'TERADATA':
					conn2 = teradatasql.connect(host=srcdbParams['SOURCE_HOST'],user=srcdbParams['SOURCE_LOGIN'], password=srcPassword)
				elif sourcedbType.upper() in ('HANA','HANANRT'):
					hostName=srcdbParams['SOURCE_HOST'].split(';')[0].split(':')[0]
					if 'SOURCE_ADDITIONAL_PARAMS' in srcdbParams:
						conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword,srcdbParams['SOURCE_ADDITIONAL_PARAMS'])
					else:
						conn2 = open_hana_connection(hostName,srcdbParams['SOURCE_PORT'],srcdbParams['SOURCE_LOGIN'],srcPassword)
				elif sourcedbType.upper() == 'SNOWFLAKE':
					#conn2=snowflake.connector.connect(user=srcdbParams['SOURCE_LOGIN'],password=srcPassword,account=srcdbParams['ACCOUNT'],warehouse=srcdbParams['WAREHOUSE'])
					conn2=open_sf_connection(srcdbParams['ACCOUNT'],srcdbParams['SOURCE_LOGIN'],query_tag=sf_query_tag)
				cursor2=conn2.cursor()
			except Exception as e:
				colval="Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'] + "."
				updDIYmstr(reqid,'err_msg',colval  + str(e),cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				logdata(errFile,colval,1)
				#print ("Error occurred while connecting to source database")
				logdata(errFile,str(e),1)
				Subject ="Error: Error occurred while connecting to source database " + srcdbParams['SOURCE_HOST'] + "."
				errMsg=Subject + "\n\n" + str(e)
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			## Check for filter in EDW_JOB_STREAMS and if exists tag the load type as FILTERED* in DIY_MASTER table.
			if len(whereClause) > 0:
				ltype=f"Filtered_{eType}"
			else:
				ltype=eType
			## If source is Teradata then set the query band to pull TD insights on need basis.
			if sourcedbType.upper() == 'TERADATA':
				cursor2.execute(f"set query_band='Module=Main;LoadType={ltype};user={currOsUser};' for Session")
			## Derieve destination schema in Snowflake. If destination is not Snowflake same needs to be passed through -k verb
			if itype.lower() == 'jct' or itype.lower() == 'table':
				destSchema=find_dest_schema(jobStream,cursor1,logFile,errFile,envType,sourceDb,sourceSchema,sourceTable,sourcedbType.upper(),cursor2)
				if destSchema == -1:
					destSchema=None
			else:
				destSchema=determine_dest_schema(envType,sourceDb,sourceSchema,sourceTable,sourcedbType.upper(),cursor1,cursor2,logFile,errFile,target_schema_name)
				if destSchema == -1:
					destSchema=None
			if destSchema is None:
				colval="Unable to determine destination schema.. Hence not proceeding with ingestion.."
				updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
				logdata(errFile,colval,1)
				#print ("Error occurred while connecting to source database")
				#logdata(errFile,str(e),1)
				Subject =colval
				errMsg=colval
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				continue
			else:
				logdata(logFile,f"For jct {jobStream} determined SF schema as {destSchema}",0)
			## Validate if passed source object is View
			rc=validate_view(envType,sourceDb,sourcedbType,cursor2,sourceSchema,sourceTable,emailAdd,logFile,errFile,cursor1)
			if rc == -1:
				continue
			else:
				objArray = rc.split("~")
				srcBaseschema = objArray[0]
				srcBasetable = objArray[1]
				logdata(logFile,"Found base schema as " + srcBaseschema + " and base table as " + srcBasetable,0)
			if excepView:
				if sCmn is None:
					logdata(logFile,"Since source is a Oracle view and approved through exception can do ingestion only with 1 thread and split",0)
					splitcount=1
					threadoverride=True
					tCnt=1
				else:
					logdata(logFile,f"Since source is a Oracle view and approved and override column provided, proceeeding with split and load using column {sCmn}",0)
			if retryFlag == 'FALSE':
				updDIYmstr(reqid,'load_type',ltype,cursor1,errFile,logFile,emailAdd)
				## If requested load type is not full load then query the incremental column from JOB_STREAMS table.
				if eType != 'ALL_DATA':
					logdata(logFile,"Checking incremental column",0)
					try:
						sql5="SELECT distinct INCREMENTAL_COLUMN_NAME from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and workflow_type='SRC2STG'"
						cursor1.execute(sql5)
						results=cursor1.fetchall()
						for incColObj in results:
							incCol=incColObj[0]
						logdata(logFile,"Identified incremental column as - " + incCol,0)
					except  Exception as e:
						colval="Exception occurred while checking job control incremental column." 
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						logdata(errFile,sql5,0)
						#print ("Exception occurred while checking job control incremental column")
						logdata(errFile,str(e),1)
						Subject ="Error: Exception occurred while checking job control incremental column."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
						#print(str(e))
				## Ingestion logic Oracle source tables.
				if sourcedbType.upper() == 'ORACLE':
					logdata(logFile,"Invoking data ingestion engine for Oracle db",0)
					viewsplitflag=False
					if (tblCntvar == 0 and itype.lower() == 'table') or itype.lower() == 'db' or itype.lower() == 'jct':
						tblCntvar=1
						## Validate existence of metadata and if not collect the same.
						metRc=validateMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if metRc == -1:
							colval="Not able to verify existence of metadata for table - " + srcBasetable + ". Hence aborting ingetsion for this table.."
							updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							Subject ="Error: Not able to verify existence of metadata for table - " + srcBasetable + ". Hence aborting ingetsion for this table.."
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						else:
							updDIYmstr(reqid,'current_phase','Metadata_validation_Succesful',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'INIT','Metadata_validation_Succesful',cursor1,errFile,emailAdd)
							logdata(logFile,"Found - " + str(metRc) + " columns for table - " + srcBasetable + " from metadata table",0)
					
						try:
							## Check if table is partitioned 
							logdata(logFile,"Checking if passed table is partitioned",0)
							sql21="select count(1) from all_tab_partitions where table_name='" + srcBasetable + "' and table_owner='" + srcBaseschema + "'"
							cursor2.execute(sql21)
							results=cursor2.fetchall()
							for partcheckobj in results:
								partCnt=partcheckobj[0]
							if int(partCnt) == 0:
								logdata(logFile,"Source Oracle object - " + srcBaseschema + "." + srcBasetable + " is not partitioned",0)
							else:
								logdata(logFile,"Source Oracle object - " + srcBaseschema + "." + srcBasetable + " partitioned and has " + str(partCnt) + " partitions",0)
						except Exception as e:
							logdata(errFile,"Error occurred while checking table partitions",1)
							logdata(errFile,sql21,0)
							logdata(errFile,str(e),1)
							Subject ="Error: Error occurred while checking table partitions"
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue

						if eType == 'ALL_DATA' or len(eType) == 0:
							logdata(logFile,"Invoking a history load",0)
							updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
							if len(whereClause) == 0:
								try:
									logdata(logFile,"Since this is Full load with no filters checking space availability before triggering ingestion",0)
									if partCnt == 0:
										sql16="select sum(bytes)/1024 from dba_segments where owner='" + srcBaseschema + "' and segment_name='" + srcBasetable + "' and segment_type like 'TABLE'" 
									else:
										sql16="select sum(bytes)/1024 from dba_segments where owner='" + srcBaseschema + "' and segment_name='" + srcBasetable + "' and segment_type like 'TABLE%'"
									cursor2.execute(sql16)
									results=cursor2.fetchall()
									for tblsizeObj in results:
											tblSize=tblsizeObj[0]
									if len(results) == 0 or tblSize is None:
										logdata(logFile,"Could not Calculate space required for " + srcBaseschema + "." + srcBasetable + ". Hence ignoring space validation and proceeding further",1)
									else:
										osFreespacecmd="df " + stageMnt +" | tail -1 | awk '{print $4}'"
										freeSpace=int(os.popen(osFreespacecmd).read())
										spaceCheck=True
										#if freeSpace < tblSize*2:
										#	logdata(errFile,"Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " table.. Hence aborting ingestion",1)
										#	continue
										updDIYmstr(reqid,'tbl_size',str(tblSize),cursor1,errFile,logFile,emailAdd)
								except Exception as e:
									logdata(logFile,"Issue occurred while pulling mount free space.. Ignoring this check and continuing..",0)
									logdata(logFile,str(e),1)
								
								try:
									## Check source table count. If source count is > 1 M by default full load isnt permitted. Exceptions to such case needs to be logged in EDW_LARGE_TBL_INGESTION table.
									if itype.lower()=='jct':
										logdata(logFile,"Querying count from source for audit purpose",0)
										#sql5="select /*+ parallel (16) */  count(*) from " + sourceSchema + "." + sourceTable + " a"
										sql5=f'select /*+ parallel (16) */  count(*) from {sourceSchema}."{sourceTable}" a'
										cursor2.execute(sql5)
										results=cursor2.fetchall()
										for srcCntobj in results:
											srcNumrows=srcCntobj[0]
											statCount=1
											srclastAnal=None
										logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows pulled by running count",0)
										if srcNumrows > 1000000 and eType == 'ALL_DATA' and mType != 'APPEND': 
											sql=f"select count(1) cnt from diy_master where source_table='{sourceTable}' and source_schema='{sourceSchema}' and attribute1 in ('root','tidaladm')"
											cnt_obj=execute_df_qry(conn,sql,reqid,conn)
											jct_diy_cnt=cnt_obj["cnt"][0]
											## If it is a recurring ingestion from scheduler validate if it is exceptionally approved. If not abort the process.
											if currOsUser in ['tidaladm','root','edwadm']:
												rc=validate_large_table_ingestion(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile)
												if rc == -1:
													raise Exception(f"Source table  {sourceDb}.{sourceSchema}.{sourceTable} has count of {srcNumrows} and not in exception list.")
											else:
												if jct_diy_cnt == 0:
													if incColname is None or len(incColname) == 0:
														logdata(logFile,f"Source table  {sourceDb}.{sourceSchema}.{sourceTable} has count of {srcNumrows}, but misssing inc column. Hence JCT will be disabled post ingestion",1)
														disableJct=True
														ext_type='LARGE_TBL_WO_INC_COL'
													else:
														## If source oracle table has more than 10 Million rows , check for existence of index on incremental column. If not disable the same post history load.
														if sourcedbType.upper() == 'ORACLE' and srcNumrows > 10000000:
															rc=validate_incremental_column_index(envType,sourceDb,sourceSchema,sourceTable,cursor2,logFile,errFile,incColname)
															if rc == -1:
																logdata(logFile,f"Source table  {sourceDb}.{sourceSchema}.{sourceTable} has count of {srcNumrows}, but incremental column {incColname} is missing index. Hence JCT will be disable post ingestion",1)
																disableJct=True
																ext_type='NO_INC_COL_INDEX'
									else:
										## If it is a new ingestion, check the source stats age. If it it not very old fetch approximate row count based on source table stats.
										logdata(logFile,"Querying metadata for source table rowcount based on stats",0)
										sql5="SELECT num_rows as maxrowcount,round((sysdate-last_analyzed)) elapsed_days from all_tables where owner='" + srcBaseschema + "' and table_name='" + srcBasetable + "'"
										cursor2.execute(sql5)
										results=cursor2.fetchall()
										statCount=len(results)
										if (statCount != 0):
											for srcanalobj in results:
												srclastAnal=srcanalobj[1]
												srcNumrows=srcanalobj[0]
												logdata(logFile,"Source table has " + str(srcNumrows) + " rows based on stats which is " + str(srclastAnal) + " days old",0)
									if (statCount == 0 or srcNumrows is None) :
										#print ("Source table - " + sourceSchema + "." + sourceTable + " stats could not be located on source database - " + sourceDb + ".")
										logdata(logFile,"Since source table - " + srcBaseschema + "." + srcBasetable + " stats could not be located on source database - " + sourceDb + " proceeding with row count.",1)
										try:
											## If source is view then ingestion by default will be done using single thread. Later if we can find a table which has selective columns then it can be loaded using splits
											if sourceType =='VIEW':
												srcNumrows=1
												splitcount=1
												threadoverride=True
											else:
												sql6="select /*+ parallel (16) */  count(*) from " + sourceSchema + "." + sourceTable + " a"
												cursor2.execute(sql6)
												results=cursor2.fetchall()
												for srcCntobj in results:
													srcNumrows=srcCntobj[0]
												logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows pulled by running count",0)
										except Exception as e:
											colval="Error occurred while collecting source table row count."
											updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
											updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
											logdata(errFile,"Error occurred while collecting source table row count1",1)
											logdata(errFile,sql6,0)
											#print ("Error occurred while collecting source table row count1")
											logdata(errFile,str(e),1)
											Subject ="Error: Error occurred while collecting source table row count."
											errMsg=Subject + "\n\n" + str(e)
											sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
											continue
											#print(str(e))
								except Exception as e:
									colval="Error occurred while collecting source table row count."
									updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,"Error occurred while collecting source table stats",1)
									logdata(errFile,sql6,0)
									#print ("Error occurred while collecting source table stats")
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while collecting source table row count."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
									#print(str(e))
								try:
									if (statCount > 0 and srclastAnal is not None ):
										if srclastAnal > Anal_Threshold:
											logdata(logFile,"Since table stats are not upto date pulling table count from table",0)
											sql6="select /*+ parallel(8) */ count(*) from " + sourceSchema + "." + sourceTable
											cursor2.execute(sql6)
											results=cursor2.fetchall()
											for srcCntobj in results:
												srcNumrows=srcCntobj[0]
											logdata(logFile,"Table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows",0)
								except Exception as e:
									#print ("Error occurred while collecting source table row count2")
									colval="Error occurred while collecting source table row count."
									updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,"Error occurred while collecting source table row count2",1)
									logdata(errFile,sql6,0)
									#print(str(e))
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while collecting source table row count."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
								if (srcNumrows == 0 and statCount >0 and srclastAnal is not None):
									if srclastAnal <= Anal_Threshold:
										logdata(logFile,"Since source table had 0 rows based on stats revalidating count",0)
										sql6=f"select count(*) from {sourceSchema}.{sourceTable}"
										cursor2.execute(sql6)
										results=cursor2.fetchall()
										for srcCntobj in results:
											srcNumrows=srcCntobj[0]
									logdata(logFile,"Table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows pulled from table",0)
							else:
								## If source JCT has filters then stats cannot be used and so running querying is only option to pull the eligible record count.
								logdata(logFile,"Since source table have filters enabled count directly from table",0)
								try:
									sql6="select /*+ parallel (16) */  count(*) from " + sourceSchema + "." + sourceTable + " a where " + whereClause
									cursor2.execute(sql6)
									results=cursor2.fetchall()
									for srcCntobj in results:
										srcNumrows=srcCntobj[0]
									logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows",0)
								except Exception as e:
									colval="Error occurred while collecting source table row count."
									updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,"Error occurred while collecting source table row count3",1)
									logdata(errFile,sql6,0)
									#print ("Error occurred while collecting source table row count3")
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while collecting source table row count."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									#print(str(e))
									continue
							aud_num_rows=srcNumrows
						else:
							## If the load is incremental, then eligible record count needs to be pulled by applying filter on incremental column.
							## If filter exist in JCT same will be applied on top of incremental column filter to pull the eligible record count.
							logdata(logFile,"Pulling incremental column data",0)
							updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
							try:
								logdata(logFile,"Checking for table partition enablement",0)
								part_name=query_partition(cursor1,cursor2,sourceDb,sourceSchema,sourceTable,logFile,errFile)
								sql8 = "SELECT distinct decode(EXTRACT_TYPE,'DATE',to_char(to_extract_dtm,'MM/DD/YYYY HH24:MI:SS'),'ID',to_char(to_extract_id)) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and workflow_type='SRC2STG'"
								cursor1.execute(sql8)
								results = cursor1.fetchall()
								for lastextobj in results:
									lastextVal=lastextobj[0]
								if incColname is None:
									sql9=''
									raise Exception("Missing incremental column for JCT - " + jobStream + ". Please check and fix..")
								if lastextVal is None:
									sql9=''
									raise Exception("Missing incremental column value for JCT - " + jobStream + ". Please check and fix..")
								if part_name == -1:
									src_tbl_2_query=f"{sourceSchema}.{sourceTable}"
								else:
									src_tbl_2_query=f"{sourceSchema}.{sourceTable} PARTITION({part_name})"
								logdata(logFile,f"Now querying {src_tbl_2_query} to pull source count",0)
								if eType == 'DATE':
									logdata(logFile,"Pulling count from source based on last extract date - " + str(lastextVal),0)
									sql9 ="SELECT /*+ parallel (16) */  count(*) from " + src_tbl_2_query + " a where " + incColname + " > to_date('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"
								elif eType == 'ID':
									logdata(logFile,"Pulling count from source table based on last extract id - " + str(lastextVal),0)
									sql9 ="SELECT /*+ parallel (16) */  count(*) from " + src_tbl_2_query + " a where " + incColname + " > " + str(lastextVal) 
								else:
									#print("Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry")
									colval="Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
									updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,colval,1)
									Subject ="Error: Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
									errMsg=Subject
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
								if len(whereClause) > 0:
									logdata(logFile,"Filter existing.. Applying the same before pulling rowcount",0)
									sql9 = sql9 + " AND " + whereClause
								cursor2.execute(sql9)
								results=cursor2.fetchall()
								for srcCntobj in results:
									srcNumrows=srcCntobj[0]
								logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows for ingestion",0)
							except Exception as e:
								#print ("Error occurred while collecting source table row count4")
								colval="Error occurred while collecting source table row count."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row count4",1)
								if sql9 is not None:
									logdata(errFile,sql9,0)
								#print(str(e))
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						#print("Source table count is - " + str(srcNumrows))
						updDIYmstr(reqid,'src_rows',str(srcNumrows),cursor1,errFile,logFile,emailAdd)
						## If no eligible records found for ingestion, just mark both SRC2STG and STG2BR jct's are completed and launch the soft delete process alone if enabled.
						if srcNumrows == 0:
							zeroRecIngestion(jobStream,reqid,cursor1,errFile,logFile,emailAdd,envType,LOGS_DIR,cursor3,sourcedbType,ltype)
							continue
						if itype.lower()=='jct':
							try:
								## If source count is not pulled in earlier process and soft delete is enabled we need to pull the total source record count for audit purpose.
								logdata(logFile,"Pulling source count for audit purpose",0)
								sql=f"select source_deleted_flag from edw_job_streams where job_stream_id='{jobStream}'"
								cursor1.execute(sql)
								results=cursor1.fetchall()
								for results_obj in results:
									del_flag=results_obj[0]
								if del_flag == 'A' and aud_num_rows == 0:
									sql=f"SELECT /*+ parallel (16) */  count(*) from {sourceSchema}.{sourceTable} a "
									if len(whereClause) > 0:
										if " where" in whereClause:
											sql += f" and  {whereClause}"
										else:
											sql += f" where {whereClause}"
									cursor2.execute(sql)
									results=cursor2.fetchall()
									for srcCntobj in results:
										aud_num_rows=srcCntobj[0]
							except Exception as e:
								colval="Error occurred while collecting source table row count for audit."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row for audit",1)
								if sql is not None:
									logdata(errFile,sql,0)
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count for audit."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue


						
						## Check if eligible record count is greater than split threshold so that framework will attempt to split and load the data
						if srcNumrows >= Split_Orcl_Threshold and excepView and sCmn is None and sCnt is None:
							logdata(logFile,"Checking even though source is view if it can be splitted and loaded",0)
							## If earlier a succesful split has been attempted try to re-use the same column to perform subsequent split
							sql=f"select nvl(max(reqid),0) reqid from diy_master where attribute3='{jobStream}' "
							sql+=f" and current_phase='Success' and to_number(replace(attribute5,'u')) > 8.1 and split_count > 1 and thread_count > 1"
							diy_reqid_obj=execute_df_qry(conn, sql, reqid, conn)
							diy_reqid=diy_reqid_obj["reqid"][0]
							repo_split_col_a=''
							if diy_reqid != 0:
								sql=f"select split_column sc from diy_master where reqid={diy_reqid}"
								split_col_obj=execute_df_qry(conn, sql, reqid, conn)
								repo_split_col_a=split_col_obj["sc"][0].split('~')[0]
							if len(repo_split_col_a) > 0:
								sCmn=repo_split_col_a
							else:
								## If requested object is view then check for tables which forms the view and check if any highly selective table can be leveraged to perform split
								sql=f"select owner,table_name from ("
								sql+=f"select referenced_owner owner,referenced_name table_name from "
								sql+=f" (select * from  all_dependencies a,all_tables b where name='{sourceTable}' and referenced_type='TABLE'"
								sql+=f" and a.owner='{sourceSchema}' and a.referenced_owner=b.owner "
								sql+=f" and dependency_type='HARD' and a.referenced_name=b.table_name order by num_rows desc)"
								sql+=f" union all "
								sql+=f" select owner owner,table_name from  ("
								sql+=f" select b.owner,b.table_name from  all_dependencies a,all_tables b,all_synonyms c "
								sql+=f" where name='{sourceTable}' and referenced_type in ('SYNONYM') and a.owner='{sourceSchema}'"
								sql+=f" and a.referenced_owner=c.owner  and dependency_type='HARD' and a.referenced_name=c.synonym_name"
								sql+=f" and c.table_name=b.table_name and c.table_owner=b.owner "
								sql+=f" order by num_rows desc))"
								sql+=f" where rownum=1"
								viewobj=execute_df_qry(conn2,sql,reqid,conn)
								if len(viewobj) > 0:
									view_tbl_owner=viewobj["owner"][0]
									view_tbl_name=viewobj["table_name"][0]
									logdata(logFile,f"Checking if column can be found on table {view_tbl_owner}.{view_tbl_name} for split",0)
									sql=f"select column_name cn from "
									sql+=f" (select a.column_name from all_tab_columns a,all_tab_columns b where a.table_name='{view_tbl_name}'  and a.owner='{view_tbl_owner}' and "   
									sql+=f" a.data_type in ('DATE','NUMBER','VARCHAR2') and (a.nullable='N' or a.num_nulls=0)  "
									sql+=f" and b.table_name='{sourceTable}' and b.owner='{sourceSchema}' and a.column_name=b.column_name order by a.num_distinct desc) "
									sql+=f" where rownum=1"
									splitcolobj=execute_df_qry(conn2,sql,reqid,conn)
									if len(splitcolobj) > 0:
										sCmn=splitcolobj["cn"][0]
									else:
										logdata(logFile,f"Could not find appropriate column for splitting view {sourceTable}. Hence proceeding with 1 thread",0)
								else:
									logdata(logFile,f"Could not find corresponding table for splitting view {sourceTable}. Hence proceeding with 1 thread",0)
							if sCmn is not None:
								## If split column is passed through over-ride use the same 
								logdata(logFile,f"Checking if view can be splitted using column {sCmn}",0)
								filter=''
								if eType == 'ALL_DATA' or len(eType) == 0:
									if len(whereClause) > 0:
										filter=whereClause
								else:
									if eType == 'DATE':
										filter += f" {incColname} > to_date('{lastextVal}','MM/DD/YYYY HH24:MI:SS')"
									elif eType == 'ID':
										filter += f"{incColname} > {lastextVal}"
									if len(whereClause) > 0:
										filter += f" and {whereClause}"
								viewsplitflag=True
								## Derive at split count based on eligible row count. This value can be over-ridden through -s verb
								if srcNumrows >= Split_Threshold and srcNumrows < (Split_Threshold*5):
									splitcount=2
								elif srcNumrows >= (Split_Threshold*5) and srcNumrows < (Split_Threshold*10):
									splitcount=3
								elif srcNumrows >= (Split_Threshold*10) and srcNumrows < (Split_Threshold*50):
									splitcount=5
								else:
									splitcount=10
								## Set current phase as split phase
								updDIYmstr(reqid,'current_phase','Split_Phase',cursor1,errFile,logFile,emailAdd)
								insDIYlines(reqid,'Invoking_Data_Load','Split_Phase',cursor1,errFile,emailAdd)
								logdata(logFile,"Going with " + str(splitcount) + " parallel threads",0)
								## Invoke split module to perform the split and arrive at range partitions
								splitQrydict=diy_split(conn2,srcBaseschema,srcBasetable,splitcount,reqid,conn,filter,srcNumrows,eType,sCmn,sourcedbType.upper(),sourceDb,'SRC2STG')
								if type(splitQrydict) != dict:
									## If split fails for some reason then proceed with single thread
									logdata(logFile,f"Could not split view {sourceTable} because of exception {splitQrydict}. Hence proceeding with 1 thread",0)
								
						if not threadoverride:
							#if lobcolCnt == 0:
							if srcNumrows >= Split_Orcl_Threshold:
								logdata(logFile,"Qualified rows demands divide and conquer type ingestion",0)
								if srcNumrows >= Split_Threshold and srcNumrows < (Split_Threshold*5):
									splitcount=2
								elif srcNumrows >= (Split_Threshold*5) and srcNumrows < (Split_Threshold*10):
									splitcount=3
								elif srcNumrows >= (Split_Threshold*10) and srcNumrows < (Split_Threshold*50):
									splitcount=5
								elif srcNumrows >= (Split_Threshold*50) and srcNumrows < (Split_Threshold*200):
									splitcount=10
								elif srcNumrows >= (Split_Threshold*200) and srcNumrows < (Split_Threshold*600):
									splitcount=20
								elif srcNumrows >= (Split_Threshold*600):
									splitcount=30
								#updDIYmstr(reqid,'Thread_count',str(splitcount),cursor1,errFile,logFile,emailAdd)
							#else:
							#	if srcNumrows > Split_Lob_Threshold:
							#		splitcount=round(srcNumrows/Split_Lob_Threshold)
							#		Thread_count=Thread_Lob_Count
							#	else:
							#		splitcount=1
						#if tCnt is not None:
							#logdata(logFile,"Since thread count override is specified using " + tCnt + " threads for ingestion",0)
							#threadcount=tCnt
							## New logic to check the mount space
						if not viewsplitflag:
							if splitcount > 1:
								updDIYmstr(reqid,'current_phase','Split_Phase',cursor1,errFile,logFile,emailAdd)
								insDIYlines(reqid,'Invoking_Data_Load','Split_Phase',cursor1,errFile,emailAdd)
								logdata(logFile,"Going with " + str(splitcount) + " parallel threads",0)
								indName=''
								splitQrydict={}
								try:
									filter=''
									if eType == 'ALL_DATA' or len(eType) == 0:
										if len(whereClause) > 0:
											filter=whereClause
									else:
										if eType == 'DATE':
											filter += f" {incColname} > to_date('{lastextVal}','MM/DD/YYYY HH24:MI:SS')"
										elif eType == 'ID':
											filter += f"{incColname} > {lastextVal}"
										if len(whereClause) > 0:
											filter += f" and {whereClause}"
									if sCmn is None:
										splitQrydict=diy_split(conn2,srcBaseschema,srcBasetable,splitcount,reqid,conn,filter,srcNumrows,eType,None,sourcedbType.upper(),sourceDb,'SRC2STG')
										if type(splitQrydict) != dict:
											logdata(logFile,f"Attempting to split resulted in {splitQrydict}. Hence checking if it can be ingested in single thread",1)
											if tblSize is not None and tblSize < 262144000:
												splitcount=1
												threadoverride=True
												splitQrydict={}
											else:
												logdata(errFile,"Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually",1)
												Subject ="Error:Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually"
												errMsg=Subject
												sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
												continue
									else:
										## If a split column is passed while invoking framework same will be used to split
										logdata(logFile,"Using the over-ride column for parallelizing extraction",0)
										srcIndcol=sCmn
										cursor2.execute("select data_type from all_tab_columns where table_name='" + srcBasetable + "' and owner='" + srcBaseschema + "' and column_name='" + sCmn + "'")
										results=cursor2.fetchall()
										if len(results) == 0:
											colval="Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
											updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
											updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
											logdata(errFile,colval,1)
											Subject ="Error: Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
											errMsg=Subject
											sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
											continue
										else:
											splitQrydict=diy_split(conn2,srcBaseschema,srcBasetable,splitcount,reqid,conn,filter,srcNumrows,eType,sCmn,sourcedbType.upper(),sourceDb,'SRC2STG')
											if type(splitQrydict) != dict:
												logdata(logFile,f"Attempting to split resulted in {splitQrydict}. Hence checking if it can be ingested in single thread",1)
												## If table size is less than 250 GB then framework will attempt to load using single thread. If process will be aborted.
												## This 250GB should be moved to config in future to avoid hard coding.
												if tblSize is not None and tblSize < 262144000:
													splitcount=1
													threadoverride=True
													splitQrydict={}
												else:
													logdata(errFile,"Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually",1)
													Subject ="Error:Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually"
													errMsg=Subject
													sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
													continue
									logdata(logFile,"Completed pulling the range partitions for data pull",0)
								except Exception as e:
									colval="Error occurred while gathering columns for source table."
									updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									#print ("Error occurred while gathering columns for source table")
									logdata(errFile,colval,1)
									#print(str(e))
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while gathering columns for source table."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
								## End of indentation for multi thread based ingestion
							else:
								splitQrydict={}
								#splitQrydict[0]="select * from " + sourceSchema + "." + sourceTable 
								if len(whereClause) > 0:
									splitQrydict[0] = whereClause
								else:
									splitQrydict[0]=""
					insQryDict=[]
					jobStreamarr=[]
					#jobStreammstr=jobStream + "_" + str(ftime)
					jobStreammstr=jobStream
					if not threadoverride:
						if srcNumrows >= Split_Orcl_Threshold:
							if len(splitQrydict) <2:
								colval="Table identified as large, but bug detected while deriving splitting logic.. Hence giving up now.."
								updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								Subject ="Error: Table identified as large, but bug detected while deriving splitting logic.. Hence giving up now.."
								errMsg=Subject
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
					## if identified process is split and load then master job stream will have run_status set to "A" and new child JCT's will be created which will have run_status as "P"
					## If same table has undergone split in past, then same child JCT's will be re-used by enabling respecive active_ind to "Y"
					## At end of load all child JCT's will be disabled by setting active_Ind to "N"
					if len(splitQrydict) > 1:
						jobStreammstr=jobStream + "_" + str(ftime)
						jobstreamid=jobStream
						compCode='A'
					else:
						jobStreammstr=jobStream
						jobstreamid=jobStream
						jobStreamarr.append(jobStreammstr)
						compCode='P'
					if len(incColname) > 0:
						source_del_flag='A'
					else:
						source_del_flag='N'
					if existFlag == 'FALSE':
						logdata(logFile,"Since this is new history ingestion forcing metadata collection from source",1)
						## Force collect metadata if this is history ingestion and being done for very first time.
						rc=collectMetadata(envType,sourceDb,sourceSchema,sourceTable,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if rc != 0:
							raise Exception("Issue encountered while collecting metadata. Please check logfile")
						#whereClauseIns=whereClause.replace("'","''").replace("''''","''") ## already single quote changed to double. So if two double quote change to one pair of double quote
						whereClauseIns=whereClause.replace("'","''")
						## Master record gets inserted into EDW_JOB_STREAMS if doesnt exist. Also if it is single load entry this will be only JCT insertion.
						InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
						InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
						InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
						InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by,source_deleted_flag)"
						InsQry += " values "
						InsQry += "('" + jobstreamid + "','" + jobstreamid + "','" + ijobGroup + "',1,'Y','" + compCode + "','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
						InsQry += "','" + sourceTable + "','" + whereClauseIns + "','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
						InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable  + "','" + sfWh + "','" + sfRole + "',NULL"
						InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API','" + source_del_flag + "')"
						#print (InsQry)
						logdata(logFile,"Master record for insertion",0)
						#logdata(logFile,InsQry,0)
						insQryDict.append(InsQry)
					## If JCT exists already then simply update existing JCT's run status
					if existFlag == 'TRUE':
						if len(splitQrydict) > 1:
							logdata(logFile,"Identified incremental ingestion as large batch.. Hence updating run_status for master record",0)
							try:
								UpdQry = "Update EDW_JOB_STREAMS set run_status = '" + compCode + "' where job_stream_id='" + jobstreamid + "' and workflow_type='SRC2STG' and source_db_name='"
								UpdQry = UpdQry + sourceDb + "' and source_schema='" + sourceSchema + "' and source_table_name='" + sourceTable + "'"
								cursor1.execute(UpdQry)
								cursor1.execute('commit')
							except Exception as e:
								colval="Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								logdata(errFile,InsQry,0)
								logdata(errFile,str(e),1)
								Subject ="Error: Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						else:
							cursor1.execute("update EDW_JOB_STREAMS set job_group_id='" + ijobGroup + "' where job_stream_id='" + jobstreamid + "'")
							cursor1.execute("commit")
					## If it is split and load obtain the range partition and run through loop to have one record inserted/updated for each partition
					## Partition key will be passed as filter through WHERE_CLAUSE in EDW_JOB_STREAMS and so one JCT per child will be activated.
					if len(splitQrydict) > 1:
						split4upd=len(splitQrydict)
						logdata(logFile,"Child records for insertion",0)
						for key,val in splitQrydict.items():
							value=val.replace("|~|","'")
							if len(whereClause) > 0:
								sql=f"select distinct split_column sc from diy_splits where reqid={reqid}"
								splitcol_obj=execute_df_qry(conn,sql,reqid,conn)
								srcIndcol=splitcol_obj["sc"][0]
								#if srcIndcol + " " in whereClause:
								#	whereClausenew = value
								#else:
								whereClausenew = whereClause + ' and ' + value
							else:
								whereClausenew = value
							## Change 4.13
							if len(incColname) > 0:
								if eType == 'DATE':
									whereClausenew = whereClausenew + ' AND ' + incColname + " > to_date(''" + lastextVal + "'',''MM/DD/YYYY HH24:MI:SS'')"  ## check to_date syntax
								elif  eType == 'ID':
									whereClausenew = whereClausenew + ' AND ' + incColname + " > " + str(lastextVal) 
							#jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
							#whereClauseIns=whereClausenew.replace("'","''").replace("''''","''")
							whereClauseIns=whereClausenew.replace("'","''")
							InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
							InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
							InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
							InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by)"
							InsQry += " values "
							InsQry += "('" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + ijobGroup + "','" + str(key)  
							InsQry += "','Y','P','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
							InsQry += "','" + sourceTable + "',' " + whereClauseIns + " ','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
							InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable + "_" + str(key) + "','" + sfWh + "','" + sfRole + "',NULL"
							InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API')"
							try:
								cursor1.execute("select * from (select job_stream_id from EDW_JOB_STREAMS where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG' order by create_date) where rownum=1")
								results=cursor1.fetchall()
								if len(results) == 0:
									insQryDict.append(InsQry)
									jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
								else:
									for existObj in results:
										jctExist=existObj[0]
									updQry = "Update EDW_JOB_STREAMS set active_ind='Y',run_status='P',where_clause='" + whereClauseIns + "',modify_date=sysdate"
									updQry = updQry + ",SOURCE_DB_NAME='" + sourceDb + "',SOURCE_SCHEMA='" + sourceSchema + "',target_db_name='" + targetDb + "',target_schema='" + targetSchema + "',sf_warehouse='" + sfWh + "',sf_role='" + sfRole + "',extract_type='" + eType + "'"
									updQry = updQry + ",source_db_connection='" + connectionName + "',source_table_name='" + sourceTable + "',target_table_name='" + targetTable + "_" + str(key) + "',modified_by='" + currOsUser + "',incremental_column_Name='" + incColname + "'"
									updQry = updQry + ",job_group_id='" + ijobGroup + "' where job_stream_id like '" +  jctExist + "' and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG'"
									insQryDict.append(updQry)
									jobStreamarr.append(jctExist)
							except Exception as e:
								logdata(logFile,"Issue encountered while checking existing JCT child process",1)
								logdata(errFile,str(e),1)
								insQryDict.append(InsQry)
							#print (InsQry)
							#logdata(logFile,InsQry,0)
							#insQryDict.append(InsQry)
					## End of Indentation for Oracle type Ingestion
				## Most of the logic stated above remains the same for other source platforms as well. Only queries will vary as each source platform will have different metadata tables.
				## Logic and process flow remains the same.
				elif sourcedbType.upper() == 'TERADATA':
					logdata(logFile,"Invoking data ingestion engine for Teradata db",0)
					vt_flag=False
					if (tblCntvar == 0 and itype.lower() == 'table') or itype.lower() == 'db'  or itype.lower() == 'jct':
						tblCntvar=1
						metRc=validateMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if metRc == -1:
							colval="Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
							updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							Subject ="Error: Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						else:
							updDIYmstr(reqid,'current_phase','Metadata_validation_Succesful',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'INIT','Metadata_validation_Succesful',cursor1,errFile,emailAdd)
							logdata(logFile,"Found - " + str(metRc) + " columns for table - " + sourceTable + " from metadata table",0)
						if eType == 'ALL_DATA' or len(eType) == 0:
							logdata(logFile,"Invoking a history load",0)
							updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
							if len(whereClause) == 0:
								try:
									logdata(logFile,"Since this is Full load with no filters checking space availability before triggering ingestion",0)
									sql16="LOCK ROW FOR ACCESS select sum(currentperm)/1024 from dbc.tablesizev where databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "'" 
									cursor2.execute(sql16)
									results=cursor2.fetchall()
									if len(results) == 0:
										logdata(errFile,"Could not locate base table " + srcBaseschema + "." + srcBasetable + ". Hence aborting ingestion for this stream",1)
										Subject ="Error: Could not locate base table " + srcBaseschema + "." + srcBasetable + ". Hence aborting ingestion for this stream"
										errMsg=Subject
										sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
										continue
									else:
										for tblsizeObj in results:
											tblSize=tblsizeObj[0]
										osFreespacecmd="df " + stageMnt +" | tail -1 | awk '{print $4}'"
										freeSpace=int(os.popen(osFreespacecmd).read())
										spaceCheck=True
										#if freeSpace < tblSize*2:
										#	logdata(errFile,"Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " table.. Hence aborting ingestion",1)
										#	continue
									updDIYmstr(reqid,'tbl_size',str(tblSize),cursor1,errFile,logFile,emailAdd)
								except Exception as e:
									logdata(errFile,"Issue occurred while checking mount free space.. Ignoring this check and continuing..",0)
									logdata(errFile,str(e),1)
								try:
									if itype.lower()=='jct':
										logdata(logFile,"Querying count from source for audit purpose",0)
										sql5="LOCK ROW FOR ACCESS select   cast( count(*) as bigint) from " + sourceSchema + "." + sourceTable + " a"
										cursor2.execute(sql5)
										results=cursor2.fetchall()
										for srcCntobj in results:
											srcNumrows=srcCntobj[0]
											statCount=1
											srclastAnal=Anal_Threshold
										logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows pulled by running count",0)
									else:
										logdata(logFile,"Querying metadata for source table rowcount based on stats",0)
										sql5 = "LOCK ROW FOR ACCESS  SELECT	rowcount, extract (day from ((current_timestamp - (lastcollecttimestamp)) day(4) TO SECOND(6)))  (NAMED  elapsed_days) elapsed_days"
										sql5 = sql5 + " from	dbc.statsv where	databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "'"
										sql5 = sql5 + " qualify row_number() over ( partition by databasename, tablename order by lastcollecttimestamp desc ) = 1"
										cursor2.execute(sql5)
										results=cursor2.fetchall()
										statCount=len(results)
										if (statCount == 0):
											#print ("Source table - " + sourceSchema + "." + sourceTable + " stats could not be located on source database - " + sourceDb + ".")
											logdata(logFile,"Since source table - " + sourceSchema + "." + sourceTable + " stats could not be located on source database - " + sourceDb + " proceeding with row count.",1)
											try:
												sql6="LOCK ROW FOR ACCESS select count(*) from " + sourceSchema + "." + sourceTable
												cursor2.execute(sql6)
												results=cursor2.fetchall()
												for srcCntobj in results:
													srcNumrows=srcCntobj[0]
												logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows pulled by running count",0)
											except Exception as e:
												colval="Error occurred while collecting source table row count."
												updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
												updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
												logdata(errFile,"Error occurred while collecting source table row count1",1)
												logdata(errFile,sql5,0)
												#print ("Error occurred while collecting source table row count1")
												logdata(errFile,str(e),1)
												Subject ="Error: Error occurred while collecting source table row count."
												errMsg=Subject + "\n\n" + str(e)
												sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
												continue
												#print(str(e))
										else:
											for srcanalobj in results:
												#srclastAnal=srcanalobj[2]
												srclastAnal=srcanalobj[1]
												srcNumrows=srcanalobj[0]
												logdata(logFile,"Source table has " + str(srcNumrows) + " rows based on stats gathered which is " + str(srclastAnal) + " days old",0)
								except Exception as e:
									updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,"Error occurred while collecting source table stats",1)
									#print ("Error occurred while collecting source table stats")
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while collecting source table stats."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
									#print(str(e))
								try:
									if (statCount > 0):
										if srclastAnal > Anal_Threshold:
											logdata(logFile,"Since table stats are not upto date pulling table count from table",0)
											sql6="LOCK ROW FOR ACCESS select cast(count(*) as bigint) from " + sourceSchema + "." + sourceTable
											cursor2.execute(sql6)
											results=cursor2.fetchall()
											for srcCntobj in results:
												srcNumrows=srcCntobj[0]
											logdata(logFile,"Table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows",0)
										else:
											if srcNumrows == 0:
												logdata(logFile,"Since source table had 0 rows based on stats revalidating count",0)
												sql6=f"LOCK ROW FOR ACCESS select count(*) from {sourceSchema}.{sourceTable}"
												cursor2.execute(sql6)
												results=cursor2.fetchall()
												for srcCntobj in results:
													srcNumrows=srcCntobj[0]
												logdata(logFile,"Table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows pulled from table",0)
								except Exception as e:
									#print ("Error occurred while collecting source table row count2")
									updDIYmstr(reqid,'err_msg',colval  + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,"Error occurred while collecting source table row count2",1)
									logdata(errFile,sql6,0)
									#print(str(e))
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while collecting source table row count."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
							else:
								logdata(logFile,"Since source table dont have stats enabled pulling count directly from table",0)
								try:
									sql6="LOCK ROW FOR ACCESS select cast(count(*) as bigint) from " + sourceSchema + "." + sourceTable + " where " + whereClause
									cursor2.execute(sql6)
									results=cursor2.fetchall()
									for srcCntobj in results:
										srcNumrows=srcCntobj[0]
									logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows",0)
								except Exception as e:
									updDIYmstr(reqid,'err_msg',colval  + str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,"Error occurred while collecting source table row count3",1)
									logdata(errFile,sql6,0)
									#print ("Error occurred while collecting source table row count3")
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while collecting source table row count."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									#print(str(e))
									continue
							aud_num_rows=srcNumrows
						else:
							logdata(logFile,"Pulling incremental column data",0)
							updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
							try:
								sql8 = "SELECT distinct decode(EXTRACT_TYPE,'DATE',to_char(to_extract_dtm,'MM/DD/YYYY HH24:MI:SS'),'ID',to_char(to_extract_id)) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and workflow_type='SRC2STG'"
								cursor1.execute(sql8)
								results = cursor1.fetchall()
								for lastextobj in results:
									lastextVal=lastextobj[0]
								if incColname is None:
									sql9=''
									raise Exception("Missing incremental column for JCT - " + jobStream + ". Please check and fix..")
								if lastextVal is None:
									sql9=''
									raise Exception("Missing incremental column value for JCT - " + jobStream + ". Please check and fix..")
								if eType not in ('ID','DATE'):
									colval="Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
									updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,colval,1)
									Subject ="Error: Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
									errMsg=Subject
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
								si_cnt=0
								sql=f"select count(1) cnt from dbc.indicesv where indextype='S' and columnname='{incColname}'"
								sql+=f" and tablename='{srcBasetable}' and databasename='{srcBaseschema}'"
								cursor2.execute(sql)
								results=cursor2.fetchall()
								for cntobj in results:
									si_cnt=cntobj[0]
								if si_cnt > 0 and eType == 'DATE':
									logdata(logFile,"Pulling distinct incremental value",0)
									sql=f"LOCK ROW FOR ACCESS select distinct {incColname} from {sourceSchema}.{sourceTable} "
									sql+=f" where {incColname} > to_timestamp('{lastextVal}','MM/DD/YYYY HH24:MI:SS') GROUP BY 1"
									cursor2.execute(sql)
									results=cursor2.fetchall()
									if len(results) > 0:
										if len(results) <= 200:
											vt_flag=True
											logdata(logFile,f"Found distinct count as {len(results)} rows. Hence proceeding with forming array",0)
											dt_array='('
											for results_obj in results:
												dt_array+=f"'{results_obj[0]}',"
											dt_array=dt_array[:-1] ## remove last comma
											dt_array+=')'
											logdata(logFile,f"Now update DIY_Master with dt_array",0)
											updDIYmstr(reqid,'attribute7',dt_array,cursor1,errFile,logFile,emailAdd)
										else:
											logdata(logFile,f"Though inc index exists count of distinct is > 200 so not proceeding with array",0)
								if vt_flag:
									logdata(logFile,"Pulling count from source based on last extract date array - " + dt_array,0)
									sql9 =f"LOCK ROW FOR ACCESS SELECT cast(count(*) as bigint) from {sourceSchema}.{sourceTable} where {incColname} in {dt_array}"
								else:
									if eType == 'DATE':
										logdata(logFile,"Pulling count from source based on last extract date - " + str(lastextVal),0)
										sql9 ="LOCK ROW FOR ACCESS SELECT cast(count(*) as bigint) from " + sourceSchema + "." + sourceTable + " where " + incColname + " > to_timestamp('" + lastextVal + "','MM/DD/YYYY HH24:MI:SS')"
									elif eType == 'ID':
										logdata(logFile,"Pulling count from source table based on last extract id - " + str(lastextVal),0)
										sql9 ="LOCK ROW FOR ACCESS SELECT cast(count(*) as bigint) from " + sourceSchema + "." + sourceTable + " where " + incColname + " > " + str(lastextVal) 
								if len(whereClause) > 0:
									logdata(logFile,"Filter existing.. Applying the same before pulling rowcount",0)
									sql9 = sql9 + " AND " + whereClause
								cursor2.execute(sql9)
								results=cursor2.fetchall()
								for srcCntobj in results:
									srcNumrows=srcCntobj[0]
								logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows for ingestion",0)
							except Exception as e:
								#print ("Error occurred while collecting source table row count4")
								colval="Error occurred while collecting source table row count."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row count4",1)
								if sql9 is not None:
									logdata(errFile,sql9,0)
								#print(str(e))
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						#print("Source table count is - " + str(srcNumrows))
						updDIYmstr(reqid,'src_rows',str(srcNumrows),cursor1,errFile,logFile,emailAdd)
						if itype.lower()=='jct':
							try:
								logdata(logFile,"Pulling source count for audit purpose",0)
								sql=f"select source_deleted_flag from edw_job_streams where job_stream_id='{jobStream}'"
								cursor1.execute(sql)
								results=cursor1.fetchall()
								for results_obj in results:
									del_flag=results_obj[0]
								if del_flag == 'A' and aud_num_rows == 0:
									sql=f"LOCK ROW FOR ACCESS SELECT cast(count(*) as bigint) from {sourceSchema}.{sourceTable}"
									if len(whereClause) > 0:
										if " where" in whereClause:
											sql += f" and  {whereClause}"
										else:
											sql += f" where {whereClause}"
									cursor2.execute(sql)
									results=cursor2.fetchall()
									for srcCntobj in results:
										aud_num_rows=srcCntobj[0]
							except Exception as e:
								colval="Error occurred while collecting source table row count for audit."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row for audit",1)
								if sql is not None:
									logdata(errFile,sql,0)
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count for audit."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						if srcNumrows == 0:
							zeroRecIngestion(jobStream,reqid,cursor1,errFile,logFile,emailAdd,envType,LOGS_DIR,cursor3,sourcedbType,ltype)
							continue
						if not threadoverride:
							if srcNumrows >= Split_Threshold:
								logdata(logFile,"Qualified rows demands divide and conquer type ingestion",0)
								if srcNumrows >= Split_Threshold and srcNumrows < (Split_Threshold*10):
									splitcount=2
								elif srcNumrows >= (Split_Threshold*10) and srcNumrows < (Split_Threshold*50):
									splitcount=3
								elif srcNumrows >= (Split_Threshold*50) and srcNumrows < (Split_Threshold*100):
									splitcount=5
								elif srcNumrows >= (Split_Threshold*100):
									splitcount=10
								#elif srcNumrows > (Split_Threshold*50):
								#	splitcount=10
						#if tCnt is not None:
						#	logdata(logFile,"Since thread count override is specified using " + tCnt + " threads for ingestion",0)
						#	threadcount=tCnt
						## Override parallel threads for TD based on table size to avoid source system resource issue
						if tblSize is not None:
							logdata(logFile,"Validating parallel thread count based on tablesize",1)
							if Thread_count > 5 or tCnt > 5:
								if tblSize > 1073741824: ## Table greater than 1 TB.. Force to use only 5 parallel threads due to long running nature.
									logdata(logFile,"Since source table - " + sourceTable + " is of size > 1 TB, overriding  paralle thread count from " + str(tCnt) + " to 5",1)
									Thread_count=5
									tCnt=5
							elif Thread_count > 10 or tCnt > 10:
								logdata(logFile,"Since a max of 10 parallel threads are only permitted for TD system, overriding  paralle thread count from " + str(tCnt) + " to 10",1)
								Thread_count=10
								tCnt=10
									
						if splitcount > 1:
							updDIYmstr(reqid,'current_phase','Split_Phase',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Invoking_Data_Load','Split_Phase',cursor1,errFile,emailAdd)
							logdata(logFile,"Going with " + str(splitcount) + " parallel threads",0)
							splitQrydict={}
							try:
								filter=''
								if eType == 'ALL_DATA' or len(eType) == 0:
									if len(whereClause) > 0:
										filter=whereClause
								else:
									if vt_flag:
										filter +=f"{incColname} in {dt_array}"
									else:
										if eType == 'DATE':
											filter += f" {incColname} > to_timestamp('{lastextVal}','MM/DD/YYYY HH24:MI:SS')"
										elif eType == 'ID':
											filter += f"{incColname} > {lastextVal}"
										if len(whereClause) > 0:
											filter += f" and {whereClause}"
									
								if sCmn is None:
									splitQrydict=diy_split(conn2,srcBaseschema,srcBasetable,splitcount,reqid,conn,filter,srcNumrows,eType,None,sourcedbType.upper(),sourceDb,'SRC2STG')
									if type(splitQrydict) != dict:
										logdata(logFile,f"Attempting to split resulted in {splitQrydict}. Hence checking if it can be ingested in single thread",1)
										if tblSize is not None and tblSize < 262144000:
											splitcount=1
											threadoverride=True
											splitQrydict={}
										else:
											logdata(errFile,"Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually",1)
											Subject ="Error:Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually"
											errMsg=Subject
											sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
											continue
								else:
										logdata(logFile,"Using the over-ride column for parallelizing extraction",0)
										srcIndcol=sCmn
										cursor2.execute("LOCK ROW FOR ACCESS select trim(columntype),regexp_replace(regexp_replace(substr(trim(columnformat),1,19),'DDHH','DD HH24'),'DDBHH','DD HH24') columnformat from dbc.columnsv where databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "' and columnname='" + sCmn + "'")
										results=cursor2.fetchall()
										if len(results) == 0:
											colval="Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
											updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
											updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
											logdata(errFile,colval,1)
											Subject ="Error: Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
											errMsg=Subject
											sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
											continue
										else:
											splitQrydict=diy_split(conn2,srcBaseschema,srcBasetable,splitcount,reqid,conn,filter,srcNumrows,eType,sCmn,sourcedbType.upper(),sourceDb,'SRC2STG')
											if type(splitQrydict) != dict:
												logdata(logFile,f"Attempting to split resulted in {splitQrydict}. Hence checking if it can be ingested in single thread",1)
												if tblSize is not None and tblSize < 262144000:
													splitcount=1
													threadoverride=True
													splitQrydict={}
												else:
													logdata(errFile,"Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually",1)
													Subject ="Error:Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually"
													errMsg=Subject
													sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
													continue
								logdata(logFile,"Completed pulling the range partitions for data pull",0)
							except Exception as e:
								#print ("Error occurred while gathering columns for source table")
								colval="Error occurred while gathering columns for source table."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								#print(str(e))
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while gathering columns for source table."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						else:
							splitQrydict={}
							#splitQrydict[0]="select * from " + sourceSchema + "." + sourceTable 
							if len(whereClause) > 0:
								splitQrydict[0] = whereClause
							else:
								splitQrydict[0]=""
					insQryDict=[]
					jobStreamarr=[]
					#jobStreammstr=jobStream + "_" + str(ftime)
					jobStreammstr=jobStream
					if len(splitQrydict) > 1:
						jobStreammstr=jobStream + "_" + str(ftime)
						jobstreamid=jobStream
						compCode='A'
					else:
						jobStreammstr=jobStream
						jobstreamid=jobStream
						jobStreamarr.append(jobStreammstr)
						compCode='P'
					if len(incColname) > 0:
						source_del_flag='A'
					else:
						source_del_flag='N'
					if existFlag == 'FALSE':
						logdata(logFile,"Since this is new history ingestion forcing metadata collection from source",1)
						rc=collectMetadata(envType,sourceDb,sourceSchema,sourceTable,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if rc !=0 :
							raise Exception("Issue encountered while collecting metadata. Please check logfile")
						#whereClauseIns=whereClause.replace("'","''").replace("''''","''")
						whereClauseIns=whereClause.replace("'","''")
						InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
						InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
						InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
						InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by,source_deleted_flag)"
						InsQry += " values "
						InsQry += "('" + jobstreamid + "','" + jobstreamid + "','" + ijobGroup + "',1,'Y','" + compCode + "','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
						InsQry += "','" + sourceTable + "','" + whereClauseIns + "','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
						InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable  + "','" + sfWh + "','" + sfRole+ "',NULL"
						InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API','" + source_del_flag + "')"
						#print (InsQry)
						logdata(logFile,"Master record for insertion",0)
						#logdata(logFile,InsQry,0)
						insQryDict.append(InsQry)
					if existFlag == 'TRUE':
						if len(splitQrydict) > 1:
							logdata(logFile,"Identified incremental ingestion as large batch.. Hence updating run_status for master record",0)
							try:
								UpdQry = "Update EDW_JOB_STREAMS set run_status = '" + compCode + "' where job_stream_id='" + jobstreamid + "' and workflow_type='SRC2STG' and source_db_name='"
								UpdQry = UpdQry + sourceDb + "' and source_schema='" + sourceSchema + "' and source_table_name='" + sourceTable + "' and workflow_type='SRC2STG'"
								cursor1.execute(UpdQry)
								cursor1.execute('commit')
							except Exception as e:
								colval="Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								logdata(errFile,InsQry,0)
								logdata(errFile,str(e),1)
								Subject ="Error: Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						else:
							cursor1.execute("update EDW_JOB_STREAMS set job_group_id='" + ijobGroup + "' where job_stream_id='" + jobstreamid + "'")
							cursor1.execute("commit")
					if len(splitQrydict) > 1:
						split4upd=len(splitQrydict)
						logdata(logFile,"Child records for insertion",0)
						for key,value in splitQrydict.items():
							if len(whereClause) > 0:
								sql=f"select distinct split_column sc from diy_splits where reqid={reqid}"
								splitcol_obj=execute_df_qry(conn,sql,reqid,conn)
								srcIndcol=splitcol_obj["sc"][0]
								#if srcIndcol + " " in whereClause:
								#	whereClausenew = value
								#else:
								whereClausenew = whereClause + ' and ' + value
							else:
								whereClausenew = value
							## Change 4.13
							if len(incColname) > 0:
								if eType == 'DATE':
									if vt_flag:
										whereClausenew = whereClausenew + f' AND {incColname} in {dt_array}'   ## check to_date syntax
									else:
										whereClausenew = whereClausenew + ' AND ' + incColname + " > to_timestamp(''" + lastextVal + "'',''MM/DD/YYYY HH24:MI:SS'')"  ## check to_date syntax
								elif  eType == 'ID':
									whereClausenew = whereClausenew + ' AND ' + incColname + " > " + str(lastextVal) 
							#jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
							#whereClauseIns=whereClausenew.replace("'","''").replace("''''","''")
							whereClauseIns=whereClausenew.replace("'","''")
							InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
							InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
							InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
							InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by)"
							InsQry += " values "
							InsQry += "('" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + ijobGroup + "','" + str(key)  
							InsQry += "','Y','P','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
							InsQry += "','" + sourceTable + "',' " + whereClauseIns + " ','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
							InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable + "_" + str(key) + "','" + sfWh + "','" + sfRole + "',NULL"
							InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API')"
							try:
								cursor1.execute("select * from (select job_stream_id from EDW_JOB_STREAMS where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG' order by create_date) where rownum=1")
								results=cursor1.fetchall()
								if len(results) == 0:
									insQryDict.append(InsQry)
									jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
								else:
									for existObj in results:
										jctExist=existObj[0]
									updQry = "Update EDW_JOB_STREAMS set active_ind='Y',run_status='P',where_clause='" + whereClauseIns + "',modify_date=sysdate"
									updQry = updQry + ",SOURCE_DB_NAME='" + sourceDb + "',SOURCE_SCHEMA='" + sourceSchema + "',target_db_name='" + targetDb + "',target_schema='" + targetSchema + "',sf_warehouse='" + sfWh + "',sf_role='" + sfRole + "',extract_type='" + eType + "'"
									updQry = updQry + ",source_db_connection='" + connectionName + "',source_table_name='" + sourceTable + "',target_table_name='" + targetTable + "_" + str(key) + "',modified_by='" + currOsUser + "',incremental_column_Name='" + incColname + "'"
									updQry = updQry + ",job_group_id='" + ijobGroup + "' where job_stream_id like '" +  jctExist + "' and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG'"
									insQryDict.append(updQry)
									jobStreamarr.append(jctExist)
							except Exception as e:
								logdata(logFile,"Issue encountered while checking existing JCT child process",1)
								logdata(errFile,str(e),1)
								insQryDict.append(InsQry)
							#print (InsQry)
							#logdata(logFile,InsQry,0)
							#insQryDict.append(InsQry)
					## End of indentation for Teradata 

				elif sourcedbType.upper() == 'HANA':
					logdata(logFile,"Invoking data ingestion engine for HANA db",0)
					if (tblCntvar == 0 and itype.lower() == 'table') or itype.lower() == 'db'  or itype.lower() == 'jct':
						tblCntvar=1
						metRc=validateMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if metRc == -1:
							colval="Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
							updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							Subject ="Error: Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						else:
							updDIYmstr(reqid,'current_phase','Metadata_validation_Succesful',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'INIT','Metadata_validation_Succesful',cursor1,errFile,emailAdd)
							logdata(logFile,"Found - " + str(metRc) + " columns for table - " + sourceTable + " from metadata table",0)
						if eType == 'ALL_DATA' or len(eType) == 0:
							logdata(logFile,"Invoking a history load",0)
							updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
							if len(whereClause) == 0:
								try:
									if obj_type == 'TABLE':
										logdata(logFile,"Since this is Full load with no filters checking space availability before triggering ingestion",0)
										sql16="select  (disk_size/1024)*10 from M_TABLE_PERSISTENCE_STATISTICS where schema_name='" + srcBaseschema + "' and table_name='" + srcBasetable + "'" 
										cursor2.execute(sql16)
										results=cursor2.fetchall()
										if len(results) == 0:
											logdata(errFile,"Could not locate base table " + srcBaseschema + "." + srcBasetable + ". Hence aborting ingestion for this stream",1)
											Subject ="Error: Could not locate base table " + srcBaseschema + "." + srcBasetable + ". Hence aborting ingestion for this stream"
											errMsg=Subject
											sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
											continue
										else:
											for tblsizeObj in results:
												tblSize=tblsizeObj[0]
											osFreespacecmd="df " + stageMnt +" | tail -1 | awk '{print $4}'"
											freeSpace=int(os.popen(osFreespacecmd).read())
											spaceCheck=True
											#if freeSpace < tblSize*2:
											#	logdata(errFile,"Stage mount - " + stageMnt + " doesnt have enough space for unloading data from " + srcBaseschema + "." + srcBasetable + " table.. Hence aborting ingestion",1)
											#	continue
										updDIYmstr(reqid,'tbl_size',str(tblSize),cursor1,errFile,logFile,emailAdd)
								except Exception as e:
									logdata(errFile,"Issue occurred while checking mount free space.. Ignoring this check and continuing..",0)
									logdata(errFile,str(e),1)
								try:
									logdata(logFile,"Querying count from source for audit purpose",0)
									sql5=f'select cast( count(*) as bigint) from {sourceSchema}."{sourceTable}"'
									cursor2.execute(sql5)
									results=cursor2.fetchall()
									for srcCntobj in results:
										srcNumrows=srcCntobj[0]
										statCount=1
										srclastAnal=Anal_Threshold
										logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows pulled by running count",0)
								except Exception as e:
									updDIYmstr(reqid,'err_msg',str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,"Error occurred while collecting source table count",1)
									#print ("Error occurred while collecting source table stats")
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while collecting source table count."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
									#print(str(e))
							else:
								logdata(logFile,"Pulling row count by applying filter",0)
								try:
									sql6=f'select cast(count(*) as bigint) from {sourceSchema}."{sourceTable}" where {whereClause}'
									cursor2.execute(sql6)
									results=cursor2.fetchall()
									for srcCntobj in results:
										srcNumrows=srcCntobj[0]
									logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows",0)
								except Exception as e:
									updDIYmstr(reqid,'err_msg',str(e),cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,"Error occurred while collecting source table row count3",1)
									logdata(errFile,sql6,0)
									#print ("Error occurred while collecting source table row count3")
									logdata(errFile,str(e),1)
									Subject ="Error: Error occurred while collecting source table row count."
									errMsg=Subject + "\n\n" + str(e)
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									#print(str(e))
									continue
							aud_num_rows=srcNumrows
						else:
							logdata(logFile,"Pulling incremental column data",0)
							updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
							try:
								sql8 = "SELECT distinct decode(EXTRACT_TYPE,'DATE',to_char(to_extract_dtm,'MM/DD/YYYY HH24:MI:SS'),'ID',to_char(to_extract_id)) from EDW_JOB_STREAMS where JOB_STREAM_ID like '"   + jobStream + "' and workflow_type='SRC2STG'"
								cursor1.execute(sql8)
								results = cursor1.fetchall()
								for lastextobj in results:
									lastextVal=lastextobj[0]
								if incColname is None:
									sql9=''
									raise Exception("Missing incremental column for JCT - " + jobStream + ". Please check and fix..")
								if lastextVal is None:
									sql9=''
									raise Exception("Missing incremental column value for JCT - " + jobStream + ". Please check and fix..")
								if eType == 'DATE':
									logdata(logFile,"Pulling count from source based on last extract date - " + str(lastextVal),0)
									sql9 =f'SELECT cast(count(*) as bigint) from {sourceSchema}."{sourceTable}" where {incColname} > to_timestamp(\'{lastextVal}\',\'MM/DD/YYYY HH24:MI:SS\')'
								elif eType == 'ID':
									logdata(logFile,"Pulling count from source table based on last extract id - " + str(lastextVal),0)
									sql9 =f'SELECT cast(count(*) as bigint) from {sourceSchema}."{sourceTable}" where {incColname} > {lastextVal}' 
								else:
									#print("Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry")
									colval="Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
									updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
									updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
									logdata(errFile,colval,1)
									Subject ="Error: Invalid incremental column data type. Only DATE & ID are supported.. Please check and retry."
									errMsg=Subject
									sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
									continue
								if len(whereClause) > 0:
									logdata(logFile,"Filter existing.. Applying the same before pulling rowcount",0)
									sql9 = sql9 + " AND " + whereClause
								cursor2.execute(sql9)
								results=cursor2.fetchall()
								for srcCntobj in results:
									srcNumrows=srcCntobj[0]
								logdata(logFile,"Source table - " + sourceSchema + "." + sourceTable + " has " + str(srcNumrows) + " rows for ingestion",0)
							except Exception as e:
								#print ("Error occurred while collecting source table row count4")
								colval="Error occurred while collecting source table row count."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row count4",1)
								if sql9 is not None:
									logdata(errFile,sql9,0)
								#print(str(e))
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						#print("Source table count is - " + str(srcNumrows))
						updDIYmstr(reqid,'src_rows',str(srcNumrows),cursor1,errFile,logFile,emailAdd)
						if itype.lower()=='jct':
							try:
								logdata(logFile,"Pulling source count for audit purpose",0)
								sql=f"select source_deleted_flag from edw_job_streams where job_stream_id='{jobStream}'"
								cursor1.execute(sql)
								results=cursor1.fetchall()
								for results_obj in results:
									del_flag=results_obj[0]
								if del_flag == 'A' and aud_num_rows == 0:
									sql=f'SELECT cast(count(*) as bigint) from {sourceSchema}."{sourceTable}"'
									if len(whereClause) > 0:
										if " where" in whereClause:
											sql += f" and  {whereClause}"
										else:
											sql += f" where {whereClause}"
									cursor2.execute(sql)
									results=cursor2.fetchall()
									for srcCntobj in results:
										aud_num_rows=srcCntobj[0]
							except Exception as e:
								colval="Error occurred while collecting source table row count for audit."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,"Error occurred while collecting source table row for audit",1)
								if sql is not None:
									logdata(errFile,sql,0)
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while collecting source table row count for audit."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						if srcNumrows == 0:
							zeroRecIngestion(jobStream,reqid,cursor1,errFile,logFile,emailAdd,envType,LOGS_DIR,cursor3,sourcedbType,ltype)
							continue
						if not threadoverride:
							if srcNumrows >= Split_Threshold:
								logdata(logFile,"Qualified rows demands divide and conquer type ingestion",0)
								splitcount=5
								if srcNumrows >= (Chunk_Orcl_Threshold/2) and  srcNumrows < (Chunk_Orcl_Threshold*3):
									splitcount=10
								elif srcNumrows >= (Chunk_Orcl_Threshold*3):
									splitcount=20
								#elif srcNumrows > (Split_Threshold*50):
								#	splitcount=10
						## Override parallel threads for TD based on table size to avoid source system resource issue
						if splitcount > 1:
							updDIYmstr(reqid,'current_phase','Split_Phase',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Invoking_Data_Load','Split_Phase',cursor1,errFile,emailAdd)
							logdata(logFile,"Going with " + str(splitcount) + " parallel threads",0)
							try:
								splitQrydict={}
								filter=''
								if eType == 'ALL_DATA' or len(eType) == 0:
									if len(whereClause) > 0:
										filter=whereClause
								else:
									if eType == 'DATE':
										filter += f" {incColname} > to_timestamp('{lastextVal}','MM/DD/YYYY HH24:MI:SS')"
									elif eType == 'ID':
										filter += f"{incColname} > {lastextVal}"
									if len(whereClause) > 0:
										filter += f" and {whereClause}"
								if sCmn is None:
									splitQrydict=diy_split(conn2,srcBaseschema,srcBasetable,splitcount,reqid,conn,filter,srcNumrows,eType,None,sourcedbType.upper(),sourceDb,'SRC2STG')
									if type(splitQrydict) != dict:
										logdata(logFile,f"Attempting to split resulted in {splitQrydict}. Hence checking if it can be ingested in single thread",1)
										if tblSize is not None and tblSize < 262144000:
											splitcount=1
											threadoverride=True
											splitQrydict={}
										else:
											logdata(errFile,"Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually",1)
											Subject ="Error:Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually"
											errMsg=Subject
											sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
											continue
								else:
									logdata(logFile,"Using the over-ride column for parallelizing extraction",0)
									srcIndcol=sCmn
									cursor2.execute("select data_type_name from sys.table_columns where table_name='" + srcBasetable + "' and schema_name='" + srcBaseschema + "' and column_name='" + sCmn + "'")
									results=cursor2.fetchall()
									if len(results) == 0:
										colval="Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
										updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
										updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
										logdata(errFile,colval,1)
										Subject ="Error: Provided column " + sCmn + " for table " + srcBaseschema + "." + srcBasetable + " is invalid.. Please check and retry"
										errMsg=Subject
										sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
										continue
									else:
										splitQrydict=diy_split(conn2,srcBaseschema,srcBasetable,splitcount,reqid,conn,filter,srcNumrows,eType,sCmn,sourcedbType.upper(),sourceDb,'SRC2STG')
										if type(splitQrydict) != dict:
											logdata(logFile,f"Attempting to split resulted in {splitQrydict}. Hence checking if it can be ingested in single thread",1)
											if tblSize is not None and tblSize < 262144000:
												splitcount=1
												threadoverride=True
												splitQrydict={}
											else:
												logdata(errFile,"Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually",1)
												Subject ="Error:Could not find any number/date column on large table. Hence could not proceed with multi thread ingestion. Please check and take care manually"
												errMsg=Subject
												sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
												continue
								logdata(logFile,"Completed pulling the range partitions for data pull",0)
							except Exception as e:
								#print ("Error occurred while gathering columns for source table")
								colval="Error occurred while gathering columns for source table."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								#print(str(e))
								logdata(errFile,str(e),1)
								Subject ="Error: Error occurred while gathering columns for source table."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						else:
							splitQrydict={}
							#splitQrydict[0]="select * from " + sourceSchema + "." + sourceTable 
							if len(whereClause) > 0:
								splitQrydict[0] = whereClause
							else:
								splitQrydict[0]=""
					insQryDict=[]
					jobStreamarr=[]
					#jobStreammstr=jobStream + "_" + str(ftime)
					jobStreammstr=jobStream
					if len(splitQrydict) > 1:
						jobStreammstr=jobStream + "_" + str(ftime)
						jobstreamid=jobStream
						compCode='A'
					else:
						jobStreammstr=jobStream
						jobstreamid=jobStream
						jobStreamarr.append(jobStreammstr)
						compCode='P'
					if len(incColname) > 0:
						source_del_flag='A'
					else:
						source_del_flag='N'
					if existFlag == 'FALSE':
						logdata(logFile,"Since this is new history ingestion forcing metadata collection from source",1)
						rc=collectMetadata(envType,sourceDb,sourceSchema,sourceTable,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if rc !=0 :
							raise Exception("Issue encountered while collecting metadata. Please check logfile")
						#whereClauseIns=whereClause.replace("'","''").replace("''''","''")
						whereClauseIns=whereClause.replace("'","''")
						InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
						InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
						InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
						InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by,source_deleted_flag)"
						InsQry += " values "
						InsQry += "('" + jobstreamid + "','" + jobstreamid + "','" + ijobGroup + "',1,'Y','" + compCode + "','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
						InsQry += "','" + sourceTable + "','" + whereClauseIns + "','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
						InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable  + "','" + sfWh + "','" + sfRole+ "',NULL"
						InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API','" + source_del_flag + "')"
						#print (InsQry)
						logdata(logFile,"Master record for insertion",0)
						#logdata(logFile,InsQry,0)
						insQryDict.append(InsQry)
					if existFlag == 'TRUE':
						if len(splitQrydict) > 1:
							logdata(logFile,"Identified incremental ingestion as large batch.. Hence updating run_status for master record",0)
							try:
								UpdQry = "Update EDW_JOB_STREAMS set run_status = '" + compCode + "' where job_stream_id='" + jobstreamid + "' and workflow_type='SRC2STG' and source_db_name='"
								UpdQry = UpdQry + sourceDb + "' and source_schema='" + sourceSchema + "' and source_table_name='" + sourceTable + "' and workflow_type='SRC2STG'"
								cursor1.execute(UpdQry)
								cursor1.execute('commit')
							except Exception as e:
								colval="Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								logdata(errFile,InsQry,0)
								logdata(errFile,str(e),1)
								Subject ="Error: Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						else:
							cursor1.execute("update EDW_JOB_STREAMS set job_group_id='" + ijobGroup + "' where job_stream_id='" + jobstreamid + "'")
							cursor1.execute("commit")
					if len(splitQrydict) > 1:
						split4upd=len(splitQrydict)
						logdata(logFile,"Child records for insertion",0)
						for key,value in splitQrydict.items():
							if len(whereClause) > 0:
								sql=f"select distinct split_column sc from diy_splits where reqid={reqid}"
								splitcol_obj=execute_df_qry(conn,sql,reqid,conn)
								srcIndcol=splitcol_obj["sc"][0]
								#if srcIndcol + " " in whereClause:
								#	whereClausenew = value
								#else:
								whereClausenew = whereClause + ' and ' + value
							else:
								whereClausenew = value
							## Change 4.13
							if len(incColname) > 0:
								if eType == 'DATE':
									whereClausenew = whereClausenew + ' AND ' + incColname + " > to_timestamp(''" + lastextVal + "'',''MM/DD/YYYY HH24:MI:SS'')"  ## check to_date syntax
								elif  eType == 'ID':
									whereClausenew = whereClausenew + ' AND ' + incColname + " > " + str(lastextVal) 
							#jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
							#whereClauseIns=whereClausenew.replace("'","''").replace("''''","''")
							whereClauseIns=whereClausenew.replace("'","''")
							InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
							InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
							InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
							InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by)"
							InsQry += " values "
							InsQry += "('" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + jobStream + "_" + str(ftime) + "_" + str(key) + "','" + ijobGroup + "','" + str(key)  
							InsQry += "','Y','P','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
							InsQry += "','" + sourceTable + "',' " + whereClauseIns + " ','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
							InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable + "_" + str(key) + "','" + sfWh + "','" + sfRole + "',NULL"
							InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API')"
							try:
								cursor1.execute("select * from (select job_stream_id from EDW_JOB_STREAMS where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG' order by create_date) where rownum=1")
								results=cursor1.fetchall()
								if len(results) == 0:
									insQryDict.append(InsQry)
									jobStreamarr.append(jobStream + "_" + str(ftime) + "_" + str(key))
								else:
									for existObj in results:
										jctExist=existObj[0]
									updQry = "Update EDW_JOB_STREAMS set active_ind='Y',run_status='P',where_clause='" + whereClauseIns + "',modify_date=sysdate"
									updQry = updQry + ",SOURCE_DB_NAME='" + sourceDb + "',SOURCE_SCHEMA='" + sourceSchema + "',target_db_name='" + targetDb + "',target_schema='" + targetSchema + "',sf_warehouse='" + sfWh + "',sf_role='" + sfRole + "',extract_type='" + eType + "'"
									updQry = updQry + ",source_db_connection='" + connectionName + "',source_table_name='" + sourceTable + "',target_table_name='" + targetTable + "_" + str(key) + "',modified_by='" + currOsUser + "',incremental_column_Name='" + incColname + "'"
									updQry = updQry + ",job_group_id='" + ijobGroup + "' where job_stream_id like '" +  jctExist + "' and job_stream_seq_num=" + str(key) + " and run_status='C' and workflow_type='SRC2STG'"
									insQryDict.append(updQry)
									jobStreamarr.append(jctExist)
							except Exception as e:
								logdata(logFile,"Issue encountered while checking existing JCT child process",1)
								logdata(errFile,str(e),1)
								insQryDict.append(InsQry)
							#print (InsQry)
							#logdata(logFile,InsQry,0)
							#insQryDict.append(InsQry)
					## End of indentation for HANA type ingestion
				## HANANRT alone will have different logic as no split involved and it gets loaded batch by batch.
				elif sourcedbType.upper() == 'HANANRT':
					logdata(logFile,"Invoking data ingestion engine for HANANRT db",0)
					if (tblCntvar == 0 and itype.lower() == 'table') or itype.lower() == 'db'  or itype.lower() == 'jct':
						tblCntvar=1
						metRc=validateMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if metRc == -1:
							colval="Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
							updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							Subject ="Error: Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						else:
							updDIYmstr(reqid,'current_phase','Metadata_validation_Succesful',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'INIT','Metadata_validation_Succesful',cursor1,errFile,emailAdd)
							logdata(logFile,"Found - " + str(metRc) + " columns for table - " + sourceTable + " from metadata table",0)
						## HANA NRT loads are not incremental and so shoud have load type as ALL_DATA only.
						## Though load type says ALL_DATA, only incremental changes will produce new batches and framework will process only new batches.
						if eType == 'ALL_DATA' or len(eType) == 0:
							updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
							if len(whereClause) != 0:
								logdata(logFile,"Filtered load for HANA NRT is unsupported",0)
								err_msg="Fintered load is unsupported for HANA NRT"
								updDIYmstr(reqid,'err_msg',str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								Subject = err_msg
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								#print(str(e))
								continue
							logdata(logFile,"Querying count from source for audit purpose",0)
							sql5=f'select cast( count(*) as bigint) from {srcBaseschema}."{srcBasetable}"'
							cursor2.execute(sql5)
							results=cursor2.fetchall()
							for srcCntobj in results:
								aud_num_rows=srcCntobj[0]
						else:
								logdata(logFile,"HANA NRT Type can handle only ALL_DATA",0)
								err_msg="HANA NRT Type can handle only ALL_DATA"
								updDIYmstr(reqid,'err_msg',str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								Subject = err_msg
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								#print(str(e))
								continue
						#print("Source table count is - " + str(srcNumrows))
						## Pending batches eligible for process will be pulled thorugh a stored procedure provided by HANA team.
						logdata(logFile,"Querying rowcount to process based on pending batch count",0)
						sql=f"CALL \"EXTRACT\".\"extract.procedures::getPendingBatches\"('SNFLK','{srcBaseschema}','{srcBasetable}')"
						cursor2.execute(sql)
						results=cursor2.fetchall()
						batch_2_process=sorted(results,key=itemgetter(1))
						srcNumrows=0
						for pendbtch_obj in batch_2_process:
							srcNumrows += pendbtch_obj[1]
						## Summing up batch row count acorss all pending patches will get total source rows eligible for process. 
						updDIYmstr(reqid,'src_rows',str(srcNumrows),cursor1,errFile,logFile,emailAdd)
						## If no eligible batches found then zero record logic will be followed
						if srcNumrows == 0:
							zeroRecIngestion(jobStream,reqid,cursor1,errFile,logFile,emailAdd,envType,LOGS_DIR,cursor3,sourcedbType,ltype)
							continue
						else:
							## If we found eligible batches to process same will be inserted into DIY_HANA_NRT_BATCH_LIST. REQID will be link between this table and DIY_MASTER table.
							## Through this link we can easily identify which batch got processed when.
							logdata(logFile,"Inserting batches to process table",0)
							for pendbtch_obj in batch_2_process:
								btch_number=pendbtch_obj[0]
								row_count=pendbtch_obj[1]
								sql=f"insert into DIY_HANA_NRT_BATCH_LIST (reqid,batch_number,src_rows,run_status) values "
								sql+=f"({reqid},{btch_number},{row_count},'P')"
								cursor1.execute(sql)
							cursor1.execute('commit')
					splitQrydict={}
					splitQrydict[0]="" ## Hard code since NRT will always go with single JCT
					insQryDict=[]
					jobStreamarr=[]
					#jobStreammstr=jobStream + "_" + str(ftime)
					jobStreammstr=jobStream
					if len(splitQrydict) > 1:
						jobStreammstr=jobStream + "_" + str(ftime)
						jobstreamid=jobStream
						compCode='A'
					else:
						jobStreammstr=jobStream
						jobstreamid=jobStream
						jobStreamarr.append(jobStreammstr)
						compCode='P'
					source_del_flag='N' ## source delete not possible for HANA NRT
					if existFlag == 'FALSE':
						logdata(logFile,"Since this is new history ingestion forcing metadata collection from source",1)
						rc=collectMetadata(envType,sourceDb,sourceSchema,sourceTable,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if rc !=0 :
							raise Exception("Issue encountered while collecting metadata. Please check logfile")
						#whereClauseIns=whereClause.replace("'","''").replace("''''","''")
						whereClauseIns=whereClause.replace("'","''")
						InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
						InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
						InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
						InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by,source_deleted_flag)"
						InsQry += " values "
						InsQry += "('" + jobstreamid + "','" + jobstreamid + "','" + ijobGroup + "',1,'Y','" + compCode + "','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
						InsQry += "','" + sourceTable + "','" + whereClauseIns + "','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
						InsQry += ",NULL,NULL,NULL,'EDW_DATALAKE_SVC','" + targetDb + "','" + targetSchema + "','" + targetTable  + "','" + sfWh + "','" + sfRole+ "',NULL"
						InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API','" + source_del_flag + "')"
						#print (InsQry)
						logdata(logFile,"Master record for insertion",0)
						#logdata(logFile,InsQry,0)
						insQryDict.append(InsQry)
					if existFlag == 'TRUE':
						if len(splitQrydict) > 1:
							logdata(logFile,"Identified incremental ingestion as large batch.. Hence updating run_status for master record",0)
							try:
								UpdQry = "Update EDW_JOB_STREAMS set run_status = '" + compCode + "' where job_stream_id='" + jobstreamid + "' and workflow_type='SRC2STG' and source_db_name='"
								UpdQry = UpdQry + sourceDb + "' and source_schema='" + sourceSchema + "' and source_table_name='" + sourceTable + "' and workflow_type='SRC2STG'"
								cursor1.execute(UpdQry)
								cursor1.execute('commit')
							except Exception as e:
								colval="Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								logdata(errFile,InsQry,0)
								logdata(errFile,str(e),1)
								Subject ="Error: Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						else:
							cursor1.execute("update EDW_JOB_STREAMS set job_group_id='" + ijobGroup + "' where job_stream_id='" + jobstreamid + "'")
							cursor1.execute("commit")
				## Snowflake's process also will differ slightly as it is based on Snowstreams for incremental processing.
				elif sourcedbType.upper() == 'SNOWFLAKE':
					logdata(logFile,"Invoking data ingestion engine for SNOWFLAKE db",0)
					if (tblCntvar == 0 and itype.lower() == 'table') or itype.lower() == 'db'  or itype.lower() == 'jct':
						tblCntvar=1
						metRc=validateMetadata(envType,sourceDb,sourceSchema,sourceTable,cursor1,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if metRc == -1:
							colval="Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
							updDIYmstr(reqid,'err_msg',colval,cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							logdata(errFile,colval,1)
							Subject ="Error: Not able to verify existence of metadata for table - " + sourceTable + ". Hence aborting ingetsion for this table.."
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
							continue
						else:
							updDIYmstr(reqid,'current_phase','Metadata_validation_Succesful',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'INIT','Metadata_validation_Succesful',cursor1,errFile,emailAdd)
							logdata(logFile,"Found - " + str(metRc) + " columns for table - " + sourceTable + " from metadata table",0)
						## Neither Full load nor incremental will undergo split as copy command can have size specified for each file and we will use the same to get data copied into mutliple files
						## Also we can specifiy thread count while invoking get command to read and write the files in parallel from stage to file system.
						if eType == 'ALL_DATA' or len(eType) == 0:
							updDIYmstr(reqid,'current_phase','Invoking_Data_Load',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Metadata_validation_Succesful','Invoking_Data_Load',cursor1,errFile,emailAdd)
							logdata(logFile,"Querying count from source for audit purpose",0)
							sql=f"use database {sourceDb}"
							cursor2.execute(sql)
							sql5=f'select count(*) cnt from {srcBaseschema}."{srcBasetable}"'
							cursor2.execute(sql5)
							results=cursor2.fetchall()
							for srcCntobj in results:
								aud_num_rows=srcCntobj[0]
							srcNumrows=aud_num_rows
						else:
							logdata(logFile,"Validting existence of stream for incremental load",0)
							## Incase of snowflake INCREMENTAL_COLUMN_NAME column is re-purposed for storing the snow stream name.
							sql=f"select incremental_column_name,sf_warehouse from edw_job_streams where job_stream_id='{jobStream}'"
							cursor1.execute(sql)
							results=cursor1.fetchall()
							for obj in results:
								inc_col_name=obj[0]
								sf_wh=obj[1]
							cursor2.execute(f"use warehouse {sf_wh}")
							if inc_col_name.count('.') != 2:
								raise Exception(f"Invalid Incremental column {inc_col_name}. Incremental column should be given as <<db>>.<<schema>>.<<stream>>")
							stream_db=inc_col_name.split('.')[0]
							stream_schema=inc_col_name.split('.')[1]
							stream_name=inc_col_name.split('.')[2]
							#sql=f"show streams like '{stream_name}' in schema {stream_db}.{stream_schema}"
							#cursor2.execute(sql)
							#results=cursor2.fetchall()
							#if len(results) != 1:
								#raise Exception(f"Invalid stream name {stream_db}.{stream_schema}.{stream_name}")
								#continue
							sql=f"select count(*) cnt from {stream_db}.{stream_schema}.{stream_name}"
							cursor2.execute(sql)
							results=cursor2.fetchall()
							for obj in results:
								srcNumrows=obj[0]
						## Based on stream name incremental volume count will be pulled and if no incremental data found zero record logic will be followed. 
						updDIYmstr(reqid,'src_rows',str(srcNumrows),cursor1,errFile,logFile,emailAdd)
						if srcNumrows == 0:
							zeroRecIngestion(jobStream,reqid,cursor1,errFile,logFile,emailAdd,envType,LOGS_DIR,cursor3,sourcedbType,ltype)
							continue
					splitQrydict={}
					splitQrydict[0]="" ## Hard code since SF always go with single JCT
					insQryDict=[]
					jobStreamarr=[]
					#jobStreammstr=jobStream + "_" + str(ftime)
					jobStreammstr=jobStream
					if len(splitQrydict) > 1:
						jobStreammstr=jobStream + "_" + str(ftime)
						jobstreamid=jobStream
						compCode='A'
					else:
						jobStreammstr=jobStream
						jobstreamid=jobStream
						jobStreamarr.append(jobStreammstr)
						compCode='P'
					source_del_flag='A' ## source delete not possible for HANA NRT
					if existFlag == 'FALSE':
						logdata(logFile,"Since this is new history ingestion forcing metadata collection from source",1)
						rc=collectMetadata(envType,sourceDb,sourceSchema,sourceTable,logFile,errFile,sourcedbType,connectionName,LOGS_DIR)
						if rc !=0 :
							raise Exception("Issue encountered while collecting metadata. Please check logfile")
						#whereClauseIns=whereClause.replace("'","''").replace("''''","''")
						whereClauseIns=whereClause.replace("'","''")
						InsQry = "Insert into EDW_JOB_STREAMS (job_stream_id,job_stream_name,job_group_id,job_stream_seq_num,active_ind,run_status,workflow_type,source_db_connection,source_db_name,source_schema"
						InsQry += ",source_table_name,where_clause,extract_type,max_value_from,incremental_column_name,from_extract_dtm,to_extract_dtm,from_extract_id,to_extract_id,previous_from_extract_dtm"
						InsQry += ",previous_to_extract_dtm,previous_from_extract_id,previous_to_extract_id,target_db_connection,target_db_name,target_schema,target_table_name,sf_warehouse,sf_role,src2stg_ff_path"
						InsQry += ",current_src2stg_ff_name,src2stg_ff_delimiter,merge_type,job_stream_id_link,batch_id,system_call,job_stream_description,run_frequency,create_date,created_by,modify_date,modified_by,source_deleted_flag)"
						InsQry += " values "
						InsQry += "('" + jobstreamid + "','" + jobstreamid + "','" + ijobGroup + "',1,'Y','" + compCode + "','SRC2STG','" +  connectionName + "','" + sourceDb + "','" + sourceSchema 
						InsQry += "','" + sourceTable + "','" + whereClauseIns + "','ALL_DATA','SOURCE','" + incColname + "',NULL,NULL,NULL,NULL,NULL"
						InsQry += ",NULL,NULL,NULL,'" + target_db_conn + "','" + targetDb + "','" + targetSchema + "','" + targetTable  + "','" + sfWh + "','" + sfRole+ "',NULL"
						InsQry += ",NULL,'|',NULL,NULL,NULL,'" + jobStream + "','" + jobStream + "','D',sysdate,'API',sysdate,'API','" + source_del_flag + "')"
						#print (InsQry)
						logdata(logFile,"Master record for insertion",0)
						#logdata(logFile,InsQry,0)
						insQryDict.append(InsQry)
					if existFlag == 'TRUE':
						if len(splitQrydict) > 1:
							logdata(logFile,"Identified incremental ingestion as large batch.. Hence updating run_status for master record",0)
							try:
								UpdQry = "Update EDW_JOB_STREAMS set run_status = '" + compCode + "' where job_stream_id='" + jobstreamid + "' and workflow_type='SRC2STG' and source_db_name='"
								UpdQry = UpdQry + sourceDb + "' and source_schema='" + sourceSchema + "' and source_table_name='" + sourceTable + "' and workflow_type='SRC2STG'"
								cursor1.execute(UpdQry)
								cursor1.execute('commit')
							except Exception as e:
								colval="Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								logdata(errFile,colval,1)
								logdata(errFile,InsQry,0)
								logdata(errFile,str(e),1)
								Subject ="Error: Updating JCT for multi thread incremental ingestion failed.. Hence skipping the ingestion for job stream " + jobstreamid + "."
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
								continue
						else:
							cursor1.execute("update EDW_JOB_STREAMS set job_group_id='" + ijobGroup + "' where job_stream_id='" + jobstreamid + "'")
							cursor1.execute("commit")

				if len(insQryDict) > 0:
					logdata(logFile,"Building JCT insert sql",0)
					jctFile = open(LOGS_DIR+'jctinsert.sql','w')
					for value in insQryDict:
						jctFile.write(value + ';\n')
					jctFile.write('commit')
					jctFile.close()
					## Execute generated SQL file
					jctFilef = open(LOGS_DIR+'jctinsert.sql')
					full_sql = jctFilef.read()
					jctFilef.close()
					sql_commands = full_sql.split(';\n')
					logdata(logFile,"Inserting JCT sqls",0)
					try:
						for sql_command in sql_commands:
							#print ("executing sql command - " + sql_command)
							cursor1.execute(sql_command)
					except Exception as e:
						logdata(logFile,"Rolling back inserted data due to exception",1)
						logdata(errFile,sql_command,0)
						cursor1.execute('rollback')
						colval="Error occurred while inserting sqls into JCT."
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						#print ("Error occurred while inserting sqls into JCT")
						logdata(errFile,str(e),1)
						Subject ="Error: Error occurred while inserting sqls into JCT."
						errMsg=Subject + "\n\n" + str(e)
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						continue
						#print(str(e))
				#if conn2:
				#	conn2.close()
			## End of indentation for retryflag=false
			else:
				logdata(logFile,"Now retrying existing jobstream - " + jobStream,1)
				jobStreamarr=[]
				try:
					sql14="select job_stream_id,job_group_id from edw_job_streams where job_stream_id like '" + jobStream + "' and active_ind='Y' and run_status='P' and workflow_type='SRC2STG'"
					cursor1.execute(sql14)
					results=cursor1.fetchall()
					if len(results) == 1:
						for resjobstr in results:
							jobStreammstr=resjobstr[0]
							ijobGroup=resjobstr[1]
						jobStreamarr.append(jobStreammstr)
					else:
						sql15="select job_stream_id,job_group_id from edw_job_streams where job_stream_id like '" + jobStream + "' and active_ind='Y' and run_status='A' and workflow_type='SRC2STG'"
						cursor1.execute(sql15)
						results=cursor1.fetchall()
						if len(results) == 0:
							cursor1.execute("select job_stream_id,job_group_id from edw_job_streams where job_stream_id like '" + jobStream + "' and active_ind='Y' and run_status='R' and workflow_type='SRC2STG'")
							results=cursor1.fetchall()
							if len(results) == 0:
								logdata(logFile,"Could not find suitable job with job_stream - " + jobStream + " for retrying..",1)
								continue
							else:
								for resjobstr in results:
									jobStreammstr=resjobstr[0]
									ijobGroup=resjobstr[1]
								jobStreamarr.append(jobStreammstr)
						else:
							for resultobjstr in results:
								jobStreammstr=resultobjstr[0]
								ijobGroup=resultobjstr[1]
							#sql16="select job_stream_id from edw_job_streams where job_stream_id like '" + jobStreammstr + "%' and run_status in ('P','R')"
							sql16="SELECT job_stream_id,job_group_id from EDW_JOB_STREAMS where (JOB_STREAM_ID like '"   + jobStreammstr + "' and RUN_STATUS in ('P','R'))"
							sql16=sql16 + " or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and RUN_STATUS in ('P','R')) and workflow_type='SRC2STG' and active_ind='Y'"
							cursor1.execute(sql16)
							results=cursor1.fetchall()
							for resobjarr in results:
								jobStreamarr.append(resobjarr[0])
								ijobGroup=resobjarr[1]
				except Exception as e:
					colval="Error occurred while selecting JCT for retrying."
					updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
					updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
					logdata(errFile,colval,1)
					logdata(errFile,sql14,0)
					#print ("Error occurred while inserting sqls into JCT")
					logdata(errFile,str(e),1)
					Subject ="Error: Error occurred while selecting JCT for retrying."
					errMsg=Subject + "\n\n" + str(e)
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
				try:
					## Incase of retry and it is split and load, split column and its data type will be queried from DY_MASTER table. 
					cursor1.execute("select src_rows,substr(split_column,instr(split_column,'~')+1) split_col_data_type,substr(split_column,1,instr(split_column,'~')-1) split_column from diy_master where reqid=" + str(reqid))
					results=cursor1.fetchall()
					for rowObj in results:
						srcNumrows=rowObj[0]
						surcIndColtype=rowObj[1]
						split_column=rowObj[2]
					if surcIndColtype in ['TS','TZ','DA']:
						cursor2.execute("LOCK ROW FOR ACCESS  select regexp_replace(regexp_replace(substr(trim(columnformat),1,19),'DDHH','DD HH24'),'DDBHH','DD HH24')  col_format from dbc.columnsv where databasename='" + srcBaseschema + "' and tablename='" + srcBasetable + "' and columnname='" + split_column + "'")
						results=cursor2.fetchall()
						for results_obj in results:
							surcIndColFormat=results_obj[0]
					if srcNumrows is None:
						raise Exception("Could not get source count during job retry")
				
				except Exception as e:
					logdata(logFile,"Could not get source count during job retry",1)
			## End of indentation for retryflag=True
			if len(incColname) == 0:
				logdata(logFile,"Since no incremental column is passed " + sourceTable + " table will undergo full load every time",1)
			else:
				## If it is incremental load then pull the max value at present in incremental column and store the same in DIY_MASTER (attribute6 column)
				## If it is retry then incremental column value pulled at first run will be re-used  as load will be based on the same. 
				## This will be updated in JCT table at end of the load process.
				logdata(logFile,"Checking if Incremental column to be pulled",0)
				cursor1.execute(f"select nvl(attribute6,'-1') from diy_master where reqid={reqid}")
				results=cursor1.fetchall()
				for incr_value_obj in results:
					incRetcd=incr_value_obj[0]
				if str(incRetcd) == '-1':
					logdata(logFile,"Looks like this is first run. So pulling incremental data",0)
					if sourcedbType.upper() == 'TERADATA':
						incRetcd=updTdInccol(srcBaseschema,srcBasetable,cursor2,jobStream,incColname,cursor1,logFile,errFile)
						if conn2:
							conn2.close()
					elif sourcedbType.upper() == 'ORACLE':
						if part_name == -1:
							src_tbl_2_query=f"{sourceSchema}.{sourceTable}"
						else:
							src_tbl_2_query=f"{sourceSchema}.{sourceTable} PARTITION({part_name})"
						incRetcd=updOracleInccol(sourceSchema,sourceTable,cursor2,jobStream,incColname,cursor1,logFile,errFile,src_tbl_2_query)
						if conn2:
							conn2.close()
					elif sourcedbType.upper() == 'HANA':
						incRetcd=updHanaInccol(sourceSchema,sourceTable,cursor2,jobStream,incColname,cursor1,logFile,errFile,obj_type)
						if conn2:
							conn2.close()
					cursor1.execute(f"update diy_master set attribute6='{incRetcd}' where reqid={reqid}")
					cursor1.execute("commit")
				else:
					logdata(logFile,"Looks like this is re-run. So reusing existing incremental data ",0)
			## Change the phase to Extract phase and invoke respective extract module to extract data from source into flat files.
			updDIYmstr(reqid,'current_phase','Launch_Extract',cursor1,errFile,logFile,emailAdd)
			if splitcount > 1:
				insDIYlines(reqid,'Split_Phase','Launch_Extract',cursor1,errFile,emailAdd)
			else:
				insDIYlines(reqid,'Invoking_Data_Load','Launch_Extract',cursor1,errFile,emailAdd)
			if len(jobStreamarr) > 1:
				if Thread_count > len(jobStreamarr):
					thread4upd=len(jobStreamarr)
					Thread_count=len(jobStreamarr)
				else:
					thread4upd=Thread_count
			else:
				thread4upd=1
			if retryFlag == 'FALSE':
				updDIYmstr(reqid,'Thread_count',str(thread4upd),cursor1,errFile,logFile,emailAdd)
				updDIYmstr(reqid,'split_count',str(split4upd),cursor1,errFile,logFile,emailAdd)
			else:
				if tCnt != Thread_count and thread4upd > 1:
					updDIYmstr(reqid,'Thread_count',str(tCnt),cursor1,errFile,logFile,emailAdd)
			invokeExtract(envType,Data_Ext_Scr,reqid,ijobGroup,jobStreamarr,jobStreammstr,LOGS_DIR,logFile,Thread_count,tblSize,cursor1,sourceSchema,sourceTable,srcBasetable,srcBaseschema,sourcedbType)
			logdata(logFile,"Done with " + str(i+1) + " record.. Proceeding to next ..",0)
			logdata(logFile,"-----------------------------------------------------------------------------------------------------------------------------------------",0)
			## Check for completeness
			logdata(logFile,"Check for successfull ingestion for jobstream - " + jobStreammstr,0)
			if retryFlag == 'FALSE':
				noPieces=len(jobStreamarr)
			else:
				try:
					#cursor1.execute("select max(job_stream_seq_num)+1 from edw_job_streams where job_stream_id like '" + jobStream +"%'")
					cursor1.execute("SELECT max(job_stream_seq_num)+1 from EDW_JOB_STREAMS where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and workflow_type='SRC2STG'  and active_ind='Y'")
					results=cursor1.fetchall()
					for nopiecur in results:
						noPieces=nopiecur[0]
					if len(results) == 0 or noPieces is None :
						noPieces=1
			
				except Exception as e:
					colval="Error occurred while pulling piece count for merging.. Please check and merge manually."
					updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
					updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
					logdata(errFile,colval,1)
					logdata(errFile,str(e),1)
					Subject ="Error: Error occurred while pulling piece count for merging.. Please check and merge manually."
					errMsg=Subject
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					continue
			try:
				#if len(jobStreamarr) > 1: ## 5.6 change
				## Incase of split and load check if all child threads are completed.  and if yes then mark the master JCT's run status as completed.
				## If not retry the failed child threads alone once again to see if it can be completed. If not abort the load and send appropriate communication. 
				if noPieces > 1: 
					sql16="select job_stream_id from edw_job_streams where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and run_status in ('P','R') and workflow_type='SRC2STG' and active_ind='Y'"
					cursor1.execute(sql16)
					results=cursor1.fetchall()
					if len(results) == 0:
						try:
							logdata(logFile,"Successful completion of all child threads for " + sourceSchema + "." + sourceTable,1)
							cursor1.execute("Update edw_job_streams set run_status='C' where job_stream_id='" + jobStream + "' and run_status='A' and workflow_type='SRC2STG'")
							cursor1.execute("commit")
							SuccFlag=True
						except Exception as e:
							logdata(errFile,"Could not update master record for jobstream " + jobStreammstr + " though all child threads completed. Please check and update manually",1)
							logdata(errFile,str(e),1)
							Subject ="Error: Could not update master record for jobstream " + jobStreammstr + " though all child threads completed. Please check and update manually"
							errMsg=Subject + "\n\n" + str(e)
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
					else:
						logdata(logFile,"Some of the child thread for jobstream " + jobStreammstr + " is not in completed status.. Hence retrying the same now..",1)
						newjobstreamArr=[]
						for newresultsobj in results:
							newjobstreamArr.append(newresultsobj[0])
						invokeExtract(envType,Data_Ext_Scr,reqid,ijobGroup,newjobstreamArr,jobStreammstr,LOGS_DIR,logFile,Thread_count,tblSize,cursor1,sourceSchema,sourceTable,srcBasetable,srcBaseschema,sourcedbType)
						logdata(logFile,"Checking for successful ingestion post retry for jobstream - " + jobStreammstr,0)
						cursor1.execute(sql16)
						results=cursor1.fetchall()
						if len(results) == 0:
							try:
								logdata(logFile,"Successful completion of all child threads for " + sourceSchema + "." + sourceTable,1)
								cursor1.execute("Update edw_job_streams set run_status='C' where job_stream_id='" + jobStream + "' and run_status='A' and workflow_type='SRC2STG'")
								cursor1.execute("commit")
								SuccFlag=True
							except Exception as e:
								logdata(errFile,"Could not update master record post retry for jobstream  " + jobStreammstr + " though all child threads completed. Please check and update manually",1)
								logdata(errFile,str(e),1)
								Subject ="Error: Could not update master record post retry for jobstream  " + jobStreammstr + " though all child threads completed. Please check and update manually"
								errMsg=Subject + "\n\n" + str(e)
								sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
						else:
							logdata(errFile,"Some of the child thread for jobstream " + jobStreammstr + " is not in compelted status even after retrying.. Hence giving up now..",1)
							Subject ="Error: Some of the child thread for jobstream " + jobStreammstr + " is not in compelted status even after retrying.. Hence giving up now.."
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				else:
					## Its single stream ingestion
					## If it is single stream and still failed, again attempt to reprocess once and if it fails give up. 
					sql16="select job_stream_id from edw_job_streams where job_stream_id like '" + jobStreammstr + "' and run_status in ('P','R','A') and workflow_type='SRC2STG'"
					cursor1.execute(sql16)
					results=cursor1.fetchall()
					if len(results) == 0:
						logdata(logFile,"Successful completion of data ingestion for " + sourceSchema + "." + sourceTable,1)
						SuccFlag=True
					else:
						logdata(logFile,"Jobstream " + jobStreammstr + " is not in compelted status.. Hence retrying the same now..",1)
						invokeExtract(envType,Data_Ext_Scr,reqid,ijobGroup,jobStreamarr,jobStreammstr,LOGS_DIR,logFile,Thread_count,tblSize,cursor1,sourceSchema,sourceTable,srcBasetable,srcBaseschema,sourcedbType)
						logdata(logFile,"Checking for successful ingestion post retry for jobstream - " + jobStreammstr,0)
						cursor1.execute(sql16)
						results=cursor1.fetchall()
						if len(results) == 0:
							logdata(logFile,"Successful completion of data ingestion for " + sourceSchema + "." + sourceTable,1)
							SuccFlag=True
						else:
							logdata(errFile,"Ingestion for jobstream " + jobStreammstr + " is not in completed status even after retrying.. Hence giving up now..",1)
							Subject ="Error: Some of the child thread for jobstream " + jobStreammstr + " is not in compelted status even after retrying.. Hence giving up now.."
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			except Exception as e:
				logdata(errFile,"Could not query jobstream " + jobStreammstr + " and child for completeness",1)
				logdata(errFile,str(e),1)
				Subject ="Error: Could not query jobstream " + jobStreammstr + " and child for completeness"
				errMsg=Subject
				sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
			if SuccFlag:
				## Beginning of Change 4.60 Done to pass base schema/table to SS step 
				logdata(logFile,"Updating JCT Description for handling Merge from view",0)
				## If it is succesfull run and if base object is different from requested object(applicable only incase of views) base object details are captured in edw_job_streams table for future use.
				sql13="update edw_job_streams set job_stream_description='srcBaseschema=" + srcBaseschema + ",srcBasetable=" + srcBasetable + "' where job_stream_id="
				sql13 += "'" + jobStream + "'"
				cursor1.execute(sql13) 
				cursor1.execute("commit")
				## END of Change 4.60 Done to pass base schema/table to SS step 
				## Merge step is triggered to merge all child tables into single stage table 
				updDIYmstr(reqid,'current_phase','Start_SF_Merge',cursor1,errFile,logFile,emailAdd)
				insDIYlines(reqid,'Launch_Extract','Start_SF_Merge',cursor1,errFile,emailAdd)
				logdata(logFile,"Since Data ingestion to SF completed succesfully now proceeding with stage merge and/or rowcount validation process",1)
				if sourcedbType != 'SNOWFLAKE':
					sfRowcnt=mergeSFstgtable(targetDb,targetSchema,targetTable,noPieces,logFile,errFile,envType,srcNumrows,cursor1,emailAdd,sfRole,sfWh,reqid,surcIndColtype,surcIndColFormat,sf_user,jobStream)
					updDIYmstr(reqid,'tgt_rows',str(sfRowcnt),cursor1,errFile,logFile,emailAdd)
					if sfRowcnt == -1:
						logdata(errFile,"Issue encountered while merging and/or checking rowcount in snowflake.. Please check and take care",1)
						errMsg="Issue encountered while merging and/or checking rowcount in snowflake.. Please check and take care"
						Subject="Error: Issue encountered while merging and/or checking rowcount in snowflake.. Please check and take care"
						SuccFlag=False
					else:
						if sfRowcnt >= srcNumrows:
							logdata(logFile,"Succesfully validated SF ingestion. Source had " + str(srcNumrows) + " rows and snowflake has " + str(sfRowcnt) + " rows.",1)
							Subject = "Success: Succesfully completed ingesting data into " + targetDb + "." + targetSchema + "." + targetTable
							errMsg = "Succesfully validated SF ingestion. Source had " + str(srcNumrows) + " rows and snowflake has " + str(sfRowcnt) + " rows. Please review attached logs for details."
						else:
							logdata(errFile,"Row count mismatch between source and target. Source had " + str(srcNumrows) + " rows and snowflake has " + str(sfRowcnt) + " rows.",1)
							Subject = "Warning: Ingestion completed for " + targetDb + "." + targetSchema + "." + targetTable + ". However rowcount mismatch exists. Please check and take action."
							errMsg = "Row count mismatch between source and target. Source had " + str(srcNumrows) + " rows and snowflake has " + str(sfRowcnt) + " rows."
			if SuccFlag:
				try:
					logdata(logFile,"Now resetting job group and setting active ind to N for master job stream",1)
					#if eType == 'ALL_DATA' and existFlag == 'FALSE':
					#	sql="update edw_job_streams set job_group_id='" + jobGroup + "',active_ind='N' where job_stream_id='" + jobStream + "'"
					#else:
					sql="update edw_job_streams set job_group_id='" + jobGroup + "' where job_stream_id='" + jobStream + "'"
					cursor1.execute(sql)
					cursor1.execute('commit')
				except Exception as e:
					colval="Could not reset jobgroup for jobstream " + jobStream + ". Please check and update."
					updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
					updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
					logdata(errFile,colval,1)							
					logdata(errFile,str(e),1)
					continue
				## If it is incremental load then unique key will be queried from data catalog. If stg2br load is MERGE type and unique key found then incremental column
				## value gets updated in EDW_JOB_STREAMS table for future run.
				if len(incColname) > 0:
					logdata(logFile,"Checking for existince of unique key",0)
					if sourcedbType != 'SNOWFLAKE':
						sql=f"select count(*) cnt from EDS_DATA_CATALOG.EDW_UNIQUE_KEY where db_instance_name='{sourceDb}' and db_schema_name='{sourceSchema}'"
						sql+=f" and table_name='{sourceTable}' and environment_name='{envType}' and active_flag='A'"
					else:
						sql=f"select target_db_name,target_schema,target_table_name from edw_snowflake_stream_link where source_db_name='{targetDb}'"
						sql+=f" and source_schema='{targetSchema}' and source_table_name='{targetTable}'"
						results=cursor1.execute(sql)
						for dsrcobj in results:
							dtargetDb=dsrcobj[0]
							dtargetSchema=dsrcobj[1]
							dtargetTable=dsrcobj[2]
						sql=f"select count(*) cnt from EDS_DATA_CATALOG.EDW_UNIQUE_KEY where db_instance_name='{dtargetDb}' and db_schema_name='{dtargetSchema}'"
						sql+=f" and table_name='{dtargetTable}' and environment_name='{envType}' and active_flag='A'"
					results=cursor1.execute(sql)
					for unique_cnt_obj in results:
						unique_key_cnt=unique_cnt_obj[0]
					if mType == 'APPEND':
						unique_key_cnt=1
					## Incremental value is updated both on child (only for reference) and parent thread(for future runs). 
					## EDW_JOB_STREAMS has a column called BACK_TIME_SEC. This column stores a integer which represents number of seconds for date type incremental load
					## and absolute value for id type incremental load. Framework will look back this value for future runs. 
					if unique_key_cnt > 0 and (len(whereClause) == 0 or (ltype == "Filtered_DATE" or ltype == "Filtered_ID")):
						if sourcedbType != 'SNOWFLAKE':
							logdata(logFile,f"Unique key found for jobstream {jobStream}. Hence proceeding with update for subsequent loads",0)
							results=cursor1.execute("select to_extract_dtm,to_extract_id from edw_job_streams where job_stream_id='" + jobStream + "' and workflow_type='SRC2STG'")
							for lastExtobj in results:
								lastExtdtm=lastExtobj[0]
								lastExtid=lastExtobj[1]
							if (lastExtdtm is None) and (lastExtid is None):
								if incRetcd == -1:
									logdata(errFile,"Exception occurred while pulling value for incremental column update.. So check and update manually",1)
									#continue
								else:
									try:
										intdType=incRetcd.split('~')[0]
										intValue=incRetcd.split('~')[1]
										logdata(logFile,"Now updating current incremental column details for subsequent loads",1)
										if intdType == 'DATE':
											if len(str(intValue)) > 21 and '.' in str(intValue):
												intValue=str(intValue).split('.')[0]
											updSql="update  edw_job_streams set from_extract_dtm=to_date('" + str(intValue) + "','yyyy-mm-dd hh24:mi:ss'),to_extract_dtm=to_date('" 
											updSql=updSql +  str(intValue) + "','yyyy-mm-dd hh24:mi:ss')-(nvl(back_time_sec,0)/(24*3600)), extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
											updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
										elif intdType == 'ID':
											updSql="update  edw_job_streams set from_extract_id=" + str(intValue) + ",to_extract_id=" + str(intValue) + "-to_number(nvl(back_time_sec,0))"
											updSql=updSql + ", extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
											updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
										else:
											logdata(errFile,"Could not update incremental data for jobstream " + jobStream + " post ingestion.",1)
											logdata(errFile,str(e),1)
											#continue
										cursor1.execute(updSql)
										cursor1.execute("commit")
									except Exception as e:
										colval="Could not update incremental data for jobstream " + jobStream + ". Please check and update."
										updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
										updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
										logdata(errFile,colval,1)							
										logdata(errFile,str(e),1)
										#continue
							else:
								if incRetcd == -1:
									logdata(errFile,"Exception occurred while pulling value for incremental column update.. So check and update manually",1)
									#continue
								else:
									try:
										intdType=incRetcd.split('~')[0]
										intValue=incRetcd.split('~')[1]
										logdata(logFile,"Now updating previous and current incremental column details for subsequent loads",1)
										if intdType == 'DATE':
											if len(str(intValue)) > 21 and '.' in str(intValue):
												intValue=str(intValue).split('.')[0]
											if lastUpdColFlag:
												updSql="update  edw_job_streams set PREVIOUS_FROM_EXTRACT_DTM=FROM_EXTRACT_DTM,PREVIOUS_TO_EXTRACT_DTM=TO_EXTRACT_DTM"
												updSql=updSql + ",from_extract_dtm=TO_EXTRACT_DTM,to_extract_dtm=to_date('" + str(intValue) + "','MM/DD/YYYY') "
												updSql=updSql + ", extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
												updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
											else:
												updSql="update  edw_job_streams set PREVIOUS_FROM_EXTRACT_DTM=FROM_EXTRACT_DTM,PREVIOUS_TO_EXTRACT_DTM=TO_EXTRACT_DTM"
												updSql=updSql + ",from_extract_dtm=TO_EXTRACT_DTM,to_extract_dtm=to_date('" + str(intValue) + "','yyyy-mm-dd hh24:mi:ss')-(nvl(back_time_sec,0)/(24*3600)) "
												updSql=updSql + ", extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
												updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
										elif intdType == 'ID':
											updSql="update  edw_job_streams set PREVIOUS_FROM_EXTRACT_ID=FROM_EXTRACT_ID,PREVIOUS_TO_EXTRACT_ID=TO_EXTRACT_ID"
											updSql=updSql + ",from_extract_id=TO_EXTRACT_ID,to_extract_id="+ str(intValue) + "-to_number(nvl(back_time_sec,0))"
											updSql=updSql + ", extract_type='" + intdType + "' where (JOB_STREAM_ID like '" + jobStream + "')"
											updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
										else:
											logdata(errFile,"Could not update incremental data for jobstream " + jobStream + " post ingestion.",1)
											logdata(errFile,str(e),1)
											#continue
										cursor1.execute(updSql)
										cursor1.execute("commit")
									except Exception as e:
										colval="Could not update incremental data for jobstream " + jobStream + ". Please check and update."
										updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
										updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
										logdata(errFile,colval,1)							
										logdata(errFile,str(e),1)
						else:
							logdata(logFile,"Now switching extract type for subsequent loads",1)
							if len(incColname) > 0:
								updSql="update  edw_job_streams set extract_type='STREAM' where (JOB_STREAM_ID like '" + jobStream + "')"
								updSql=updSql + "or (regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])')) and workflow_type='SRC2STG'"
								cursor1.execute(updSql)
								cursor1.execute("commit")
							else:
								logdata(logFile,"For stream based load recursive full data isnt allowed.. Hence disabling JCT status",1)
								updSql=f"update edw_job_streams set active_ind='N' where JOB_STREAM_ID='{jobStream}'"
								cursor1.execute(updSql)
								cursor1.execute("commit")
					else:
						if len(whereClause) == 0:
							logdata(logFile,f"Unique key not found for jobstream {jobStream}. Hence updating extract type as MERGE_KEY_NOT_FOUND",0)
							MERGE_KEY_NOT_FOUND=True
							Subject=f"Unique key not found for {jobStream}"
							errMsg=Subject
							sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
				## Now since data has been loaded succesfully from source to Stage, STG2BR process gets triggered to push the data from stage to final layer.
				br_view_recompile_flag = False
				if destSchema is not None:
					if destSchema == 'BR' and envType == 'PRD':
						sql=f"select job_stream_id from edw_job_streams where job_stream_Id_link='{jobStream}' and active_ind='Y'"
						cursor1.execute(sql)
						results=cursor1.fetchall()
						for results_obj in results:
							br_job_stream=results_obj[0]
						if len(results) > 0:
							sql=f"select target_db_name,target_schema,target_table_name from edw_job_streams where job_stream_id='{br_job_stream}' and active_ind='Y'"
							cursor1.execute(sql)
							results=cursor1.fetchall()
							for results_obj in results:
								target_db=results_obj[0]
								target_schema=results_obj[1]
								target_table=results_obj[2]
							if len(results) > 0:
								logdata(logFile,"Recompile view with PRE refresh data",0)
								if target_schema == 'BR':
									conn3=connect_snowflake(targetDb,targetSchema,logFile,errFile,envType,cursor1,emailAdd,sfRole,sfWh,sf_user)
									if type(conn3) == int:
										raise Exception("Exception encountered while establing SF connection. Please check")
									cursor3=conn3.cursor()
									#sql=f"call EDW_ADMIN_BR_DB.BR.edw_recomp_br_view ('{target_db}','BR', '{target_table}' ,to_char(current_timestamp))"
									sql=f"call EDW_ADMIN_BR_DB.BR.EDW_RECOMP_BR_VIEW_WITH_MODE ('{target_db}','{jobGroup}', '{target_table}', 'PRE')"
									compile_result=""
									cursor3.execute(sql)
									results=cursor3.fetchall()
									for obj in results:
										compile_result=obj[0]
									if "Succeeded Merge" not in compile_result:
										raise Exception(f"Exception - {compile_result} encountered while recompiling PRE BR view.. Please check ")
									else:
										br_view_recompile_flag = True
							else:
								logdata(logFile,f"Could not fetch target schema/tbl details from jct - {br_job_stream}",0)
						else:
							logdata(logFile,f"Could not locate br jobstream through sql {sql}",0)
					else:
						logdata(logFile,"Desitnation schema is not BR and so not proceeding with view recompilation",0)
					try:
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'current_phase','Launch_STG2BR',cursor1,errFile,logFile,emailAdd)
						insDIYlines(reqid,'Start_SF_Merge','Launch_STG2BR',cursor1,errFile,emailAdd)
						sql=f"select batch_id,SRC2STG_FF_PATH from diy_job_params where reqid={reqid}"
						cursor1.execute(sql)
						results=cursor1.fetchall()
						for obj in results:
							E_BATCH_ID=obj[0]
							E_PATH=obj[1]
						os.environ['BATCH_ID']=str(E_BATCH_ID)
						os.environ['SRC2STG_FF_PATH']=E_PATH
						logdata(logFile,"Now launching stg2br script to push data from stg to ss/br" + "DIY_stg2br.py -e " + envType + " -j " + jobStream + " -a " + emailAdd + " -d " + destSchema + " -m " + mType ,1)
						cmnd=subprocess.Popen("/apps/edwsfdata/python/scripts/DIY_stg2br.py -e " + envType + " -j " + jobStream + " -a " + emailAdd + " -d " + destSchema + " -m " + mType,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
						pid=cmnd.pid
						logdata(logFile,f"stg2br script launched with pid {pid}",1)
						(output, error) = cmnd.communicate()
						Err=error.decode('ascii')
						Out=output.decode('ascii')
						if Err is not None and len(Err)>0:
							logdata(errFile,"Exception encountered while launching stg2br. Please find below the exception \n" + Err,0)
							print(Err)
						else:
							logdata(logFile,"Completed launching stg2br. Please find below the output \n" + Out,0)
							print(Out)
						#logdata(logFile,"Completed launching stg2br. Please find below the output",1)
						#print(stg2brout)
						if "Error occurred " in Out:
							stg2brFail=True
						else:
							stg2brFail=False
					except Exception as e:
						logdata(errFile,"Below exception occurred while launching stg2br for jobstream - " + jobStream,1)
						logdata(errFile,str(e),1)

				## Re-initialize job groups
				try:
					logdata(logFile,"Now re-initializing master job stream and deactivating child job streams for subsequent runs",1)
					#cursor1.execute("update edw_job_streams set run_status='P' where job_stream_id='" + jobStream + "'")
					cursor1.execute("update edw_job_streams set active_ind='N' where run_status='C' and regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and workflow_type='SRC2STG'")
					#if len(splitQrydict) > 1:
					if len(jobStreamarr) > 1:
						updIncqry="update edw_job_streams set (previous_from_extract_dtm,from_extract_dtm,previous_to_extract_dtm,to_extract_dtm,previous_from_extract_id,from_extract_id,previous_to_extract_id,to_extract_id)="
						updIncqry+="(select previous_from_extract_dtm,from_extract_dtm,previous_to_extract_dtm,to_extract_dtm,previous_from_extract_id,from_extract_id,previous_to_extract_id,to_extract_id from edw_job_streams where regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and run_status='P' and active_ind='Y') where workflow_type='SRC2STG'"
						updIncqry+=" and active_ind='N' and regexp_like (JOB_STREAM_ID,'^" + jobStream + "_([0-9]{14}.[0-9])') and workflow_type='SRC2STG'"
						cursor1.execute(updIncqry)
					## below update statement to handle retried jct where master remains in "A" status 
					cursor1.execute(f"update edw_job_streams set run_status='C' where job_stream_id='{jobStream}' and run_status='A'") 
					cursor1.execute("commit")
					## below update statement is to disable STG2BR JCT if merge key not found
					if MERGE_KEY_NOT_FOUND:
						updSql=f"update edw_job_streams set extract_type='MERGE_KEY_NOT_FOUND',active_ind='N' where JOB_STREAM_ID='{jobStream}'"
						cursor1.execute(updSql)
						upd_statement=f"update edw_job_streams set active_ind='N' where job_stream_id in (select job_stream_id from edw_job_streams where job_stream_Id_link='{jobStream}')"
						cursor1.execute(upd_statement)
						cursor1.execute("commit")

				except Exception as e:
					colval="Could not re-initialize jobstream " + jobStream + " for subsequent runs. Please check and update."
					updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
					updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
					logdata(errFile,colval,1)
					logdata(errFile,str(e),1)
					continue
				if disableJct:
					logdata(logFile,f"Disabling JCT to prevent future runs",0)
					upd_statement=f"update edw_job_streams set active_ind='N',extract_type='{ext_type}' where job_stream_id = '{jobStream}'"
					cursor1.execute(upd_statement)
					upd_statement=f"update edw_job_streams set active_ind='N',extract_type='{ext_type}' where job_stream_id in (select job_stream_id from edw_job_streams where job_stream_Id_link='{jobStream}')"
					cursor1.execute(upd_statement)
					cursor1.execute("commit")
					Subject=f"Incorrect incremental column setup for {jobStream}"
					errMsg=Subject
					sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)

				## Upon succesfull completion of STG2BR, if it is production load and soft delete is enabled then trigger soft delete process. 
				if not stg2brFail and envType == 'PRD':
					try:
						del_enable_count=0
						sql=f"select count(1) from edw_job_streams where job_stream_id='{jobStream}' and (extract_type <> 'ALL_DATA' or source_db_connection='HANANRT_PRD') and source_deleted_flag='A' and active_ind='Y'"
						cursor1.execute(sql)
						results=cursor1.fetchall()
						for del_flag_obj in results:
							del_enable_count=del_flag_obj[0]
						if del_enable_count == 1 and (eType != 'ALL_DATA' or sourcedbType.upper() == 'HANANRT'):
							source_delete='PASS'
							logdata(logFile,f"Source delete enabled for job stream {jobStream}. Hence proceeding with same",0)
							updDIYmstr(reqid,'current_phase','Launch_SoftDelete',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Launch_STG2BR','Launch_SoftDelete',cursor1,errFile,emailAdd)
							if "Filtered" not in ltype and aud_num_rows is not None and aud_num_rows > 0:
								logdata(logFile,f"Calling soft delete with source count as {aud_num_rows}",0)
								cmnd=subprocess.Popen(f"/apps/edwsfdata/python/scripts/DIY_soft_delete.py -e {envType} -l {LOGS_DIR} -j {jobStream} -r {reqid} -c {aud_num_rows}" ,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
							else:
								cmnd=subprocess.Popen(f"/apps/edwsfdata/python/scripts/DIY_soft_delete.py -e {envType} -l {LOGS_DIR} -j {jobStream} -r {reqid}" ,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
							pid=cmnd.pid
							logdata(logFile,f"Soft delete script launched with pid {pid}",1)
							(output, error) = cmnd.communicate()
							Err=error.decode('ascii')
							Out=output.decode('ascii')
							if (Err is not None and len(Err)>0) or ('exception occurred' in Out):
								logdata(errFile,"Exception encountered while launching soft deletes. Please find below the exception \n" + Err,0)
								print(Err)
								source_delete='FAIL'
							else:
								logdata(logFile,"Completed launching soft deletes. Please find below the output \n" + Out,0)
								print(Out)
								if "Error occurred " in Out:
									logdata(errFile,"Exception encountered while launching soft deletes. Please find below the exception \n" + Err,0)
									source_delete='FAIL'
							if source_delete == 'PASS':
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'current_phase','Success',cursor1,errFile,logFile,emailAdd)
								insDIYlines(reqid,'Launch_SoftDelete','Completed_Ingestion',cursor1,errFile,emailAdd)
							else:
								updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
								updDIYmstr(reqid,'current_phase','Softdelete_Failure',cursor1,errFile,logFile,emailAdd)
								insDIYlines(reqid,'Launch_SoftDelete','Completed_Ingestion',cursor1,errFile,emailAdd)
								sys.exit(99)
						else:
							logdata(logFile,f"Source delete disabled for for job stream {jobStream} or ingestion is of type {eType}. Hence not proceeding with same",0)
							updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
							updDIYmstr(reqid,'current_phase','Success',cursor1,errFile,logFile,emailAdd)
							insDIYlines(reqid,'Launch_STG2BR','Completed_Ingestion',cursor1,errFile,emailAdd)
						## If destination schema in BR and it is production load then BR_VIEW gets recompiled with latest timestamp. 
						if br_view_recompile_flag:
							logdata(logFile,"Recompile view with latest POST refresh data",0)
							conn3=connect_snowflake(targetDb,targetSchema,logFile,errFile,envType,cursor1,emailAdd,sfRole,sfWh,sf_user)
							if type(conn3) == int:
								raise Exception("Exception encountered while establing SF connection. Please check")
							cursor3=conn3.cursor()
							#sql=f"call EDW_ADMIN_BR_DB.BR.edw_recomp_br_view ('{target_db}','BR', '{target_table}' ,to_char(current_timestamp))"
							sql=f"call EDW_ADMIN_BR_DB.BR.EDW_RECOMP_BR_VIEW_WITH_MODE ('{target_db}','{jobGroup}', '{target_table}', 'POST')"
							compile_result=""
							cursor3.execute(sql)
							results=cursor3.fetchall()
							for obj in results:
								compile_result=obj[0]
							if "Succeeded Merge" not in compile_result:
								raise Exception(f"Exception - {compile_result} encountered while recompiling BR view.. Please check ")
					except Exception as e:
						colval=f"Exception encounterend while checking/launching source delete for jobstream {jobStream} "  
						updDIYmstr(reqid,'err_msg',colval + str(e),cursor1,errFile,logFile,emailAdd)
						updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
						logdata(errFile,colval,1)
						logdata(errFile,str(e),1)
						conn3.close()
						continue
				elif not stg2brFail:
					updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
					updDIYmstr(reqid,'current_phase','Success',cursor1,errFile,logFile,emailAdd)
					insDIYlines(reqid,'Launch_STG2BR','Completed_Ingestion',cursor1,errFile,emailAdd)
				else:
					updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
					updDIYmstr(reqid,'current_phase','SRC2STG_Success_But_STG2BR_Fail',cursor1,errFile,logFile,emailAdd)
					insDIYlines(reqid,'Launch_STG2BR','Completed_Ingestion',cursor1,errFile,emailAdd)
				if mailonSucc is not None:
					if mailonSucc.upper() == 'YES':
						sendEmail(emailAdd,logFile,errFile,errMsg,Subject,cursor1)
		except Exception as e:
			linenu=sys.exc_info()[-1].tb_lineno
			print(f"Exception - {e} occurred at line {linenu} while processing ingestion for jobstream {jobStream}")
			if conn is not None and reqid is not None:
				msg_2_update=str(e).replace("'","''")
				sql=f"update diy_master set err_msg=err_msg || '{msg_2_update}' where reqid={reqid}"
				execute_qry(conn, sql, reqid, conn)
				execute_qry(conn,"commit",reqid,conn)
			continue
			logdata(errFile,str(e),1)
		finally:
			updDIYmstr(reqid,'end_time','curr_date',cursor1,errFile,logFile,emailAdd)
	#End of For loop indentation
#End of For DB/Table based ingestion indentation
logdata(logFile,"Closing Connections",0)
if conn:
	conn.close()
if isinstance(conn3,snowflake.connector.connection.SnowflakeConnection):
	conn3.close()
logdata(logFile,"Done with ingestion",1)
logFile.close()
errFile.close()










