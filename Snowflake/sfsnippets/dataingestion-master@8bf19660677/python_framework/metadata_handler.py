"""
Methods:
	1. set_env()
	2. collect_property_file_contents(property_file, header)
	3. open_sf_connection(sf_account, user_name, warehouse=None, role=None, database=None, schema=None, query_tag=None)
	4. open_teradata_connection(dsn, req_id, repo_conn)
	5. close_connection(conn)
	6. execute_df_qry(conn, qry, req_id, repo_conn)
	7. execute_qry(conn, qry, req_id, repo_conn)
	8. debug_logger(conn, qry, results, req_id)
	9. send_email(email_add, log_file, err_file, err_msg, subject, repo_conn=None, req_id=None, wc_flag=False)
	10. validate_params(input_type, param_file, table_name, env_type, thread_cnt,
					job_stream_id, email_add, dest_schema, success_mail)
	11. create_dirs(job_group, env_type, job_group_init_scr, log_dir)
	12. collect_metadata(env_type, source_db, source_schema, source_table, log_file, err_file,
					 connection_name, log_dir)
	13. log_data(file_name, msg, print_flag)
	14. log_diymaster_error(req_id, msg,G e, repo_conn)
	15. update_diymaster(req_id, update_column, column_value, repo_conn, err_file, log_file, email_add)
	16. insert_diylines(req_id, pre_phase, curr_phase, repo_conn, err_file, email_add, log_file)
	17. check_result(inc_dtype, err_file)
	18. update_incr_column(source_schema, source_table, inc_col_name, log_file, err_file, conn, req_id, repo_conn)
	19. invoke_csv_download(env_type, scr_path, scr, job_group, job_stream_arr, log_dir, master_log_file,
						thread_count, table_size, conn, base_table, base_schema, req_id)
	20. validate_view(env_type, source_db, db_type, source_conn, source_schema, source_table, email_add, log_file,
				  err_file, repo_conn, req_id)
	21. validate_metadata(env_type, source_db, source_schema, source_table, conn, log_file, err_file, source_db_type,
					  connection_name, log_dir, req_id, repo_conn)
"""

import configparser
import os
import subprocess
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib,math
import decimal
from datetime import timedelta
from operator import itemgetter

import snowflake.connector
import hvac
import cx_Oracle
import teradata
import teradatasql
import time
import ast

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
from datetime import datetime

import json
import pdb

from datetime import timedelta

import sys
import pandas as pd

import re
import json

from  hdbcli import dbapi

import psutil

def set_env():
	"""
	Set the OS environments for LD_LIBRARY_PATH and ORACLE_HOME
	:return:
	"""
	os.environ['LD_LIBRARY_PATH'] = "/usr/cisco/packages/oracle/oracle-12.1.0.2/lib"
	os.environ['ORACLE_HOME'] = "/usr/cisco/packages/oracle/oracle-12.1.0.2"
	os.environ['ODBCINI']='/apps/edwsfdata/python/scripts/odbc/.odbc.ini'
	os.environ['TNS_ADMIN']="/apps/edwsfdata/python/network/admin"

def collect_property_file_contents(property_file, header):
	"""

	 This method will attempt to open and load the property file to a property file object)

	:param property_file: The file which contains all the properties to be loaded
	:param header: The Account instance. EG. ciscodev.us-east-1

	:return:
	"""

	def as_dict(config):
		"""
		Convert the config parser to a valid dictionary

		:param config:  this is the config parser object after called .read
		:return:
		"""
		d = dict(config._sections)
		for k in d:
			d[k] = dict(config._defaults, **d[k])
			d[k].pop('__name__', None)
		return d

	try:
		config_dict = {}
		with open(property_file, 'r') as fvar:
			config = configparser.ConfigParser()
			config.read(property_file)
			config_dict = as_dict(config)[header]

		return config_dict
	except Exception as e:
		print('ERROR: Unable to open and collect property file contents for (property file: '
			  + property_file + ')')
		print('ERROR: ' + str(e))
		exit(1)
	
def open_hana_connection(host,cport,cuser,password,addlnParam=None):
	try:
		if addlnParam is None:
			conn=dbapi.connect(address=host,port=int(cport),user=cuser,password=password)
		else:
			arg=addlnParam.split("=")[0]
			val=addlnParam.split("=")[1]
			if arg.lower() == 'databasename':
				conn=dbapi.connect(address=host,port=int(cport),user=cuser,password=password,databaseName=val)
			else:
				conn=dbapi.connect(address=host,port=int(cport),user=cuser,password=password,arg=val)
		return conn
	except Exception as e:
		raise Exception(f"Unable to establish Hana connection to user: {cuser}. {e}")

def open_sf_connection(sf_account, user_name, warehouse=None, role=None, database=None, schema=None, query_tag=None):
	"""
	Establish a database connection. Assumes Snowflake.

	Expectations in config file :
		[CISCOSTAGE.US-EAST-1]
		TOKEN.EDW_HR_HRR_ETL_SVC=s.dsfdfrferfe.VTsyF
		SFKEYPATH=secret/snowflake/stg/{}/key
		SFKEEPURL=https://alphaeast.keeper.cisco.com
		SFKEEPNAME=cloudDB
		CONNECTOR.EDW_HR_HRR_ETL_SVC={'warehouse':'EDW_DATALAKE_WH', 'role':'EDW_DATALAKE_ROLE'}

	CONNECTOR.EDW_HR_HRR_ETL_SVC can include database and schema

	:param sf_account: the specific SF account
	:param user_name: the specific user_name in sf you want to connect too
	Optional : warehouse=None, role=None, database=None, schema=None
	:return: The database connection object
	"""
	# Connect to Keeper to collect secrets
	step_name='Inside establishing connection'
	config_properties = collect_property_file_contents('/apps/edwsfdata/python/config/ejc.conf', sf_account.upper())

	token_keys = [k for k,v in config_properties.items() if 'TOKEN' in k.upper()]
	if len(token_keys) == 0:
		raise Exception(f"No tokens found in config files for {sf_account}")

	user_name_key = None

	for key in token_keys:
		vals = key.split('.')
		if len(vals) != 2:
			continue
		if vals[1].upper() == user_name.upper():
			user_name_key = key
	try:
		if user_name_key is None:
			raise Exception(f"No tokens found for user {user_name} in config files for {sf_account}")
		attempts=0
		succ=True
		while succ:
			try:
				client = hvac.Client(
				url=config_properties['sfkeepurl'],
				namespace=config_properties['sfkeepname'],
				token=config_properties[user_name_key]
				)
				# Secrets are stored within the key entitled 'data'
				keeper_secrets = client.read(
					config_properties['sfkeypath'].format(user_name.lower()))['data']
				passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
				private_key = keeper_secrets['private_key']

				# PEM key must be byte encoded
				key = bytes(private_key, 'utf-8')

				p_key = serialization.load_pem_private_key(
					   key, password=passphrase.encode(), backend=default_backend()
				)

				pkb = p_key.private_bytes(
					   encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8,
					   encryption_algorithm=serialization.NoEncryption())

				connector = ast.literal_eval(config_properties[f'connector.{user_name.lower()}'])
				step_name="Establishing connection"

				conn = snowflake.connector.connect(
					   user=user_name.upper(), account=config_properties['sfacc'], private_key=pkb)

				succ=False
			except Exception as e:
				attempts+=1
				messg=f"Snowflake connection on attempt {attempts} failed with exception - {e}"
				#log_data(log_file,messg,0)
				time.sleep(10)
				if attempts == 5:
					succ=False
					raise Exception (f"Snowflake connection failed even after 5 attempts")
				pass

		if role is None:
			step_name="Setting default role"
			conn.cursor().execute(f"USE ROLE {connector['role']}")
		else:
			step_name="setting passed role"
			conn.cursor().execute(f"USE ROLE {role}")
		if warehouse is None:
			step_name="setting default warehouse"
			conn.cursor().execute(f"USE WAREHOUSE {connector['warehouse']}")
		else:
			step_name="setting passed warehouse"
			conn.cursor().execute(f"USE WAREHOUSE {warehouse}")
		if database is None:
			if 'database' in connector.keys():
				step_name="setting default db"
				conn.cursor().execute(f"USE DATABASE {connector['database']}")
				if 'schema' in connector.keys():
					setp_name="setting default schema"
					conn.cursor().execute(f"USE SCHEMA {connector['SCHEMA']}")
		else:
			step_name="setting passed db"
			conn.cursor().execute(f"USE DATABASE {database}")
			if schema is not None:
				step_name="setting passed schema"
				conn.cursor().execute(f"USE SCHEMA {schema}")
		if query_tag is None:
			ppid=os.getpid()
			p=psutil.Process(ppid)
			out_dict=p.as_dict(attrs=['pid', 'name', 'username'])
			query_tag=f"PID-{out_dict['pid']};PROGRAM-{out_dict['name']};USER-{out_dict['username']}"
			conn.cursor().execute(f"alter session set query_tag='{query_tag}'")
		else:
			conn.cursor().execute(f"alter session set query_tag='{query_tag}'")
		return conn
	except Exception as e:
		raise Exception(f"Unable to establish connection to user: {user_name} in {sf_account}. Connection failed at step {step_name} - {str(e)}")


def open_oracle_connection(db_type=None, config_properties=None, encoding=False):
	"""
	Establish a database connection with Oracle

	:param db_type: This is based on repository or source info (DV1, TS3)
	:param config_properties: For Source data this will be a dictionary
	:param encoding: Default false for encoding to UTF-16
	:return: The database connection object
	"""
	# if oracle is the source then db_type can be the
	#pdb.set_trace()
	set_env()

	if db_type is not None and config_properties is None:
		config_properties = collect_property_file_contents('/apps/edwsfdata/python/config/ejc.conf', db_type)

	try:
		dsn_tns = cx_Oracle.makedsn(config_properties['ejc_host'], config_properties['ejc_port'],
									service_name=config_properties['ejc_servicename'])  # build connection string
		if encoding:
			conn = cx_Oracle.connect(config_properties['ejc_login'],
									 config_properties['ejc_password'], config_properties['ejc_servicename'], encoding="UTF-16",
									 nencoding="UTF-16")
		else:
			conn = cx_Oracle.connect(config_properties['ejc_login'],
									 config_properties['ejc_password'],config_properties['ejc_servicename'])

	except Exception:
		return -1

	return conn

def open_teradata_connection(dsn, req_id, repo_conn):
	"""
	Establish a database connection with Teradata

	:param dsn: This is the valid dsn to connect to teradata
	:param req_id: the request ID
	:param repo_conn: this is the repository connection (incase of failure)
	:return: The database connection object
	"""
	try:

		## Check filehandle on runNumber before attempting to write to avoid segmentation fault while connecting
		conn_cnt = 1
		while conn_cnt != 0:
			file_out = os.popen("lsof /apps/edwsfdata/python/pylogs/.runNumber").read()
			if len(file_out) == 0:
				uda_exec = teradata.UdaExec(appName="ModuleName", version="0.1", logConsole=False,
											odbcLibPath="/opt/teradata/client/ODBC_64/lib/libodbc.so",
											runNumberFile="/apps/edwsfdata/python/pylogs/.runNumber",
											logDir="/apps/edwsfdata/python/pylogs/logs")
				conn_cnt = 0
			else:
				time.sleep(1)

		conn = uda_exec.connect(method="odbc", DSN=dsn)

	except Exception as e:
		log_diymaster_error(req_id, f"Unable to connect to teradata DSN:  '{dsn}' ", e, repo_conn)
		return -1

	return conn

def close_connection(conn):
	"""
	Close a SF connection

	:param conn: this is the conn object when creating the connection
	"""
	if conn:
		conn.close()

def execute_df_qry(conn, qry, req_id, repo_conn):
	"""
	Execute any given query on any given connection
	:param conn: the conn (database) you would like to execute the query on
	:param qry: This is the qry to be executed
	:param req_id: the request ID
	:param repo_conn : this is the repository connection (incase of failure)
	:return: Given dataframe with output of requested query
	"""
	df_data = -1
	try:
		cursor = conn.cursor()
		results = cursor.execute(qry)
		if type(results) is  bool:
			description=cursor.description
			fetch_all = cursor.fetchall()
		else:
			description = results.description
			fetch_all = results.fetchall()
		columns = [row[0].lower() for row in description]
		df_data = pd.DataFrame(fetch_all, columns=columns)
		cursor.close()
		return df_data
	except Exception as e:
		#log_diymaster_error(req_id, f"Exception encountered while running df query:{qry} \n ", e, repo_conn)
		raise Exception(f"Exception encountered while running df query\n {qry} \n {e} \n ")
	finally:
		debug_logger(conn, qry, df_data, req_id)
	return df_data

def execute_qry(conn, qry, req_id, repo_conn):
	"""
	Execute given query that does not expect a return value

	:param conn: This is thev onnection to Oracle
	:param qry: This is the qry to be executed on Oracle
	:param req_id: the request ID
	:param repo_conn : this is the repository connection (incase of failure)
	:return output: This will be either 1(successful) or -1(unsuccessful)
	"""
	output = -1
	try:
		cursor = conn.cursor()
		val = cursor.execute(qry)
		cursor.close()
		output = 1
	except Exception as e:
		#log_diymaster_error(req_id, f"Exception encountered while running query:{qry} \n ", e , repo_conn)
		raise Exception(f"Exception encountered while running query \n {qry} \n {e} \n ")
	finally:
		debug_logger(conn, qry, output, req_id)
	return output

def debug_logger(conn, qry, results, req_id):
	"""
	After executing any query it is important to log the qry in the debug folder

	:param conn: the connection on which the query was executed
	:param qry: the query
	:param results: the return result after the execution
	:param req_id: the attached request id
	:return:
	"""
	conn_name = conn.__class__.__module__.split('.', 1)[0]
	file_name = f'/apps/edwsfdata/python/scripts/debug/{req_id}.dbg'
	if type(results) == int:
		log_data(file_name, f"{conn_name}~{qry}~{results}~{str(datetime.now())}\n", 0)
	else:
		log_data(file_name, f"{conn_name}~{qry}~{len(results)}~{str(datetime.now())}\n", 0)

def send_email(email_add, log_file, err_file, err_msg, subject, repo_conn=None, req_id=None, wc_flag=False):
	"""
	Send an email (Combined sendemail and sendemailwc definitions)

	Setting WC to True will basically invoke (sendemailwc workflow) -- cursor1 and reqid is not required then

	:param email_add: To whom you would like the email sent too
	:param log_file: Path to the log file
	:param err_file: Path to the error file
	:param err_msg: error message
	:param subject: email subject
	:param repo_conn: specific conn for the error
	:param req_id: the request ID
	:param wc_flag: wc(without cursor) if you  want to log it he DIY email logs then this should be set to True
	:return:
	"""

	msg = MIMEMultipart()
	message = err_msg
	body = MIMEText(message)
	msg_from = 'sf-smart-ingestiony@cisco.com'
	to_list = email_add
	msg['Subject'] = subject
	msg['From'] = msg_from
	msg['To'] = 'mkumarap@cisco.com'  # tolist

	files = []
	# Validating if the files have any data in them
	if os.stat(log_file).st_size > 0:
		files.append(['Logfile.txt', log_file])

	if os.stat(err_file).st_size > 0:
		files.append(['Errorfile.txt', err_file])

	for file in files:
		# file = logFile
		# msg.attach(MIMEText("Logfile.txt"))
		attachment = MIMEBase('application', 'octet-stream')
		attachment.set_payload(open(file[1], 'rb').read())
		encoders.encode_base64(attachment)
		attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % file[0])
		msg.attach(attachment)
	msg.attach(body)
	s = smtplib.SMTP("outbound.cisco.com")
	s.sendmail(msg_from, to_list.split(","), msg.as_string())
	s.quit()
	if not wc_flag:
		try:
			err_msg = err_msg.replace("'", "''")
			subject = subject.replace("'", "''")
			insert_sql = f"insert into DIY_email_logs (reqid,email_address,subject,errmsg,logfile,errfile,sent_time)" \
						 f" values ({req_id},'{email_add}','{subject}','{err_msg}','{log_file}','{err_file}', sysdate)"
			results = execute_qry(repo_conn, insert_sql, req_id, repo_conn)
			if results == -1:
				raise Exception(f'Unable to update email log: {insert_sql}')
			execute_qry(repo_conn, 'commit', req_id, repo_conn)
		except Exception as e:
			log_data(err_file, "Exception encountered while inserting/updating Email Log", 1)
			log_data(err_file, str(e), 1)
			subject = "Error: Exception encountered while updating DIY lines"
			err_msg = subject + "\n\n" + str(e)
			log_data(log_file, f"Sending email to {email_add}  with status - {err_msg}", 0)
			send_email(email_add, log_file, err_file, err_msg, subject, wc_flag=True)

def validate_params(input_type, param_file, table_name, env_type, thread_cnt,
					job_stream_id, email_add, dest_schema, success_mail):
	"""
	validating the input parameters to the the script

	:param input_type: type of input
	:param param_file: parameter file
	:param table_name: table name given by user to ingest (Fully qualified path)
	:param env_type: Which environment is the source
	:param thread_cnt: the total thread count
	:param job_stream_id: the job stream id associated to specific job
	:param email_add: the email address
	:param dest_schema: destination schema
	:param success_mail: Send a email to the email address if job is successful
	"""

	input_type = input_type.lower()

	if input_type not in ('db', 'file', 'table', 'jct'):
		print("Invalid value for argument -t. It has to be either db (if source is another database) "
			  "or file (if source is flat file) or table (previously ingested table) or jct")
		sys.exit(1)
	if input_type == 'db':
		exists = os.path.isfile(param_file)
		if not exists:
			print(f"Invalid parameter file - {param_file}. Please check for the existence of "
				  "parameter file and reinvoke the program")
			sys.exit(2)
	if input_type == 'file':
		exists = os.path.isfile(param_file)
		if not exists:
			print(
				f"Invalid data file - {param_file}. Please check for existence of source file and reinvoke the program")
			sys.exit(3)
	if input_type == 'table':
		if table_name is None:
			print("tableName is mandatory. Please provide tableName for which data was previously ingested")
			sys.exit(4)
		if env_type is None:
			print(f"Environment type (TS*/DV*/PRD) is mandatory. Please provide the environment type "
				  f"for table - {table_name} for which data was previously ingested")
			sys.exit(5)
		if table_name.count('.') < 2:
			print(f"Table name - {table_name} needs to be given in db.schema.tablename format. Please check and retry")
			sys.exit(6)
	if input_type == 'jct':
		if job_stream_id is None:
			print("Job stream id is mandtory for type jct. Please provide JCT id which is present in repository")
			sys.exit(8)
		if env_type is None:
			print("Environment type (TS*/DV*/PRD) is mandatory. Please provide the environment type")
			sys.exit(9)
	if thread_cnt is not None:
		try:
			thread_Cnt = int(thread_cnt)
		except Exception:
			print(f"Invalid value for threadcount - {thread_cnt}. Thread count needs to be a "
				  "integer if you want to override. Please check and retry")
			sys.exit(7)
	if email_add is None:
		print("Email address is mandatory. Please provide the same using -a option (example -a mkumarap@cisco.com)")
		sys.exit(10)
	if dest_schema is not None:
		if dest_schema not in ('SS', 'BR'):
			print("Invalid destination DB. Valid values are only SS and BR. Please check and retry")
			sys.exit(11)
	if success_mail is not None:
		if success_mail.upper() != 'YES' and success_mail.upper() != 'NO':
			print("Invalid value for -o mail on Success. Valid values are only Yes and No. Please check and retry")
			sys.exit(12)

def create_dirs(job_group, env_type, job_group_init_scr, log_dir):
	"""
	Create the appropriate error and log files for the associated job

	:param job_group: the associated job group
	:param env_type: the env we want to run
	:param job_group_init_scr:
	:param log_dir: the full path in which to create the log and error files
	:return:
	"""
	os.environ["RUN_ENVIRONMENT"] = env_type
	log_file = f"{log_dir}JobGroupInit_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
	err_file = f"{log_dir}JobGroupInit_{datetime.now().strftime('%Y%m%d%H%M%S')}.err"
	try:
		with open(log_file, 'w') as lf, open(err_file, 'w') as ef:
			res = subprocess.Popen([job_group_init_scr, job_group], stdout=lf, stderr=ef)
			res.wait()
			return_code = res.returncode
		return return_code
	except OSError:
		return -1

def collect_metadata(env_type, source_db, source_schema, source_table, log_file, err_file,
					 connection_name, log_dir):
	"""
	Set the environment for the source data connection

	:param env_type: The specific environment for the source
	:param source_db: the source database
	:param source_schema: the source schema
	:param source_table: source table
	:param log_file: the log file path
	:param err_file: the error file path
	:param connection_name: The specific connection name ( need more context)
	:param log_dir: log directory path
	:return: return code for the subprocess for the source data connection
	"""
	log_data(log_file, "Now launching metadata collection module", 0)

	log_meta_file = f"{log_dir}metadata_{source_table}{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
	err_meta_file = f"{log_dir}metadata_{source_table}{datetime.now().strftime('%Y%m%d%H%M%S')}.err"
	try:
		meta_scr_2_run = "/apps/edwsfdata/python/scripts/DIY_metadata.py"
		with open(log_meta_file, 'w') as lf, open(err_meta_file, 'w') as ef:
			res = subprocess.Popen([meta_scr_2_run, "-e", env_type, "-d", source_db, "-s", source_schema, "-t",
									source_table, "-c", connection_name], stdout=lf, stderr=ef)
			res.wait()
		return res.returncode
	except Exception as e:
		log_data(err_file, "Exception encountered while launching process for metadata collection", 1)
		log_data(err_file, str(e), 1)
		with open(err_meta_file, 'a') as ef:
			ef.write(f"Exception encountered while launching process for metadata collection - {e}")
		return -1

def log_data(file_name, msg, print_flag):
	"""
	log the data into the specific file provided

	:param file_name: file path only! (fully qualified)
	:param msg: message
	:param print_flag: if set to 1 then we can print current time and message
	"""
	curr_time = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
	if print_flag == 1:
		print(curr_time + " - " + msg)
	with open(file_name, 'a+') as f:  # open file in append mode
		f.write(curr_time + " - " + msg + "\n")
		f.flush()
		os.fsync(f.fileno())

def log_diymaster_error(req_id, msg, e, repo_conn):
	"""
	Log the current error status for the req_id into the DIY_master table in ODSPROD (oracle)

	:param req_id: the associated request ID
	:param msg: Current message
	:param e: the exception that occurred (send exception as is do not send the string)
	:param repo_conn: repo conn
	"""

	# Goes to get_env_db and gets the columns for the specific req_id
	req_data_qry = f'SELECT logs_dir,attribute2 FROM DIY_master where reqid = {req_id}'
	req_data = execute_df_qry(repo_conn, req_data_qry, req_id, repo_conn)

	# if data is not correct log into the debug file and return
	if len(req_data) != 1:
		file_name = f'/apps/edwsfdata/python/scripts/debug/{req_id}.dbg'
		log_data(file_name, f"{repo_conn}~req_data_qry~{len(req_data)}~{str(datetime.now())}\n", 0)
		return

	log_file = f"{req_data['logs_dir'][0]}/main.log"
	err_file = f"{req_data['logs_dir'][0]}/main.err"
	email_add = req_data['attribute2'][0]

	update_diymaster(req_id, 'err_msg', msg + str(e), repo_conn, err_file, log_file, email_add)  # attribute2
	update_diymaster(req_id, 'end_time', 'curr_date', repo_conn, err_file, log_file, email_add)

	log_data(err_file, msg, 1)
	log_data(err_file, str(e), 1)
	log_data(log_file, f"Sending email to {email_add}  with status - {msg}", 0)
	send_email(email_add, log_file, err_file, msg, f"Error:{msg}", repo_conn, req_id)

def update_diymaster(req_id, update_column, column_value, repo_conn, err_file, log_file, email_add):
	"""
	Update the DIY_master table in Oracle (ODSPROD) with information regarding the req_id (unique here)

	:param req_id: the request ID for the job
	:param update_column: the column that will be update
	:param column_value: the value to insert in the column
	:param repo_conn: the repo conn
	:param err_file: error file path
	:param log_file: log file path
	:param email_add: the email address
	:return:
	"""
	try:
		if column_value == 'curr_date':
			upd_sql = f"update DIY_master set {update_column}=sysdate,last_update_time = sysdate where reqid={req_id}"
		else:
			column_value = column_value.replace("'", "''")
			upd_sql = f"update DIY_master set {update_column} ='{str(column_value)}'," \
					  f"last_update_time = sysdate where reqid={req_id}"
		result = execute_qry(repo_conn, upd_sql, req_id, repo_conn)
		if result == -1:
			raise Exception(f'Unable to execute {upd_sql} ')

		execute_qry(repo_conn, 'commit', req_id, repo_conn)
	except Exception as e:
		log_data(err_file, "Exception encountered while updating DIY Master", 1)
		log_data(err_file, str(e), 1)
		log_data(err_file, f"Command executed is - {update_column}", 0)
		subject = "Error: Exception encountered while updating DIY Master"
		err_msg = subject + "\n\n" + str(e)
		log_data(log_file, f"Sending email to {email_add}  with status - {err_msg}", 0)
		send_email(email_add, log_file, err_file, err_msg, subject, repo_conn, req_id)

def insert_diylines(req_id, pre_phase, curr_phase, repo_conn, err_file, email_add, log_file):
	"""
	Insert the current step for the reqid in the diy_lines table in Oracle (odsprod)

	:param req_id: the request ID to track the job
	:param pre_phase: the step that the job completed
	:param curr_phase: the current step the job is on
	:param repo_conn: the repo conn
	:param err_file: the error file name
	:param email_add: the email address
	:param log_file: log file name
	"""
	try:
		insert_qry = f"insert into DIY_lines(reqid,step_name,step_start_time) values ({req_id},'{curr_phase}',sysdate)"
		result = execute_qry(repo_conn, insert_qry, req_id, repo_conn)
		if result == -1:
			raise Exception(f'Unable to execute {insert_qry} ')
		update_qry = f"update DIY_lines set step_end_time=sysdate where reqid={req_id} and step_name='{pre_phase}'"
		result = execute_qry(repo_conn, update_qry, req_id, repo_conn)
		if result == -1:
			raise Exception(f'Unable to execute {update_qry} ')
	except Exception as e:
		log_data(err_file, "Exception encountered while inserting/updating DIY lines", 1)
		log_data(err_file, str(e), 1)
		subject = "Error: Exception encountered while updating DIY lines"
		err_msg = subject + "\n\n" + str(e)
		log_data(log_file, f"Sending email to {email_add}  with status - {err_msg}", 0)
		send_email(email_add, log_file, err_file, err_msg, subject, repo_conn, req_id)

def check_result(inc_dtype, err_file):
	"""
	This method checks the result from execute_df_query() method should either be a date or an ID. Rest anything else is not a
	valid result and we will throw error that the incremental column was incorrect.
	:param incdType: The resultant dataframe
	:param err_file: The Error file to which all the errors/exception are written
	:return: -1
	"""

	if inc_dtype.values[0][0] == 'INVALID' or len(inc_dtype) == 0 or (
			inc_dtype.values[0][0] != 'DATE' and inc_dtype.values[0][0] != 'NUMBER'):
		log_data('ERRORFILE.err',
				 "Incremental column specified is neither a date data type nor a ID data type. Hence cannot"
				 " update incremental column details", 1)
		return -1

def update_incr_column(source_schema, source_table, inc_col_name, log_file, err_file, conn, req_id, repo_conn):
	"""

	:param source_schema: Source schema name
	:param source_table: Source table name
	:param inc_col_name: the incremental column name
	:param log_file: log file path
	:param err_file: error file path
	:param conn: connection
	:param req_id: request ID
	:param repo_conn: repo conn (Oracle)
	:return the incr Dtype value or -1
	"""
	try:
		log_data(log_file, "Now checking the data type for incremental column in source db", 0)
		conn_type = conn.__class__.__module__.split('.', 1)[0]
		if conn_type == 'cx_Oracle':
			query = f" select decode(data_type,'NUMBER','ID','DATE','DATE','TIMESTAMP(3)'," \
					f"'DATE','TIMESTAMP(6)','DATE','INVALID') from all_tab_columns where " \
					f"owner='{source_schema}' and table_name='{source_table}' and column_name='{inc_col_name}' "
			query_max = f" select /*+ parallel (a,16) */ max(to_date(to_char({inc_col_name}," \
						f"'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS')) from {source_schema}.{source_table} a "
		else:
			query = f" select decode(trim(columntype),'DA','DATE','I','ID','I8','ID','I1','ID','I2','ID'," \
					f"'TS','DATE','TZ','DATE','INVALID') from  dbc.columnsv where " \
					f"databasename= '{source_schema}' and tablename='{source_table}' and columnname='{inc_col_name}' "
			query_max = f" select max({inc_col_name}) from {source_schema}.{source_table}"
		inc_dtype = execute_df_qry(conn, query, req_id, repo_conn)
		result_status = check_result(inc_dtype, err_file)
		if not result_status:
			log_data(log_file,
					 f"Incremental column identified is of data type {inc_dtype.values[0][0]}."
					 f" Now extracting latest value from source", 0)
			res_inc_value = execute_df_qry(conn, query_max, req_id, repo_conn)
			if inc_dtype.values[0][0] == 'DATE':
				t = pd.to_datetime(res_inc_value.values[0][0])
				res_inc_value = t
			res_inc_value = str(res_inc_value)
			log_data(log_file, f"Max value of {inc_col_name} in table {source_schema}.{source_table} "
							   f"is {res_inc_value}", 0)
			return inc_dtype.values[0][0] + "~" + res_inc_value
		else:
			return -1
	except Exception as e:
		log_data(err_file, "Exception occurred while pulling incremental column data", 1)
		log_data(err_file, e, 1)
		return -1

def invoke_csv_download(env_type, scr_path, scr, job_group, job_stream_arr, log_dir, master_log_file,
						thread_count, table_size, conn, base_table, base_schema, req_id):
	"""
	:param env_type:
	:param scr_path:
	:param scr:
	:param job_group:
	:param job_stream_arr:
	:param log_dir:
	:param master_log_file:
	:param thread_count:
	:param table_size:
	:param conn:
	:param base_table:
	:param base_schema:
	:param req_id:
	:return:
	"""
	os.environ["RUN_ENVIRONMENT"] = env_type
	# logFile=logDir + jobStream + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + ".log"
	# oscmd=scr_Path + Talend_Scr + " " + job_group + " " + jobStream + " >> " + logFile + " 2>&1 &"
	os_cmd = scr_path + scr
	mon_job_stream = job_stream_arr[0][:-2]
	os_process_cmd = f"ps -ef | grep {scr} | grep {mon_job_stream} | grep -v grep | wc -l"
	processes = set()
	process_count = 0
	open_files = []

	for curr_job_str in job_stream_arr:
		process_count += 1
		log_data(master_log_file, "Now executing below OS Command", 0)
		log_data(master_log_file, f"{os_cmd} {job_group} {curr_job_str} "
								  f"{str(table_size)} {base_table} {base_schema}", 0)

		out_file = f"{log_dir}{curr_job_str}_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
		err_file = f"{log_dir}{curr_job_str}_{datetime.now().strftime('%Y%m%d%H%M%S')}.err"
		outf = open(out_file, 'w')
		errf = open(err_file, 'w')
		open_files.append(outf)
		open_files.append(errf)

		processes.add(subprocess.Popen([os_cmd, job_group, curr_job_str, str(table_size), base_table, base_schema],
									   stdout=outf, stderr=errf))
		print(f"Launched {str(process_count)} processes out of {str(len(job_stream_arr))} total process to submit")
		log_flag = True
		while len(processes) >= thread_count:
			cnt = int(os.popen(os_process_cmd).read())
			if log_flag:
				log_data(master_log_file, f"{str(cnt)} ingestion threads are active for {mon_job_stream} "
										  f"ingestion stream..", 1)
				log_flag = False
			os.wait()
			time.sleep(10)  # 10 seconds sleep before checking for process status
			# Just to keep the oracle connective live for very long running ingestions
			execute_qry(conn, "select sysdate from dual", req_id, conn)
			processes.difference_update(
				[p for p in processes if p.poll() is not None])
		time.sleep(5)  ## Sleeping 5 seconds between each parallel submission
	for p in processes:
		batch_sleep = 0
		total_sleep_time = 0
		while p.poll() is None:
			cnt = int(os.popen(os_process_cmd).read())
			if batch_sleep == 0:
				log_data(master_log_file, f"{str(cnt)} ingestion threads are active for {mon_job_stream} "
										  f"ingestion stream..", 1)
				batch_sleep = 1
			time.sleep(60)
			# Just to keep the oracle connection live for very long running ingestions
			execute_qry(conn, "select sysdate from dual", req_id, conn)
			total_sleep_time += 60
			if total_sleep_time >= cnt * 60:
				total_sleep_time = 0
				batch_sleep = 0

	for file in open_files:
		if file:
			file.close()

def validate_view(env_type, source_db, db_type, source_conn, source_schema, source_table, email_add, log_file, err_file,
				  repo_conn, req_id):
	"""

	:param env_type: the source environment type
	:param source_db: source database name
	:param db_type: source database type
	:param source_conn: source connection string
	:param source_schema: source schema name that will be validated
	:param source_table: source table name that will be validated
	:param email_add: email address
	:param log_file: log file name
	:param err_file: error file name
	:param repo_conn: repo conn name
	:param req_id: request ID
	:return: schema/ table name or -1
	"""
	try:
		global excep_view
		if db_type.upper() == 'TERADATA':
			qry = f" exec PERFMETRICSDB.BASEVIEWCHECK ('{source_schema}','{source_table}')"
			results = execute_df_qry(source_conn, qry, req_id, repo_conn)
			# DOES THIS RETURN STRING OR INT
			if results['src_obj'][0] in ('-1', '-2'):  # complex view
				error_msg = 'does not exist' if results['src_obj'][0] == '-2' else 'is not a 1-1 view'
				log_data(err_file, f"Source Teradata object - {source_schema}. {source_table} {error_msg}. "
								   f"Hence cannot be ingested", 1)
				subject = f"Error: Source Teradata object - {source_schema}. {source_table} {error_msg}. " \
						  f"Hence cannot be ingested"
				err_msg = subject
				send_email(email_add, log_file, err_file, err_msg, subject, repo_conn, req_id, True)
				return -1
			else:
				obj_array = results['src_obj'][0].split(".")
				src_base_schema, src_base_table = obj_array[0], obj_array[1]

				log_data(log_file, f"Found base schema as {src_base_schema} and base table as {src_base_table}", 0)
				return f'{src_base_schema}~{src_base_table}'

		elif db_type.upper() == 'ORACLE':
			log_data(log_file, "Checking if source object exists in database", 0)
			qry = f"select count(1) cnt from all_objects where object_name='{source_table}' and owner='{source_schema}'"
			results = execute_df_qry(source_conn, qry, req_id, repo_conn)
			if results['cnt'][0] == 0:
				qry = "select user from dual"
				results = execute_df_qry(source_conn, qry, req_id, repo_conn)
				current_user = results['user'][0]
				subject = f"Either source object {source_schema}.{source_table} does not exist or user {current_user}" \
						  f" does not have access. Hence cannot ingest. Please check and retry"
				err_msg = subject
				send_email(email_add, log_file, err_file, err_msg, subject, repo_conn, req_id, True)
				log_data(log_file, subject, 1)
				return -1
			log_data(log_file, "Checking if passed object is view", 0)
			qry = f"select count(1) cnt from all_tables where table_name='{source_table}' and owner='{source_schema}'"
			table_count = execute_df_qry(source_conn, qry, req_id, repo_conn)
			if table_count['cnt'][0] == 0:
				# 4.59 change starts here
				table_found_flag = False
				log_data(log_file, "Checking if passed object is Synonym", 0)
				qry = f" select b.owner, b.object_name, b.object_type from all_synonyms a, all_objects b " \
					  f"where a.synonym_name='{source_table}' and a.owner='{source_schema}' " \
					  f"and a.table_name=b.object_name and a.table_owner=b.owner and b.object_type in ('TABLE','VIEW')"
				results = execute_df_qry(source_conn, qry, req_id, repo_conn)
				source_type = ''

				if len(results) > 0:
					source_owner, source_name, source_type = results['owner'][0], results['object_name'][0], results['object_type'][0]

				if source_type == 'TABLE':
					src_base_schema, src_base_table = source_owner, source_name
					table_found_flag = True
				elif source_type == 'VIEW':
					view_owner, view_name = source_owner, source_name

				if source_type == '':
					log_data(log_file, "Checking if passed object is view", 0)
					qry = f"select owner,view_name from all_views where view_name='{source_table}' and owner='{source_schema}'"
					results = execute_df_qry(source_conn, qry, req_id, repo_conn)
					view_owner, view_name, source_type = results['owner'][0], results['view_name'][0], 'VIEW'

				if source_type == 'VIEW':
					log_data(log_file, f"Now checking if view {view_owner}. {view_name} exists in exception list", 0)
					qry = f"select count(1) cnt from edw_view_ingestion where environment_type='{env_type}' and " \
						  f"view_name='{view_name}' and db_name='{source_db}' and schema_name='{view_owner}' " \
						  f"and active_flag='Y'"
					results = execute_df_qry(source_conn, qry, req_id, repo_conn)
					except_count = results['cnt'][0]
					if except_count > 0:
						table_found_flag, excep_view = True, True
						src_base_schema, src_base_table = source_schema, source_table
						log_data(log_file,
								 "Found base schema as " + src_base_schema + " and base table as " + src_base_table, 0)
						return f'{src_base_schema}~{src_base_table}'
					else:
						log_data(log_file, f"Now checking if view {view_owner}. {view_name} is 1-1", 0)
						qry = f"select referenced_owner,referenced_name from  all_dependencies where " \
							  f"name='{view_name}' and owner='{view_owner}' and type='VIEW'  and " \
							  f"dependency_type='HARD'"
						results = execute_df_qry(source_conn, qry, req_id, repo_conn)
						table_count = len(results)
						if table_count == 1:
							src_base_schema, src_base_table = results['referenced_owner'][0],results['referenced_name'][0]
							table_found_flag = True
						else:
							log_data(err_file, f"Source  Oracle object - {source_schema}. {source_table} "
											   f"is not 1-1 view.. Hence cannot be ingested", 1)
							subject = f"Source  Oracle object - {source_schema}. {source_table} " \
									  f"is not 1-1 view.. Hence cannot be ingested"
							err_msg = subject
							send_email(email_add, log_file, err_file, err_msg, subject, repo_conn, req_id, True)
							return -1
				if source_type == 'VIEW' and not excep_view:
					log_data(log_file, f"Now checking if view {view_owner}. {view_name} has all columns as table", 0)
					qry = f"select column_name,data_type from all_tab_columns where table_name='{src_base_table}' " \
						  f"and owner='{src_base_schema}' minus select column_name,data_type from all_tab_columns " \
						  f"where table_name='{src_table}' and owner='{src_schema}'"
					results = execute_df_qry(source_conn, qry, req_id, repo_conn)
					column_count = len(results)
					if column_count > 0:
						log_data(err_file, f"Source  Oracle object - {source_schema}. {source_table} "
										   f"is not 1-1 view.. Hence cannot be ingested", 1)
						subject = f"Source  Oracle object - {source_schema}. {source_table} is not 1-1 view.. " \
								  f"Hence cannot be ingested"
						err_msg = subject
						send_email(email_add, log_file, err_file, err_msg, subject, repo_conn, req_id, True)
						return -1
				if not table_found_flag:
					log_data(err_file, f"Source Oracle object - {source_schema}. {source_table} is not a "
									   f"base table.. Hence cannot be ingested", 1)
					subject = f"Error: Source Oracle object - {source_schema}. {source_table} is not a base table.. " \
							  f"Hence cannot be ingested"
					err_msg = subject
					send_email(email_add, log_file, err_file, err_msg, subject, repo_conn, req_id, True)
					return -1
				else:
					log_data(log_file, f"Found base schema as {src_base_schema} and base table as {src_base_table}", 0)
					return f'{src_base_schema}~{src_base_table}'
			else:
				src_base_schema, src_base_table = source_schema, source_table
				log_data(log_file, f"Found base schema as {src_base_schema} and base table as {src_base_table}", 0)
				return f'{src_base_schema}~{src_base_table}'
	except Exception as e:
		log_data(err_file, f"Exception encountered while checking passed object is table or view - {source_table}", 1)
		log_data(err_file, str(e), 1)
		return -1

def validate_metadata(env_type, source_db, source_schema, source_table, conn, log_file, err_file, source_db_type,
					  connection_name, log_dir, req_id, repo_conn):
	"""
	validating metadata to make sure all source table columns exists in repository

	:param env_type:
	:param source_db:
	:param source_schema:
	:param source_table:
	:param conn:
	:param log_file:
	:param err_file:
	:param source_db_type:
	:param connection_name:
	:param log_dir:
	:param req_id:
	:param repo_conn:
	:return:
	"""

	log_data(log_file, "Now validating metadata to make sure all source table columns exists in repository", 0)
	if source_db_type.upper() == "TERADATA":
		try:
			if 'VWDB' in source_schema.upper():
				log_data(log_file, "Pulling 3NF for vw schema", 0)
				qry = f" select distinct db_schema_name from EDS_DATA_CATALOG.EDW_TD_VWDB_MAPPING " \
					  f"where view_schema_name='{source_schema}'"
				results = execute_df_qry(conn, qry, req_id, repo_conn)
				source_schema = results['db_schema_name'][0]
				log_data(log_file, f"Found base schema as {source_schema}", 0)
		except Exception as e:
			# print ("Error occurred while checking base table from 1-1 view")
			log_data(err_file, "Error occurred while checking table schema from view schema", 1)
			log_data(err_file, str(e), 1)
			return -1

	sql = f"select count(*) cnt from EDS_DATA_CATALOG.EDW_TABLE_COLUMN where db_instance_name='{source_db}' " \
		  f"and db_schema_name='{source_schema}' and table_name='{source_table}' and " \
		  f"environment_name='{env_type}' and data_type is not null"
	try:
		results = execute_df_qry(conn, sql, req_id, repo_conn)
		count = results['cnt'][0]
		if count == 0:
			log_data(log_file, f"Now attempting to collect metadata for table - {source_table}", 0)
			return_code = collect_metadata(env_type, source_db, source_schema, source_table, log_file, err_file,
										   connection_name, log_dir)
			if return_code != 0:
				raise Exception("Issue encountered while collecting metadata. Please check logfile")
			log_data(log_file, "Post collecting metadata checking for metadata in repos", 0)
			try:
				results = execute_df_qry(conn, sql, req_id, repo_conn)
				count = results['cnt'][0]
				if count == 0:
					log_data(err_file, "Even though attempted to collect metadata, collect metadata failed.. "
									   "Please check and take action", 1)
					return -1
				else:
					return count
			except Exception as e:
				log_data(err_file, f"Exception encountered while verifying metadata existence for "
								   f"table post collection- {source_table}", 1)
				log_data(err_file, f"SQL query ran was {sql}", 1)
				log_data(err_file, str(e), 1)
				return -1
		else:
			return count
	except Exception as e:
		log_data(err_file, f"Exception encountered while verifying metadata existence for table "
						   f"pre collection - {source_table}", 1)
		log_data(err_file, f"SQL query ran was {sql}", 1)
		log_data(err_file, str(e), 1)
		return -1

def zero_record_ingestion(job_stream,req_id,conn,err_file,log_file,email_add,repo_conn):
	log_data(log_file,"Now marking SRC2STG and STG2BR workflows as completed since no record to process",1)
	sql=f"select count(*) cnt from edw_job_streams where job_stream_id='{job_stream}'"
	results_obj = execute_df_qry(conn, sql, req_id, repo_conn)
	jctCnt=results_obj["cnt"][0]
	if jctCnt == 0:
		log_data(log_file,f"Job stream - {job_stream} doesnt exist on database.. Hence nothing to mark complete",1)
	else:
		sql=f"Update edw_job_streams set run_status='C' where job_stream_id='{job_stream}'"
		results = execute_qry(repo_conn, sql, req_id, repo_conn)
		if results == -1:
			raise Exception(f'Unable to update job streams: {sql}')
		log_data(log_file,"Now pulling corresponding STG2BR to mark complete",0)
		sql=f"select TARGET_DB_NAME,TARGET_SCHEMA,TARGET_DB_CONNECTION,TARGET_TABLE_NAME from  EDW_JOB_STREAMS where job_stream_id='{job_stream}'"
		results_obj = execute_df_qry(conn, sql, req_id, repo_conn)
		targetDb=results_obj["target_db_name"][0]
		targetSchema=results_obj["target_schema"][0]
		targetConn=results_obj["target_db_connection"][0]
		targetTbl=results_obj["target_table_name"][0]
		sql=f"select job_stream_id from edw_job_streams where source_db_connection='{targetConn}' and source_db_name='{targetDb}' and source_schema='{targetSchema}' and source_table_name='{targetTbl}' and active_ind='Y'"
		results_obj = execute_df_qry(conn, sql, req_id, repo_conn)
		brJobStream=results_obj["job_stream_id"][0]
		log_data(log_file,f"Found corresponding BR Jobstream as {brJobStream}. Hence marking it as completed as well",1)
		sql=f"Update edw_job_streams set run_status='C' where job_stream_id='{brJobStream}'"
		results = execute_qry(repo_conn, sql, req_id, repo_conn)
		if results == -1:
			raise Exception(f'Unable to update job streams: {sql}')
		execute_qry(conn,"commit",req_id,conn)
		update_diymaster(req_id, 'end_time', 'curr_date', repo_conn, err_file, log_file, email_add)
		update_diymaster(req_id, 'current_phase', 'Success', repo_conn, err_file, log_file, email_add)
		insert_diylines(req_id, 'Invoking_Data_Load', 'Completed_Ingestion', repo_conn, err_file, email_add, log_file)


def LobtoStr(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize,outconverter=OutConverter)
		


def ntile(conn2,colname,schema,tbl,split_cnt,reqid,conn,dist_cnt_limit):
	try:
		cursor2=conn2.cursor()
		cursor2.arraysize=10000
		sql=f'select count(1) cnt from {schema}."{tbl}"'
		cnt_obj=execute_df_qry(conn2,sql,reqid,conn)
		row_count=cnt_obj["cnt"][0]
		buck_row_count=math.ceil(row_count/split_cnt)
		sql=f'select count(distinct {colname}) cnt from {schema}."{tbl}"'
		dist_cnt_obj=execute_df_qry(conn2,sql,reqid,conn)
		dist_cnt=dist_cnt_obj["cnt"][0]
		loop_times=math.ceil(dist_cnt/dist_cnt_limit)
		splitQrydict={}
		split_index=0
		sql=f'select max({colname}) max_val,min({colname}) min_val from {schema}."{tbl}"'
		min_max_obj=execute_df_qry(conn2,sql,reqid,conn)
		min_val=min_max_obj["min_val"][0]
		max_val=min_max_obj["max_val"][0]
		range_partition=math.ceil((max_val-min_val)/loop_times)
		start_val=min_val
		max_skew_threshold=(1+(split_cnt/100))*buck_row_count
		for main_loop_cnt in range(loop_times):
			end_val=start_val+range_partition
			sql=f'select count(*),{colname} from {schema}."{tbl}" where {colname} between {start_val} and {end_val} group by {colname} having count(*) > {max_skew_threshold}'
			max_obj=execute_df_qry(conn2,sql,reqid,conn)
			if len(max_obj) > 0:
				raise Exception (f'Chosen column {colname} is highly skewed to do the split.\n Bucket {start_val}~{end_val} value has more than {max_skew_threshold} rows')
		cursor2.execute(f'select {colname},count(*) cnt from {schema}."{tbl}" group by {colname} order by 1')
		buck_cnt=0
		start_flag=True
		loop_range=math.ceil(dist_cnt/10000)
		for loop_cnt in range(loop_range):
			#cur_list=cursor2.fetchone()
			cur_l=cursor2.fetchmany(size=cursor2.arraysize)
			#inner_loop_cnt=0
			for cur_list in cur_l:
				if start_flag:
					buck_min_val=cur_list[0]
					buck_cnt=0
					start_flag=False
				buck_cnt+=cur_list[1]
				if buck_cnt >= buck_row_count:
					buck_max_val=cur_list[0]
					splitQrydict[split_index]=f"{colname} between {buck_min_val} and {buck_max_val}"
					#print(f'{buck_min_val}~{buck_max_val}~{buck_cnt}')
					if buck_cnt/buck_row_count >= 2:
						raise Exception (f'Specific bucket with min val {buck_min_val} and max val {buck_max_val} has {buck_cnt} rows and highly skewed')
					split_index += 1
					start_flag=True
				else:
					buck_max_val=cur_list[0]
				#inner_loop_cnt += 1
		#split_index += 1
		#buck_max_val=cur_list[0]
		splitQrydict[split_index]=f"{colname} between {buck_min_val} and {buck_max_val}"
		#print(f'{buck_min_val}~{buck_max_val}~{buck_cnt}')
		return(splitQrydict)
	except Exception as e:
		linenu=sys.exc_info()[-1].tb_lineno
		return(f"Exception - {e} occurred at line {linenu} while splitting data")
def c_getuser():
 ppid=os.getppid()
 p=psutil.Process(ppid)
 out_dict=p.as_dict(attrs=['pid', 'name', 'username'])
 out=out_dict['name']
 i=1
 while (out !='su' and i<200):
  p=psutil.Process(ppid).parent()
  out_dict=p.as_dict(attrs=['pid', 'name', 'username'])
  out=out_dict['name']
  ppid=out_dict['pid']
  i+=1
 user=out_dict['username']
 p=psutil.Process(ppid).parent()
 out_dict=p.as_dict(attrs=['pid', 'name', 'username'])
 out=out_dict['name']
 if out == "sudo":
  ppid=out_dict['pid']
  p=psutil.Process(ppid).parent()
  out_dict=p.as_dict(attrs=['pid', 'name', 'username'])
  user=out_dict['username']
 if i <= 200:
  return(user)
 else:
  return("-1")

