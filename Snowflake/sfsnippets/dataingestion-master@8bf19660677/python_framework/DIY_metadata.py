#!/apps/python/install/bin/python
#########################################################################################################################
#DIY_metadata.py                                                                                                        #
# Script to Collect metadata                                                                                            #
# Modification History                                                                                                  #
# Version         Modified By   Date            Change History                                                          #
# 1.0             Manick        Oct-2019        Initial Version                                                         #
# 1.1             Manick        Nov-2019        Fix for Number datatype in Oracle for scale and precisi                 #
# 1.2             Manick        Nov-2019        Validate oracle params failing for view                                 #
# 1.3             Manick        Nov-2019        Ignore M_ROW$$ columns while pulling unique index for MV's              #
# 1.4             Manick        Nov-2019        Dont add default precision if table column has ID/NUM/NUMBER            #
#                                               Strip off Timestamp precision only for TD                               #
# 1.5             Manick        Nov-2019        copy table metadata for synonym if passed object is synonym             #
# 1.6             Manick        Nov-2019        Zero length varchar failing in Snowflake                                #
# 1.7             Manick        Jan-2020        Switch location of odbc.ini file                                        #
# 1.8             Manick        Jan-2020        Switch to tdsql                                                         #
# 1.9             Manick        Jan-2020        Disable metadata collection for selective tables                        #
# 1.10            Manick        Apr-2020        Convert dynamic connection of TNS                                       #
# 1.11            Raveendra     Apr-2020        Enabled Default value and constraint information to metadata            #
# 1.12            Raveendra     May-2020        Disabled check constraint for oracle - column sql query changed         #
# 1.13            Raveendra     May-2020        Handle single quote/double quotes of default values of columns          #
# 1.14            Raveendra     May-2020        fix issue with default value containing to_char,to_number in it         #
# 1.15            Manick        May-2020        Enable HANANRT                                                          #
# 1.16            Manick        Jun-2020        Enable Snowflake                                                        #
# 1.17            Manick        Sep-2020        Enable query tag for SF connection                                      #
# 1.18            Manick        Sep-2020        Remov reference to Talend for connecting to repository                  #
# 1.19            Manick        Oct-2020        Sample data to find precision for Oracle if doesnt defined in metadata  #
# 1.20            Manick        Oct-2020        If oracle default value has paranthesis insert null                     #
# 1.21            Manick        Nov-2020        Look for Primary key for Oracle while inserting EDW_UNIQUE_KEY          #
# 1.22            Manick        Jun-2021        Map Oracle Number to Snowflake float as data profiling is very costly   #
# 1.23            Manick        Jun-2021        New version with additional comments for easy readability               #
#########################################################################################################################

import sys, pdb
import cx_Oracle
import os
from ConfigObject import ConfigObject
import argparse
import teradatasql
from metadata_handler import *
import snowflake.connector

## This method is used to set Oracle environment to make client connection

def set_env():
	os.environ['LD_LIBRARY_PATH'] = "/usr/cisco/packages/oracle/oracle-12.1.0.2/lib"
	os.environ['ORACLE_HOME'] = "/usr/cisco/packages/oracle/oracle-12.1.0.2"
	os.environ['ODBCINI'] = '/apps/edwsfdata/python/scripts/odbc/.odbc.ini'
	os.environ['TNS_ADMIN'] = "/apps/edwsfdata/python/network/admin"
	os.environ['RUN_ENVIRONMENT'] = envType

## Oracle can store numbers with decimals though precision and scale are not defined which would be a problem if defined as-is in Snowflake.
## This method is used to profile the data to find the precision incase of numeric column. However this is obsolete and no longer in use as it is very resource consuming.

def derive_data_scale(cursor2,colName,schemaName,tableName):
	sql=f"select nvl(num_rows,-1) from all_tables where table_name='{tableName}' and owner='{schemaName}'"
	cursor2.execute(sql)
	results=cursor2.fetchall()
	sample_rows=1000000
	for results_obj in results:
		tbl_row_cnt=results_obj[0]
	if len(results) > 0:
		if tbl_row_cnt > -1:
			sample_rows=round(0.1*tbl_row_cnt)
	if sample_rows >= 1000000:
		sample_rows=1000000
	sql=f"""select max(decode(length(substr({colName},instr({colName},'.',1))),length({colName}),0,length(substr({colName},instr({colName},'.',1))))-1) len
		    from  (select /*+ parallel(a,8) */ {colName} from  {schemaName}.{tableName} a where rownum < {sample_rows} and {colName} is not null) """
	cursor2.execute(sql)
	results=cursor2.fetchall()
	for results_obj in results:
		decimal_digit=results_obj[0]
	if len(results) == 0 or decimal_digit is None:
		decimal_digit=6
	if decimal_digit == -1:
		if colName.endswith ('_ID') or colName.endswith('_BY') or colName.endswith('_NUM') or colName.endswith('_NUMBER'):
			return 0
		else:
			sql=f"""select max(decode(length(substr({colName},instr({colName},'.',1))),length({colName}),0,length(substr({colName},instr({colName},'.',1))))-1) len
				from  (select /*+ parallel(a,8) */ {colName} from  {schemaName}.{tableName} a )"""
			cursor2.execute(sql)
			results=cursor2.fetchall()
			for results_obj in results:
				decimal_digit=results_obj[0]
			if decimal_digit == -1:
				return 0
			elif decimal_digit > 5:
				return 99
			else:
				return 5
	elif decimal_digit > 5:
		return 99
	else:
		return 5

## Validate if passed oracle table is valid to collect metadata

def validate_orcl_params(cursor2, schemaName, dbName, tableName):
	global sql1
	sql1 = "select count(1) from all_users where username='" + schemaName + "'"
	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for obj in results:
		srcSchemaCnt = obj[0]
	if srcSchemaCnt == 0:
		print("Schema - " + schemaName + " doesnt exist on source database. Please check and retry")
		return -1
	sql1 = "select count(1) from all_tables where owner='" + schemaName + "' and table_name='" + tableName + "'"
	sql1 += " union "
	sql1 += "select count(1) from all_views where owner='" + schemaName + "' and view_name='" + tableName + "'"
	sql1 += " union "
	sql1 += "select count(1) from all_synonyms where owner='" + schemaName + "' and synonym_name='" + tableName + "'"
	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for obj in results:
		srcTblCnt = obj[0]
	if srcTblCnt == 0:
		print("Table - " + schemaName + "." + tableName + " doesnt exist on source database. Please check and retry")
		return -1
	return 0

## Validate if passed TD table is valid to collect metadata

def validate_td_params(cursor2, schemaName, dbName, tableName):
	global sql1
	sql1 = "select count(1) from dbc.databases where DBKIND ='D' and databasename='" + schemaName + "'"
	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for obj in results:
		srcSchemaCnt = obj[0]
	if srcSchemaCnt == 0:
		print("Schema - " + schemaName + " doesnt exist on source database. Please check and retry")
		return -1
	sql1 = "select count(1) from dbc.tablesv where databasename='" + schemaName + "' and tablename='" + tableName + "'"
	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for obj in results:
		srcTblCnt = obj[0]
	if srcTblCnt == 0:
		print("Table - " + schemaName + "." + tableName + " doesnt exist on source database. Please check and retry")
		return -1
	return 0

## Validate if passed Hana table is valid to collect metadata

def validate_hana_params(cursor2, schemaName, dbName, tableName):
	global sql1
	sql1 = "select count(1) from SYS.SCHEMAS where schema_name='" + schemaName + "'"
	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for obj in results:
		srcSchemaCnt = obj[0]

	if srcSchemaCnt == 0:
		print("Schema - " + schemaName + " doesnt exist on source database. Please check and retry")
		return -1
	sql1 = "select count(1) from SYS.tables where schema_name ='" + schemaName + "' and table_name ='" + tableName + "'"
	sql1 += " union "
	sql1 += "select count(1) from SYS.views where schema_name ='" + schemaName + "' and view_name  ='" + tableName + "'"
	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for obj in results:
		srcTblCnt = obj[0]
		if srcTblCnt == 1:
			break;
	if srcTblCnt == 0:
		print("Table - " + schemaName + "." + tableName + " doesnt exist on source database. Please check and retry")
		return -1
	return 0

## Validate if passed Snowflake table is valid to collect metadata

def validate_snowflake_params(cursor2, schemaName, dbName, tableName):
	global sql1
	sql1 = f"select count(1) from {dbName}.information_schema.schemata where schema_name='{schemaName}'"
	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for obj in results:
		srcSchemaCnt = obj[0]

	if srcSchemaCnt == 0:
		print("Schema - " + schemaName + " doesnt exist on source database. Please check and retry")
		return -1
	sql1 = f"select count(1) from {dbName}.information_schema.tables where table_schema ='{schemaName}' and table_name ='{tableName}'"
	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for obj in results:
		srcTblCnt = obj[0]
		if srcTblCnt == 1:
			break;
	if srcTblCnt == 0:
		print("Table - " + dbName + "." + schemaName + "." + tableName + " doesnt exist on source database. Please check and retry")
		return -1
	return 0

## Each entity has two tables. One with _REP suffix and one without
## However _REP is obsolete and as of now both stores same data.
## Thought process was _REP will auto pull metadata from source on recurring basis where as one without _REP will be synced from _REP on adhoc basis.

## Create a new row into Schema rep table if doesnt exist.. 

def popSrcSchemaRep(cursor3, cursor2, schemaName, envType, dbName):
	global sql1
	sql1 = "insert into EDW_SCHEMA_REP (environment_name,db_instance_name,db_schema_name,db_schema_alias,schema_type,application_description,create_date,created_by,modify_date,modified_by) values "
	sql1 += "('" + envType + "','" + dbName + "','" + schemaName + "',TRIM(FN_GENERATE_ALIAS('" + schemaName + "')),'DATABASE','Schema from " + dbName + "',sysdate,'DIY',sysdate,'DIY')"
	cursor3.execute(sql1)
	cursor3.execute('commit')
	return 0

## Create a new row into Schema table if doesnt exist.. 

def popSrcSchema(cursor3, cursor2, schemaName, envType, dbName):
	global sql1
	sql1 = "insert into EDW_SCHEMA (environment_name,db_instance_name,db_schema_name,db_schema_alias,schema_type,application_description,create_date,created_by,modify_date,modified_by) values "
	sql1 += "('" + envType + "','" + dbName + "','" + schemaName + "',TRIM(FN_GENERATE_ALIAS('" + schemaName + "')),'DATABASE','Schema from " + dbName + "',sysdate,'DIY',sysdate,'DIY')"
	cursor3.execute(sql1)
	cursor3.execute('commit')
	return 0

## Create a new row into DB instance rep table if doesnt exist.. 

def popDbInstance(cursor3, dbName, envType, dbType):
	global sql1
	sql1 = f""" insert into EDW_DB_INSTANCE
		 (ENVIRONMENT_NAME,DB_INSTANCE_NAME,DB_INSTANCE_ALIAS,DB_TYPE,SOURCE_TYPE,CREATE_DATE,
		  CREATED_BY,MODIFY_DATE,MODIFIED_BY) values 
	 ('{envType}','{dbName}',TRIM(FN_GENERATE_ALIAS('{dbName}')),'{dbType}','OTHER',sysdate,'DIY',sysdate,'DIY')"""
	cursor3.execute(sql1)
	cursor3.execute('commit')
	return 0


## Delete existing metadata before populating the same.

def delete_metadata(cursor3, schemaName, envType, dbName, tableName):
	global sql1
	sql1 = "delete from edw_unique_key_column_rep where table_name='" + tableName + "' and db_instance_name='" + dbName + "'"
	sql1 += " and db_schema_name='" + schemaName + "' and environment_name='" + envType + "'"
	cursor3.execute(sql1)
	sql1 = sql1.replace('edw_unique_key_column_rep', 'edw_unique_key_column')
	cursor3.execute(sql1)
	sql1 = "delete from edw_unique_key_rep where table_name='" + tableName + "' and db_instance_name='" + dbName + "'"
	sql1 += " and db_schema_name='" + schemaName + "' and environment_name='" + envType + "'"
	cursor3.execute(sql1)
	sql1 = sql1.replace('edw_unique_key_rep', 'edw_unique_key')
	cursor3.execute(sql1)
	sql1 = "delete from edw_table_column_rep where table_name='" + tableName + "' and db_instance_name='" + dbName + "'"
	sql1 += " and db_schema_name='" + schemaName + "' and environment_name='" + envType + "'"

	cursor3.execute(sql1)
	sql1 = sql1.replace('edw_table_column_rep', 'edw_table_column')
	# print(sql1)
	cursor3.execute(sql1)
	sql1 = "delete from edw_table_rep where table_name='" + tableName + "' and db_instance_name='" + dbName + "'"
	sql1 += " and db_schema_name='" + schemaName + "' and environment_name='" + envType + "'"
	cursor3.execute(sql1)

	sql1 = sql1.replace('edw_table_rep', 'edw_table')
	cursor3.execute(sql1)
	cursor3.execute('commit')
	print('commited the changes')

## This method is used to copy metadata from base table to view if they are 1-1

def copytblstructure(cursor3, cursor2, schemaName, envType, dbName, tableName, dbType, obType):
	global sql1
	print("Copying table metadata to synonym")
	sql1 = f"select table_owner,table_name from all_synonyms where synonym_name='{tableName}' and owner='{schemaName}'"
	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for obj in results:
		baseSchema = obj[0]
		baseTable = obj[1]
	print(f"Now deleting existing metadata for base table {baseSchema}.{baseTable}")
	delete_metadata(cursor3, baseSchema.upper(), envType.upper(), dbName.upper(), baseTable.upper())
	print(f"Now collecting metadata for base table {baseSchema}.{baseTable}")
	pop_metadata(cursor3, cursor2, baseSchema, envType, dbName, baseTable, dbType)
	print(f"Now deleting existing metadata for {schemaName}.{tableName}")
	delete_metadata(cursor3, schemaName.upper(), envType.upper(), dbName.upper(), tableName.upper())
	print(f"Now copying column metadata for {schemaName}.{tableName} from {baseSchema}.{baseTable}")
	sql1 = "insert into EDW_TABLE_REP (environment_name,db_instance_name,db_schema_name,table_name,table_alias,active_flag,structure_change_flag,table_description,"
	sql1 += "create_date,created_by,modify_date,modified_by,table_type) values "
	sql1 += "('" + envType + "','" + dbName + "','" + schemaName + "','" + tableName + "',FN_GENERATE_ALIAS('" + tableName + "'),'A','Y','Table from " + dbName
	sql1 += "',sysdate,'DIY',sysdate,'DIY','" + obType + "')"
	cursor3.execute(sql1)
	sql1 = sql1.replace('EDW_TABLE_REP', 'EDW_TABLE')
	cursor3.execute(sql1)
	sql1 = "insert into EDW_TABLE_COLUMN_REP(environment_name,db_instance_name,db_schema_name,table_name,column_name,column_sequence,data_type,data_length,data_precision,"
	sql1 += "data_scale,nullable_flag,structure_change_flag,active_flag,create_date,created_by,modify_date,modified_by,default_value,constraint_type,constraint_value) "
	sql1 += f"(select environment_name,db_instance_name,'{schemaName}','{tableName}',column_name,column_sequence,data_type,data_length,data_precision,"
	sql1 += "data_scale,nullable_flag,structure_change_flag,active_flag,sysdate,created_by,sysdate,modified_by,default_value,constraint_type,constraint_value "
	sql1 += f" from EDW_TABLE_COLUMN_REP where environment_name='{envType}' and db_instance_name='{dbName}' and db_schema_name='{baseSchema}' and table_name='{baseTable}')"
	cursor3.execute(sql1)
	sql1 = sql1.replace("EDW_TABLE_COLUMN_REP", "EDW_TABLE_COLUMN")
	cursor3.execute(sql1)
	print(f"Now copying unique key metadata for {schemaName}.{tableName} from {baseSchema}.{baseTable}")
	sql1 = "Insert into EDW_UNIQUE_KEY_REP(environment_name,db_instance_name,db_schema_name,table_name,unique_key_name,active_flag,"
	sql1 += "create_date,created_by,modify_date,modified_by) "
	sql1 += f"(select environment_name,db_instance_name,'{schemaName}','{tableName}',unique_key_name,active_flag,sysdate,created_by,sysdate,modified_by "
	sql1 += f" from EDW_UNIQUE_KEY_REP where environment_name='{envType}' and db_instance_name='{dbName}' and db_schema_name='{baseSchema}' and table_name='{baseTable}')"
	sql1 = sql1.replace('EDW_UNIQUE_KEY_REP', 'EDW_UNIQUE_KEY')
	cursor3.execute(sql1)
	print(f"Now copying unique key column metadata for {schemaName}.{tableName} from {baseSchema}.{baseTable}")
	sql1 = "Insert into EDW_UNIQUE_KEY_COLUMN_REP(environment_name,db_instance_name,db_schema_name,table_name,column_name,unique_key_name,column_position,"
	sql1 += "create_date,created_by,modify_date,modified_by) "
	sql1 += f" (select environment_name,db_instance_name,'{schemaName}','{tableName}',column_name,unique_key_name,column_position,sysdate,created_by,sysdate,modified_by "
	sql1 += f" from EDW_UNIQUE_KEY_COLUMN_REP where environment_name='{envType}' and db_instance_name='{dbName}' and db_schema_name='{baseSchema}' and table_name='{baseTable}')"
	cursor3.execute(sql1)
	sql1 = "Insert into EDW_UNIQUE_KEY_COLUMN(environment_name,db_instance_name,db_schema_name,table_name,column_name,unique_key_name,column_position,"
	sql1 += "create_date,created_by,modify_date,modified_by) "
	sql1 += f" (select environment_name,db_instance_name,'{schemaName}','{tableName}',column_name,unique_key_name,column_position,sysdate,created_by,sysdate,modified_by "
	sql1 += f" from EDW_UNIQUE_KEY_COLUMN where environment_name='{envType}' and db_instance_name='{dbName}' and db_schema_name='{baseSchema}' and table_name='{baseTable}')"
	cursor3.execute(sql1)
	cursor3.execute("commit")

## This method is used to populate metadata from source platform data dictionary

def pop_metadata(cursor3, cursor2, schemaName, envType, dbName, tableName, dbType):
	global sql1
	print("Inserting table info")
	if dbType == 'ORACLE':
		obj_sql = f"select OBJECT_TYPE from ALL_OBJECTS where OWNER='{schemaName}' and OBJECT_NAME='{tableName}' "
	elif dbType == 'TERADATA':
		obj_sql = f"""Select CASE TableKind  WHEN 'T' THEN 'TABLE' WHEN 'V' THEN 'VIEW' 
		ELSE 'OTHER' END TABLE_TYPE FROM DBC.TABLESV where DatabaseName='{schemaName}' and TableName='{tableName}' """
	elif dbType == 'HANA':
		obj_sql = f""" select OBJECT_TYPE from SYS.OBJECTS where SCHEMA_NAME='{schemaName}' and OBJECT_NAME='{tableName}' """
	elif dbType == 'SNOWFLAKE':
		obj_sql=f"select table_type from {dbName}.information_schema.tables where table_schema='{schemaName}' and table_name='{tableName}'"
	cursor2.execute(obj_sql)
	res = cursor2.fetchall()
	objType = ''
	for obj_type in res:
		objType = obj_type[0]
	if dbType == 'HANA' and objType == 'VIEW' and schemaName == '_SYS_BIC':
		hana_vw_sql = f""" select base_schema_name,base_object_name from SYS.object_dependencies where dependent_object_name ='{tableName}'
						and dependent_schema_name = '{schemaName}' and base_object_type='TABLE'  
						and BASE_OBJECT_NAME <> 'BATCHES_TO_PUBLISH' and BASE_SCHEMA_NAME NOT LIKE '%SYS%'"""
		cursor2.execute(hana_vw_sql)
		view_res = cursor2.fetchall()
		if len(view_res) == 1:
			base_table_nm = view_res[0][1]
			base_schema_nm = view_res[0][0]
			sql12 = "select count(1) from EDW_SCHEMA_REP where db_schema_name='" + base_schema_nm + "' and upper(environment_name)='" + envType.upper() + "'"
			sql12 += " and db_instance_name='" + dbName.upper() + "'"
			cursor3.execute(sql12)
			results = cursor3.fetchall()
			for obj in results:
				schemaCnt = obj[0]
			if schemaCnt == 0:
				print("Schema doesnt exist in ODS REP table and hence inserting schema metadata")
				rc = popSrcSchemaRep(cursor3, cursor2, base_schema_nm, envType.upper(), dbName)
				if rc != 0:
					raise Exception("Issue with making new entry in EDW_SCHEMA table")
			sql13 = "select count(1) from EDW_SCHEMA where db_schema_name='" + base_schema_nm + "' and upper(environment_name)='" + envType.upper() + "'"
			sql13 += " and db_instance_name='" + dbName.upper() + "'"
			cursor3.execute(sql13)
			results = cursor3.fetchall()
			for obj in results:
				schemaCnt = obj[0]
			if schemaCnt == 0:
				print("Schema doesnt exist in ODS Base table and hence inserting schema metadata")
				rc = popSrcSchema(cursor3, cursor2, base_schema_nm, envType.upper(), dbName)
				if rc != 0:
					raise Exception("Issue with making new entry in EDW_SCHEMA table")
			print(f"Now deleting metadata for base table {base_schema_nm}.{base_table_nm}")
			delete_metadata(cursor3, base_schema_nm, envType.upper(), dbName, base_table_nm)
			print(f"Now collecting metadata for base table {base_schema_nm}.{base_table_nm}")
			pop_metadata(cursor3, cursor2, base_schema_nm, envType, dbName, base_table_nm, dbType)

	sql1 = "insert into EDW_TABLE_REP (environment_name,db_instance_name,db_schema_name,table_name,table_alias,active_flag,structure_change_flag,table_description,"
	sql1 += "create_date,created_by,modify_date,modified_by,table_type) values "
	sql1 += "('" + envType + "','" + dbName + "','" + schemaName + "','" + tableName + "',FN_GENERATE_ALIAS('" + tableName + "'),'A','Y','Table from " + dbName
	sql1 += "',sysdate,'DIY',sysdate,'DIY','" + objType + "')"


	cursor3.execute(sql1)


	sql1 = sql1.replace('EDW_TABLE_REP', 'EDW_TABLE')

	cursor3.execute(sql1)
	cursor3.execute('commit')

	# pdb.set_trace()
	print("Inserting table column info")
	if dbType == 'ORACLE':
		print("Checking if passed object is view")
		sql1 = f"""select object_type from all_objects where object_name='{tableName}' and owner='{schemaName}'"""
		cursor2.execute(sql1)
		results = cursor2.fetchall()
		for obj in results:
			objType = obj[0]
		if objType == 'SYNONYM':
			copytblstructure(cursor3, cursor2, schemaName, envType, dbName, tableName, dbType, objType)
			sys.exit(0)
		else:

			# sql1="select column_name,column_id,data_type,data_length,data_precision,data_scale,nullable from all_tab_columns where owner='" + schemaName + "'"
			# sql1 += " and table_name='" + tableName + "' order by column_id"
			sql1 = f""" SELECT COLUMN_NAME,COLUMN_ID,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE ,data_default , null as CONSTRAINT_TYPE, 
					   null as CONSTRAINT_VALUE FROM ALL_TAB_COLUMNS where owner= '{schemaName}' and table_name='{tableName}' order by column_id"""



	elif dbType == 'TERADATA':
		sql1 = f"""select c.column_name,column_sequence_int,data_type_code,
								   data_type_character_width_int,data_type_deciml_precision_int,data_type_decimal_scale_int, nvl(NULL_ALLOWED_FLAG,'Y') null_allowed_flag,trim(OReplace(default_value_text, '''', '')) AS default_value , CASE WHEN I.IndexType in ('P','Q') and UniqueFlag ='Y'  THEN  'UNIQUE PRIMARY INDEX'
								   WHEN I.IndexType in ('P','Q') and UniqueFlag ='N' THEN 'NON UNIQUE PRIMARY INDEX'
								   ELSE NULL END constraint_type, case when constraint_type is not null then c.column_name   else null end as constraint_value
							   from etloffloaddb.dbc_columns c
							   LEFT JOIN dbc.indicesv I
							   ON (i.databasename = c.database_name and i.tablename = c.table_name and i.columnname = c.column_name and I.indextype in ('Q','P' ) )
								   where c.database_name='{schemaName}' and c.table_name='{tableName}' order by COLUMN_SEQUENCE_INT"""
	elif dbType == 'SNOWFLAKE':
		sql1 = f"""select column_name,ordinal_position,data_type, character_maximum_length,numeric_precision,numeric_scale, substr(IS_NULLABLE,1,1) null_allowed_flag,
								null as default_value,null as CONSTRAINT_TYPE,null as CONSTRAINT_VALUE 
							   from {dbName}.information_schema.columns where table_name='{tableName}' and table_schema='{schemaName}' order by ordinal_position"""
								   
	elif dbType == 'HANA':
		sql1 = f""" select column_name,  position as column_sequence,  data_type_name as data_type,  length as data_length,
				 length as data_precision,  scale AS data_scale,  CASE when is_nullable = 'TRUE' THEN 'Y' ELSE 'N' END AS nullable_flag,
				 DEFAULT_VALUE , NULL as constraint_type, NULL as constraint_value from (select colstage.schema_name, colstage.table_name,colstage.column_name,
			   colstage.position,data_type_name, length, scale, is_nullable, DEFAULT_VALUE
	   from  ( select distinct cscol.schema_name, cscol.table_name as table_name, cscol.column_name,cscol.position,
					cscol.data_type_name, cscol.length,cscol.scale, is_nullable, DEFAULT_VALUE
					from SYS.TABLE_COLUMNS cscol
					  union all
					select distinct cscol.schema_name, cscol.view_name as table_name, cscol.column_name,cscol.position,
					cscol.data_type_name, cscol.length,cscol.scale, is_nullable,DEFAULT_VALUE
					from SYS.VIEW_COLUMNS cscol   ) as   colstage ) WHERE schema_name = '{schemaName}'AND table_name = '{tableName}'order by schema_name,table_name,position"""

	cursor2.execute(sql1)
	results = cursor2.fetchall()
	for colObj in results:
		colName = colObj[0]
		colId = colObj[1]
		dataType = colObj[2]
		dataLen = colObj[3]
		if dataLen is None:
			dataLen = 'NULL'
		dataPre = colObj[4]
		if dataPre is None:
			if dataType == 'NUMBER' and dbType == 'ORACLE':
				dataPre = 38
			else:
				dataPre = 'NULL'
		dataSca = colObj[5]
		if dataSca is None:
			if dataType == 'NUMBER' and dbType == 'ORACLE':
				#num_digit=derive_data_scale(cursor2,colName,schemaName,tableName)
				#if num_digit == 0 :
				#	dataSca='NULL'
				#elif num_digit <= 5:
				#	dataSca = 5
				#else:
				dataType ='FLOAT'
				dataSca='NULL'
			else:
				dataSca = 'NULL'
		if dataType == 'VARCHAR2' and dataLen == 0:
			dataLen = 1  ## To fix a bug with Snowflake since Snowflake cannot have 0 length Varchar fields
		nullable = colObj[6]
		if dbType == 'TERADATA':
			if "TIMESTAMP(" in dataType and "WITH TIMEZONE" in dataType:
				dataType = "TIMESTAMP WITH TIMEZONE"
			elif "TIMESTAMP(" in dataType:
				dataType = "TIMESTAMP"
			elif "TIME(" in dataType:
				dataType = "TIME"
		
		matches = ["TO_NUMBER", "TO_CHAR", "TO_DATE"]  
		null_matchs = ["SUBSTRB","USERENV"]
		if colObj[7] is None:
		   vdfault ='NULL'		
		elif any(x in colObj[7].upper() for x in null_matchs):
			vdfault = 'NULL'
		elif any(x in colObj[7].upper() for x in matches):
		   vdfault=colObj[7].replace("'","''")   
		else :
		   vdfault=colObj[7].replace("'","").replace('"','')
		   
		   
		vcontype = colObj[8]
		if vcontype is None:
			vcontype = 'NULL'
		vconval = colObj[9]
		if vconval is None:
			vconval = 'NULL'
		sql1 = "insert into EDW_TABLE_COLUMN_REP (environment_name,db_instance_name,db_schema_name,table_name,column_name,column_sequence,data_type,data_length,data_precision,"
		sql1 += "data_scale,nullable_flag,structure_change_flag,active_flag,create_date,created_by,modify_date,modified_by,default_value,constraint_type,constraint_value) values "
		sql1 += "('" + envType + "','" + dbName + "','" + schemaName + "','" + tableName + "','" + colName + "'," + str(colId) + ",'" + dataType + "'," + str(dataLen)
		sql1 += "," + str(dataPre) + "," + str(
		dataSca) + ",'" + nullable + "','N','A',sysdate,'DIY',sysdate,'DIY','" + vdfault + "','" + vcontype + "','" + vconval + "')"
		
		cursor3.execute(sql1)
		#print(sql1)
		sql1 = sql1.replace("EDW_TABLE_COLUMN_REP", "EDW_TABLE_COLUMN")
		cursor3.execute(sql1)
	print("Inserting unique key info")
	if dbType == 'ORACLE':
		sql2 = "select index_name,owner from all_indexes a where uniqueness='UNIQUE' and index_type='NORMAL' and table_owner='" + schemaName + "' and table_name='" + tableName + "'"
		sql2 += " and not exists (select 1 from all_ind_columns where column_name='M_ROW$$' and index_name=a.index_name and index_owner=owner)"
		sql1 =  "select a.index_name,a.owner from all_indexes a, all_constraints b where uniqueness='UNIQUE' and index_type='NORMAL' and b.constraint_type='P'"
		sql1 += " and a.table_owner='" + schemaName + "' and a.table_name='" + tableName + "'"
		sql1 += " and not exists (select 1 from all_ind_columns where column_name='M_ROW$$' and index_name=a.index_name and index_owner=a.owner)"
		sql1 += " and a.index_name=b.index_name and a.owner=b.owner and a.table_name=b.table_name"
	elif dbType == 'TERADATA':
		sql1 = "select distinct nvl(indexname, table_name || '_UK' || '_' || indexnumber) as index_name,indexnumber from ETLOFFLOADDB.DBC_COLUMNS_NEW where database_name='"
		sql1 += schemaName + "' and table_name='" + tableName + "' and UNIQUE_KEY_FLAG = 'Y'"
	elif dbType == 'HANA':
		if objType == 'VIEW':
			obj_sql = f""" select base_schema_name,base_object_name from SYS.object_dependencies where dependent_object_name ='{tableName}'
						and dependent_schema_name = '{schemaName}' and base_object_type='TABLE'
						and BASE_OBJECT_NAME <> 'BATCHES_TO_PUBLISH' and BASE_SCHEMA_NAME NOT LIKE '%SYS%'"""
			cursor2.execute(obj_sql)
			res = cursor2.fetchall()

			if len(res) == 1:
				base_object_name = res[0][1]
				base_schema_name = res[0][0]
			else:
				base_object_name = tableName
				base_schema_name = schemaName
		else:
			base_object_name = tableName
			base_schema_name = schemaName
		sql1 = f"""select  distinct constraint_name as unique_key_name,  1 as column_position from sys.constraints 
						WHERE schema_name = '{base_schema_name}' AND table_name = '{base_object_name}' 
						order by constraint_name,column_position """
	if dbType != 'SNOWFLAKE':
		cursor2.execute(sql1)
	results = cursor2.fetchall()
	if len(results) == 0 and dbType == 'ORACLE':
		cursor2.execute(sql2)
		results = cursor2.fetchall()
	activeFlag = 'A'
	for indObj in results:
		indName = indObj[0]
		indOwner = indObj[1]
		sql1 = "Insert into EDW_UNIQUE_KEY_REP(environment_name,db_instance_name,db_schema_name,table_name,unique_key_name,active_flag,"
		sql1 += "create_date,created_by,modify_date,modified_by) values "
		sql1 += "('" + envType + "','" + dbName + "','" + schemaName + "','" + tableName + "','" + indName + "','" + activeFlag + "',sysdate,'DIY',sysdate,'DIY')"
		cursor3.execute(sql1)
		sql1 = sql1.replace('EDW_UNIQUE_KEY_REP', 'EDW_UNIQUE_KEY')
		cursor3.execute(sql1)
		activeFlag = 'I'
		print("Inserting unique key column info for Index - " + indName)
		if dbType == 'ORACLE':
			sql1 = "select column_name,column_position from all_ind_columns where index_owner='" + indOwner + "' and index_name='" + indName + "' order by column_position"
		elif dbType == 'TERADATA':
			sql1 = "select column_name,primary_key_sequence_int from ETLOFFLOADDB.DBC_COLUMNS_NEW where database_name='" + schemaName + "' and table_name='" + tableName + "'"
			sql1 += " and indexnumber=" + str(indOwner) + " order by primary_key_sequence_int"
		elif dbType == 'HANA':
			if objType == 'VIEW':
				obj_sql = f""" select base_schema_name,base_object_name from SYS.object_dependencies where dependent_object_name ='{tableName}'
							and dependent_schema_name = '{schemaName}' and base_object_type='TABLE' 
							and BASE_OBJECT_NAME <> 'BATCHES_TO_PUBLISH' and BASE_SCHEMA_NAME NOT LIKE '%SYS%'"""
				cursor2.execute(obj_sql)
				res = cursor2.fetchall()

				if len(res) == 1:
					base_object_name = res[0][1]
					base_schema_name = res[0][0]
				else:
					base_object_name = tableName
					base_schema_name = schemaName
			else:
				base_object_name = tableName
				base_schema_name = schemaName
			sql1 = f""" select column_name, position as column_position from sys.constraints WHERE schema_name = '{base_schema_name}'
				  AND table_name = '{base_object_name}' and constraint_name='{indName}' 
				  order by schema_name,table_name,constraint_name,position """
		if dbType != 'SNOWFLAKE':
			cursor2.execute(sql1)
		results2 = cursor2.fetchall()
		for indColobj in results2:
			indColName = indColobj[0]
			indColPos = indColobj[1]
			sql1 = "Insert into EDW_UNIQUE_KEY_COLUMN_REP(environment_name,db_instance_name,db_schema_name,table_name,column_name,unique_key_name,column_position,"
			sql1 += "create_date,created_by,modify_date,modified_by) values "
			sql1 += "('" + envType + "','" + dbName + "','" + schemaName + "','" + tableName + "','" + indColName + "','" + indName + "'," + str(indColPos)
			sql1 += ",sysdate,'DIY',sysdate,'DIY')"
			cursor3.execute(sql1)
			sql1 = "Insert into EDW_UNIQUE_KEY_COLUMN(environment_name,db_instance_name,db_schema_name,table_name,column_name,unique_key_name,column_position,"
			sql1 += "create_date,created_by,modify_date,modified_by) values "
			sql1 += "('" + envType + "','" + dbName + "','" + schemaName + "','" + tableName + "','" + indColName + "','" + indName + "'," + str(indColPos)
			sql1 += ",sysdate,'DIY',sysdate,'DIY')"
			cursor3.execute(sql1)
	cursor3.execute('commit')


## Main program starts here
ejcparams = {}
sql1 = ""
configFile = ConfigObject(filename='/apps/edwsfdata/python/config/DIY_src2stg.conf')
BASE_DIR = configFile.Config.BASE_DIR
parser = argparse.ArgumentParser(description='Script to populate metadata',
								 epilog='Example: python DIY_metadata.py -e env_type -d db_name -s schema_name -t table_name -c connection_name')
parser.add_argument('-e', '--env', required=True,
					help='Envrionment type (TS*/DV*/PRD) for which metadata to be collected')
parser.add_argument('-d', '--dbName', required=True,
					help='Database containing the table whose metadata to be collected')
parser.add_argument('-s', '--schemaName', required=True,
					help='Schema containing the table whose metadata to be collected')
parser.add_argument('-t', '--table', required=True,
					help='Table name whose metadata to be collected')
parser.add_argument('-c', '--conn', required=True,
					help='Connection as present in edw_parameter for source database where table is located')
args = parser.parse_args()
envType = args.env
dbName = args.dbName
schemaName = args.schemaName
tableName = args.table
connectionName = args.conn
set_env()
try:
	conn = open_oracle_connection(envType,None,True)
except Exception as e:
	print("Error occurred while connecting to Repository database")
	print(str(e))
	sys.exit(2)

try:
	## Certain metadata can be prevent from over-writing by making entry into EDW_DISABLE_META_COLLECTION table.
	## This is primarily done if custom entries are made to metadata table. In such case exception entry needs to be disabled first manually before getting entered.
	## Extract source connection details
	stepName = "Fetching Source DB Connection details"
	srcdbParams = {}
	cursor1 = conn.cursor()
	print('Checking for manual override flag')
	sql1 = f"select count(*) cnt from eds_data_catalog.edw_disable_meta_collection where upper(environment_name)=upper('{envType}') and upper(db_instance_name)=upper('{dbName}')"
	sql1 += f" and upper(db_schema_name)=upper('{schemaName}') and table_name='{tableName}' and active_ind='Y'"
	cursor1.execute(sql1)
	results = cursor1.fetchall()
	for results_obj in results:
		override_cnt = results_obj[0]
	if override_cnt == 1:
		print(
			f"Metadata collection is disabled for {envType} - {dbName}.{schemaName}.{tableName}..Hence not proceeding further")
		sys.exit(0)
	## Connection to source datbase is made to collect metadata. 
	print('Gathering source db connection detais from repository')
	sql1 = "select parameter_name,to_char(parameter_value) from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "' and parameter_type ='" + connectionName + "'"
	cursor1.execute(sql1)
	results = cursor1.fetchall()
	if len(results) == 0:
		print("Could not find connection details for connection name - " + connectionName.strip(
			'\n') + " in Repository database.. Hence cannot proceed with metadata collection")
		sys.exit(3)
	for obj in results:
		param = obj[0]
		value = obj[1]
		srcdbParams[param] = value
	stepName = "Fetching source DB Password"
	sql1 = "SELECT UTL_I18N.RAW_TO_CHAR('" + srcdbParams['SOURCE_LOGIN_PASSWORD'] + "','AL32UTF8') from dual"
	cursor1.execute(sql1)
	results = cursor1.fetchall()
	for passobj in results:
		srcPassword = passobj[0]
	stepName = "Fetching source db type"
	sql1 = "select distinct decode(parameter_category,'HANANRT','HANA',parameter_category) from EDS_DATA_CATALOG.EDW_PARAMETER  where upper(environment_name)='" + envType.upper() + "' and parameter_type ='" + connectionName + "'"
	cursor1.execute(sql1)
	results = cursor1.fetchall()
	for obj in results:
		dbType = obj[0]
	stepName = "Connecting to Repos as Catalog User"
	# conn3 = cx_Oracle.connect(user='EDS_DATA_CATALOG', password='Cata10g_917', dsn=connstr)
	conn3 = cx_Oracle.connect('EDS_DATA_CATALOG', 'Cata10g_917', 'ODSPROD.CISCO.COM')
	cursor3 = conn3.cursor()
	if dbType.upper() == "ORACLE":
		print("Now attempting to collect metadata for a table which is of type Oracle database")
		stepName = "Connecting to source DB"
		connstr2 = cx_Oracle.makedsn(srcdbParams['SOURCE_HOST'], srcdbParams['SOURCE_PORT'],
									 service_name=srcdbParams['SOURCE_SERVICE_NAME'])
		# conn2 = cx_Oracle.connect(user=srcdbParams['SOURCE_LOGIN'], password=srcPassword, dsn=connstr2)
		conn2 = cx_Oracle.connect(srcdbParams['SOURCE_LOGIN'], srcPassword, srcdbParams['SOURCE_SERVICE_NAME'])
		cursor2 = conn2.cursor()
		stepName = "Validating passed parameters"
		rc = validate_orcl_params(cursor2, schemaName.upper(), dbName.upper(), tableName.upper())
		if rc != 0:
			raise Exception("Invalid input parameters. Please check and retry")
	elif dbType.upper() == "TERADATA":
		print("Now attempting to collect metadata for a table which is of type Teradata database")
		stepName = "Connecting to source DB"
		# udaexec = teradata.UdaExec(appName="ModuleName", version="0.1",logConsole=False,odbcLibPath="/opt/teradata/client/ODBC_64/lib/libodbc.so",runNumberFile="/apps/edwsfdata/python/pylogs/.runNumber",logDir="/apps/edwsfdata/python/pylogs/logs")
		# conn2 = udaexec.connect(method="odbc", DSN="tdprodetloffload")
		conn2 = teradatasql.connect(host=srcdbParams['SOURCE_HOST'], user=srcdbParams['SOURCE_LOGIN'],
									password=srcPassword)
		cursor2 = conn2.cursor()
		stepName = "Validating passed parameters"
		rc = validate_td_params(cursor2, schemaName.upper(), dbName.upper(), tableName.upper())
		if rc != 0:
			raise Exception("Invalid input parameters. Please check and retry")
	elif dbType.upper() == 'HANA':
		print("Now attempting to collect metadata for a table which is of type HANA database")
		stepName = "Connecting to source DB"
		conn2 = open_hana_connection(srcdbParams['SOURCE_HOST'].split(';')[0].split(':')[0],
									 srcdbParams['SOURCE_PORT'],
									 srcdbParams['SOURCE_LOGIN'],
									 srcPassword,
									 addlnParam=srcdbParams.get('SOURCE_ADDITIONAL_PARAMS'))
		cursor2 = conn2.cursor()
		stepName = "Validating passed parameters"
		rc = validate_hana_params(cursor2, schemaName, dbName, tableName)
		if rc != 0:
			raise Exception("Invalid input parameters. Please check and retry")
	elif dbType.upper() == 'SNOWFLAKE':
		print("Now attempting to collect metadata for a table which is of type Snowflake database")
		stepName = "Connecting to source DB"
		#conn2=snowflake.connector.connect(user=srcdbParams['SOURCE_LOGIN'],password=srcPassword,account=srcdbParams['ACCOUNT'],warehouse=srcdbParams['WAREHOUSE'])
		if 'sf_query_tag' in os.environ:
			sf_query_tag=os.environ["sf_query_tag"].replace("SRC2STG","METADATA")
		else:
			sf_query_tag=None
		conn2=open_sf_connection(srcdbParams['ACCOUNT'],srcdbParams['SOURCE_LOGIN'],query_tag=sf_query_tag)
		cursor2 = conn2.cursor()
		stepName = "Validating passed parameters"
		rc = validate_snowflake_params(cursor2, schemaName, dbName, tableName)
		if rc != 0:
			raise Exception("Invalid input parameters. Please check and retry")
	stepName = "Pulling and validating Schema Name in EDW_SCHEMA_REP"
	sql1 = "select count(1) from EDW_SCHEMA_REP where db_schema_name='" + schemaName + "' and upper(environment_name)='" + envType.upper() + "'"
	sql1 += " and db_instance_name='" + dbName.upper() + "'"
	cursor3.execute(sql1)
	results = cursor3.fetchall()
	for obj in results:
		schemaCnt = obj[0]
	if schemaCnt == 0:
		print("Schema doesnt exist in ODS REP table and hence inserting schema metadata")
		rc = popSrcSchemaRep(cursor3, cursor2, schemaName.upper(), envType.upper(), dbName.upper())
		if rc != 0:
			raise Exception("Issue with making new entry in EDW_SCHEMA table")
	stepName = "Pulling and validating Schema Name in EDW_SCHEMA"
	sql1 = "select count(1) from EDW_SCHEMA where db_schema_name='" + schemaName + "' and upper(environment_name)='" + envType.upper() + "'"
	sql1 += " and db_instance_name='" + dbName.upper() + "'"
	cursor3.execute(sql1)
	results = cursor3.fetchall()
	for obj in results:
		schemaCnt = obj[0]
	if schemaCnt == 0:
		print("Schema doesnt exist in ODS Base table and hence inserting schema metadata")
		rc = popSrcSchema(cursor3, cursor2, schemaName.upper(), envType.upper(), dbName.upper())
		if rc != 0:
			raise Exception("Issue with making new entry in EDW_SCHEMA table")
	stepName = "Pulling and Validating Database Name in EDW_DB_INSTANCE"
	sql1 = f""" select count(1) from  EDW_DB_INSTANCE where environment_name='{envType}' 
				and DB_INSTANCE_NAME='{dbName}' and DB_TYPE='{dbType}'"""
	cursor3.execute(sql1)
	results = cursor3.fetchall()
	for obj in results:
		dbCnt = obj[0]
	if dbCnt == 0:
		print("Database doesnt exist in ODS Base table and hence inserting database metadata")
		rc = popDbInstance(cursor3, dbName.upper(), envType.upper(), dbType.upper())
		if rc != 0:
			raise Exception("Issue with making new entry in EDW_DB_INSTANCE table")

	stepName = "Deleting data"
	print("Now deleting existing metadata")
	delete_metadata(cursor3, schemaName, envType.upper(), dbName, tableName)
	print("Now populating metadata")
	stepName = "Populating metadata"
	pop_metadata(cursor3, cursor2, schemaName, envType.upper(), dbName, tableName, dbType.upper())
	print("Completed populating metadata")
	cursor1.close()
	cursor2.close()
	cursor3.close()
	conn.close()
	conn2.close()
	conn3.close()
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Error occurred while populating metadata at  line {linenu} in step - " + stepName)
	print("Problematic query could be - " + sql1)
	print(str(e))
	cursor1.close()
	cursor2.close()
	cursor3.close()
	conn.close()
	conn2.close()
	conn3.close()
	sys.exit(7)






