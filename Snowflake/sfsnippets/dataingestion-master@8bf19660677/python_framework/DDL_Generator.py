#!/apps/python/install/bin/python
#############################################################################################################################################################
# DDL_Generator.py                                                                                                                                          #
#Modification History                                                                                                                                       #
# Version               Date                    Author                  Change description                                                                  #
# 1.0                   Feb-2019                Manick                  Initial Version                                                                     #
# 1.1                   Feb-2019                Manick                  Enable function call to use as module                                               #
# 1.2                   Mar-2019                Manick                  Include Record_Delete_flag column                                                   #
# 1.3                   Feb-2020                Manick                  Include default value for EDWSF_SOURCE_DELETED_FLAG                                 #
# 1.4                   Apr-2020                Raveendra               Include default values and Not Null constraint to table DDL definition              #
# 1.5                   Apr-2020                Manick                  Fix if default comes as "NULL" string                                               #
# 1.6                   May-2020                Raveendra               Fix where default comes as number for char field and erroring while creating table  #
# 1.7                   May-2020                Raveendra               Fix default values of oracle sysdate and user                                       #
# 1.8                   May-2020                Raveendra               Fix default value containing to_number, to_char in it - oracle db                   #
# 1.9                   May-2020                Raveendra               Fix default value containing new line character for oracle                          #
# 1.10                  JUN-2020                Raveendra               Fix TIMETSAMP_NTZ(0) defaul value handling                                          #
# 1.11                  JAN-2021                Manick                  Use explicit data type conversion if type is of date for default value from TD      #
#############################################################################################################################################################

import cx_Oracle
import os, sys
import getopt


def set_env():
    os.environ['LD_LIBRARY_PATH'] = "/usr/cisco/packages/oracle/oracle-12.1.0.2/lib"
    os.environ['ORACLE_HOME'] = "/usr/cisco/packages/oracle/oracle-12.1.0.2"


def validate_table(dbName, schemaName, tblName, cursor1):
    # print("Passed parameters are \n" + dbName + "\n" + schemaName + "\n" + tblName)
    cursor1.execute(
        "select count(1) from EDS_DATA_CATALOG.EDW_TABLE where db_instance_name='" + dbName + "' and db_schema_name='" + schemaName + "' and table_name='" + tblName + "'")
    results = cursor1.fetchall()
    for cnt in results:
        tblcnt = cnt[0]
    if tblcnt == 0:
        raise Exception("Invalid Table. Please check and re-execute the program")
    cursor1.execute(
        "select count(1) from EDS_DATA_CATALOG.EDW_TABLE_COLUMN where db_instance_name='" + dbName + "' and db_schema_name='" + schemaName + "' and table_name='" + tblName + "'")
    results = cursor1.fetchall()
    for cnt in results:
        colcnt = cnt[0]
    if colcnt == 0:
        raise Exception(
            "Missing column information for table - " + tblName + ". Please check and re-execute the program")


def pull_dbtype(dbName, cursor1):
    cursor1.execute("select DB_TYPE from eds_data_catalog.EDW_DB_INSTANCE where db_instance_name='" + dbName + "'")
    results = cursor1.fetchall()
    for cnt in results:
        dbType = cnt[0]
    if dbType == "":
        raise Exception(
            " Database " + dbName + " is of unknown DB Type. Please check config tables and re-execute the program")
    else:
        return dbType


def getoracleddl(dbName, schemaName, tblName, dbType, cursor1, cursor2, whocol, crmode):
    envName = os.environ['RUN_ENVIRONMENT']
    if crmode.upper() == "REPLACE":
        snowDDL = "create or replace table " + tblName + "\n(\n"
    else:
        snowDDL = "create table " + tblName + "\n(\n"
    cursor1.execute(
        "select COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE_FLAG,NULLIF(rtrim(case when DEFAULT_VALUE like '%NEXTVAL%' THEN 'NULL' else DEFAULT_VALUE END),NULL) as DEFAULT_VALUE from EDS_DATA_CATALOG.EDW_TABLE_COLUMN"
        " where db_instance_name='" + dbName + "' and db_schema_name='" + schemaName + "' and table_name='" + tblName + "' "
                                                                                                                        " and ENVIRONMENT_NAME='" + envName + "' order by column_sequence")

    results = cursor1.fetchall()
    for cols in results:
        vcolname = cols[0]
        vdatatype = cols[1]
        vdatalen = cols[2]
        vdatapre = cols[3]
        vdatasca = cols[4]
        vnullflg = cols[5]
        vdfault = cols[6]
        if vdfault is not None:
            vdfault = cols[6].replace('\n','')
        if vdfault in ["sys_context('USERENV','OS_USER')", "User","user","USER"]:
            vdfault = 'DEFAULT CURRENT_USER()'
        elif vdfault in ['SYSDATE','sysdate', 'Current TimeStamp(0)','CURRENT_TIMESTAMP(0)','current_timestamp(0)']:
            vdfault = 'DEFAULT CURRENT_TIMESTAMP()'
        else:
            vdfault = vdfault

        vtgttype = ""
        cursor2.execute(
            "select target_data_type from EDS_DATA_CATALOG.EDW_DATA_TYPE_MAPPING where source_db_type='" + dbType + "' and target_db_type='SNOWFLAKE' and source_data_type='" + vdatatype + "'")
        results2 = cursor2.fetchall()
        for dtype in results2:
            vtgttype = dtype[0]
        if 'NTZ' in vtgttype and vdfault is not None and vdfault !='NULL':
            vdfault= 'DEFAULT CAST(CURRENT_TIMESTAMP() as TIMESTAMP_NTZ(0))'
        if vtgttype == "":
            raise Exception(
                " Snowflake data type mapping for datatype - " + vdatatype + " doesnt exist on config tables.. Please check and re execute")
        if vdatatype.upper() == "NUMBER":
            if vdatapre is not None:
                if vdatasca is not None:
                    prestr = "(" + str(vdatapre) + "," + str(vdatasca) + ")"
                else:
                    prestr = "(" + str(vdatapre) + ")"
            else:
                prestr = ""
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + prestr
            # snowDDL = snowDDL + " " + vcolname + " " + vtgttype + "(" + str(vdatapre) + "," + str(vdatasca) + "),\n"
        elif vdatatype.upper() == "TIMESTAMP":
            if vdatasca is not None:
                snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(" + str(vdatasca) + ")"
            else:
                snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype
        elif vdatatype.upper() == "VARCHAR2" or vdatatype.upper() == "VARCHAR" or vdatatype.upper() == "NVARCHAR2" or vdatatype.upper() == "CHAR" or vdatatype.upper() == "RAW" or vdatatype.upper() == "BINARY":
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(" + str(vdatalen) + ")"
        else:
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype
        if vnullflg == 'N':
            vnullflg = 'NOT NULL'
            snowDDL = snowDDL + ' ' + vnullflg
        matches = ["TO_NUMBER", "TO_CHAR", "TO_DATE"]
        if vdfault != 'NULL' and vdfault is not None:
            if vdfault.lstrip('-').replace('.', '').isdigit():
                snowDDL = snowDDL + ' DEFAULT ' + vdfault
            elif any(x in vdfault.upper() for x in matches):
                  snowDDL = snowDDL + ' DEFAULT ' + vdfault
            elif vdfault.upper() in ['DEFAULT CURRENT_USER()', 'DEFAULT CURRENT_TIMESTAMP()','DEFAULT CAST(CURRENT_TIMESTAMP() as TIMESTAMP_NTZ(0))']:
                snowDDL = snowDDL + ' ' + vdfault
            else:
                snowDDL = snowDDL + ' DEFAULT ' + "'" + vdfault + "'"
        snowDDL = snowDDL + ", \n"
    snowDDL = snowDDL + ' ' + whocol
    snowDDL = snowDDL[:-2] + "\n"
    if crmode.upper() == "CREATE":
        snowDDL = snowDDL + ") if not exists; \n"
    else:
        snowDDL = snowDDL + ");\n"
    return snowDDL


def addwhocol():
    whocollist = "\"EDWSF_BATCH_ID\" NUMBER NOT NULL DEFAULT TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP(),'YYYYMMDDHH24MISS')),\n "
    whocollist = whocollist + "\"EDWSF_CREATE_DTM\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()::timestamp_ntz,\n "
    whocollist = whocollist + "\"EDWSF_CREATE_USER\" VARCHAR(50) NOT NULL DEFAULT CURRENT_USER(),\n "
    whocollist = whocollist + "\"EDWSF_UPDATE_DTM\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()::timestamp_ntz,\n "
    whocollist = whocollist + "\"EDWSF_UPDATE_USER\" VARCHAR(50) NOT NULL DEFAULT CURRENT_USER(),\n "
    whocollist = whocollist + "\"EDWSF_SOURCE_DELETED_FLAG\" CHAR(1) NOT NULL DEFAULT 'N' \n "
    return whocollist


def gettdddl(dbName, schemaName, tblName, dbType, cursor1, cursor2, whocol, crmode):
    envName = os.environ['RUN_ENVIRONMENT']

    if crmode.upper() == "REPLACE":
        snowDDL = "create or replace table " + tblName + "\n(\n"
    else:
        snowDDL = "create table " + tblName + "\n(\n"
    cursor1.execute(
        "select COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE_FLAG,case when DEFAULT_VALUE like '%NEXTVAL%' THEN 'NULL' when default_value like '%-%-%' and data_type='DATE' then '''' || default_value || '''::date'  else DEFAULT_VALUE end as DEFAULT_VALUE from EDS_DATA_CATALOG.EDW_TABLE_COLUMN where db_instance_name='" + dbName + "' and db_schema_name='" + schemaName + "' and table_name='" + tblName + "'  and environment_name = '" + envName + "' order by column_sequence")
    results = cursor1.fetchall()
    for cols in results:
        vcolname = cols[0]
        vdatatype = cols[1]
        vdatalen = cols[2]
        vdatapre = cols[3]
        vdatasca = cols[4]
        vnullflg = cols[5]
        vdfault = cols[6]
        if vdfault in ["sys_context('USERENV','OS_USER')", "User","USER"]:
            vdfault = 'DEFAULT CURRENT_USER()'
        elif vdfault in ['SYSDATE', 'Current TimeStamp(0)','CURRENT_TIMESTAMP(0)','current_timestamp(0)']:
            vdfault = 'DEFAULT CURRENT_TIMESTAMP()'
        elif vdfault == '3500-01-01 00:00:00':
            vdfault = 'DEFAULT cast(\'3500-01-01 00:00:00\' as TIMESTAMP_NTZ(0))'
        elif vdfault == '3500-01-01':
            vdfault = 'DEFAULT to_date(\'3500-01-01\')'
        else:
            vdfault = vdfault
        vtgttype = ""
        cursor2.execute(
            "select target_data_type from EDS_DATA_CATALOG.EDW_DATA_TYPE_MAPPING where source_db_type='" + dbType + "' and target_db_type='SNOWFLAKE' and source_data_type='" + vdatatype + "'")
        results2 = cursor2.fetchall()
        for dtype in results2:
            vtgttype = dtype[0]
        if 'NTZ' in vtgttype and vdfault is not None and vdfault !='NULL':
            vdfault= 'DEFAULT CAST(CURRENT_TIMESTAMP() as TIMESTAMP_NTZ(0))'
        if vtgttype == "":
            raise Exception(
                " Snowflake data type mapping for datatype - " + vdatatype + " doesnt exist on config tables.. Please check and re execute")
        if vdatatype.upper() == "NUMBER" or vdatatype.upper() == "DECIMAL":
            if vdatapre is not None and vdatapre != -128:
                if vdatasca is not None and vdatapre != -128:
                    prestr = "(" + str(vdatapre) + "," + str(vdatasca) + ")"
                else:
                    prestr = "(" + str(vdatapre) + ")"
            else:
                prestr = ""
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + prestr
            # snowDDL = snowDDL + " " + vcolname + " " + vtgttype + "(" + str(vdatapre) + "," + str(vdatasca) + "),\n"
        elif vdatatype.upper() == "TIMESTAMP":
            if vdatasca is not None:
                snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(" + str(vdatasca) + ")"
            else:
                snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype
        elif vdatatype.upper() == "VARCHAR" or vdatatype.upper() == "CHAR" or vdatatype.upper() == "VARBYTE":
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(" + str(vdatalen) + ")"
        elif vdatatype.upper() == "CLOB":
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(16777216)"
        elif vdatatype.upper() == "BYTE":
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(256)"
        else:
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype
        if vnullflg == 'N':
            vnullflg = 'NOT NULL'
            snowDDL = snowDDL + ' ' + vnullflg
        if vdfault != 'NULL' and vdfault is not None:
            if vdfault.lstrip('-').replace('.', '').isdigit():
                snowDDL = snowDDL + ' DEFAULT ' + vdfault
            elif vdfault in ['DEFAULT CURRENT_USER()', 'DEFAULT CURRENT_TIMESTAMP()',
                             'DEFAULT cast(\'3500-01-01 00:00:00\' as TIMESTAMP_NTZ(0))',
                             'DEFAULT to_date(\'3500-01-01\')','DEFAULT CAST(CURRENT_TIMESTAMP() as TIMESTAMP_NTZ(0))']:
                snowDDL = snowDDL + ' ' + vdfault
            elif '::date' in vdfault:
                snowDDL = snowDDL + ' DEFAULT ' + vdfault
            else:
                snowDDL = snowDDL + ' DEFAULT ' + "'" + vdfault + "'"
        snowDDL = snowDDL + ", \n"
    snowDDL = snowDDL + ' ' + whocol
    snowDDL = snowDDL[:-2] + "\n"
    if crmode.upper() == "CREATE":
        snowDDL = snowDDL + ") if not exists; \n"
    else:
        snowDDL = snowDDL + ");\n"
    return snowDDL

def gethanaddl(dbName, schemaName, tblName, dbType, cursor1, cursor2, whocol, crmode):
    envName = os.environ['RUN_ENVIRONMENT']
    if '/' in tblName:
        table_name=tblName.split('/')[1]
    elif '::' in tblName:
        table_name=tblName.split('::')[1] 
    else :
        table_name=tblName
    
    if crmode.upper() == "REPLACE":
        snowDDL = "create or replace table " + table_name + "\n(\n"
    else:
        snowDDL = "create table " + table_name + "\n(\n"
    
    cursor1.execute("select COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE_FLAG, CASE WHEN DEFAULT_VALUE ='?' THEN 'NULL' ELSE DEFAULT_VALUE END AS DEFAULT_VALUE from EDS_DATA_CATALOG.EDW_TABLE_COLUMN where db_instance_name='" + dbName + "' and db_schema_name='" + schemaName + "' and table_name='" + tblName + "'  and environment_name = '" + envName + "' order by column_sequence")
    results = cursor1.fetchall()
    for cols in results:
        vcolname = cols[0]
        vdatatype = cols[1]
        vdatalen = cols[2]
        vdatapre = cols[3]
        vdatasca = cols[4]
        vnullflg = cols[5]
        vdfault = cols[6]
        if vdfault in ['CURRENT_TIMESTAMP']:
            vdfault = 'DEFAULT CURRENT_TIMESTAMP()'
        elif vdfault == '3500-01-01 00:00:00':
            vdfault = 'DEFAULT cast(\'3500-01-01 00:00:00\' as TIMESTAMP_NTZ(0))'
        elif vdfault == '3500/01/01':
            vdfault = 'DEFAULT to_date(\'3500-01-01\')'
        elif vdfault in ['1900-01-01','1900/01/01']:
            vdfault = 'DEFAULT to_date(\'1900-01-01\')'
        else:
            vdfault = vdfault
        vtgttype = ""
        cursor2.execute("select target_data_type from EDS_DATA_CATALOG.EDW_DATA_TYPE_MAPPING where source_db_type='" + "HANA" + "' and target_db_type='SNOWFLAKE' and source_data_type = '" + vdatatype + "'")
        results2 = cursor2.fetchall()
        for dtype in results2:
            vtgttype = dtype[0]
        if 'NTZ' in vtgttype and vdfault is not None and vdfault !='NULL':
            vdfault= 'DEFAULT CAST(CURRENT_TIMESTAMP() as TIMESTAMP_NTZ(0))'

        if vtgttype == "":
            raise Exception(
                " Snowflake data type mapping for datatype - " + vdatatype + " doesnt exist on config tables.. Please check and reexecute")
        if vdatatype.upper() == "NUMBER" :
            if vdatapre != None and vdatapre != -128:
                if vdatasca != None and vdatapre != -128:
                    prestr = "(" + str(vdatapre) + "," + str(vdatasca) + ")"
                else:
                    prestr = "(" + str(vdatapre) + ")"
            else:
                prestr = ""
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + prestr
            # snowDDL = snowDDL + " " + vcolname + " " + vtgttype + "(" + str(vdatapre) + "," + str(vdatasca) + "),\n"
        elif vdatatype.upper() == "TIMESTAMP":
            if vdatasca != None:
                snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(" + str(vdatasca) + ")"
            else:
                snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype
        elif vdatatype.upper() == "VARCHAR" or vdatatype.upper() == "CHAR" or vdatatype.upper() == "VARBYTE":
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(" + str(vdatalen) + ")"
        elif vdatatype.upper() == "CLOB":
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(16777216)"
        elif vdatatype.upper() == "BYTE":
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype + "(256)"
        else:
            snowDDL = snowDDL + ' "' + vcolname + '" ' + vtgttype
        if vdfault != 'NULL' and vdfault is not None:
            if vdfault.lstrip('-').replace('.', '').isdigit() and vdatatype.upper() not in ['VARCHAR','CHAR','VARBYTE']:
                snowDDL = snowDDL + ' DEFAULT ' + vdfault
            elif vdfault in ['DEFAULT CURRENT_USER()', 'DEFAULT CURRENT_TIMESTAMP()',
                             'DEFAULT cast(\'3500-01-01 00:00:00\' as TIMESTAMP_NTZ(0))',
                             'DEFAULT to_date(\'3500-01-01\')','DEFAULT to_date(\'1900-01-01\')','CAST(CURRENT_TIMESTAMP() as TIMESTAMP_NTZ(0))']:
                snowDDL = snowDDL + ' ' + vdfault
            else:
                snowDDL = snowDDL + ' DEFAULT ' + "'" + vdfault + "'"
        snowDDL = snowDDL + ",  \n"
    # snowDDL = snowDDL + whocol
    if len(snowDDL.strip()) == 0:
        snowDDL = '------ERROR: NO METADATA'
    else:
        snowDDL = snowDDL + ' '+ whocol
    snowDDL = snowDDL[:-2] + "\n"
    if crmode.upper() == "CREATE":
        snowDDL = snowDDL + ") if not exists; \n"
    else:
        snowDDL = snowDDL + ");\n"
    return snowDDL



def genDDL(dbName, schemaName, tblName, crmode):
    try:
        connstr = cx_Oracle.makedsn('dbc-prd-3108-vip.cisco.com', '1839', service_name='ODSPROD.CISCO.COM')
        conn = cx_Oracle.connect(user='EDWRALRO', password='s0xl0ckd0wn', dsn=connstr)
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()
        if not dbName:
            print(
                "Database is mandatory.. It cannot be null.. Please check and re invoke like genDDL(dbName,schemaName,tblName,crmode)..")
            sys.exit(3)
        if not schemaName:
            print(
                "schemaName is mandatory.. It cannot be null.. Please check and re invoke like genDDL(dbName,schemaName,tblName,crmode)..")
            sys.exit(4)
        if not tblName:
            print(
                "tblName is mandatory.. It cannot be null.. Please check and re invoke like genDDL(dbName,schemaName,tblName,crmode)..")
            sys.exit(5)
        # print("Passed parameters are \n" + dbName + "\n" + schemaName + "\n" + tblName)
        # os.system('clear')
        validate_table(dbName, schemaName, tblName, cursor1)
        wholist = addwhocol()
        dbType = pull_dbtype(dbName, cursor1)
        # snowDDL = "USE ROLE EDW_BM_ROLE; \n"
        snowDDL = " "
        if dbType.upper() == "ORACLE":
            snowDDL = snowDDL + getoracleddl(dbName, schemaName, tblName, dbType, cursor1, cursor2, wholist, crmode)
        elif dbType.upper() == "TERADATA":
            snowDDL = snowDDL + gettdddl(dbName, schemaName, tblName, dbType, cursor1, cursor2, wholist, crmode)
        elif dbType.upper() == "HADOOP":
            snowDDL = snowDDL + gethdpddl(dbName, schemaName, tblName, dbType, cursor1, cursor2, wholist, crmode)
        elif dbType.upper() == "HANA":
            snowDDL = snowDDL + gethanaddl(dbName, schemaName, tblName, dbType, cursor1, cursor2, wholist, crmode)
        else:
            raise Exception(
                " DBTYPE - " + dbType + " seems to be new and not configured.. Please contact automation team for help.")
        cursor1.close()
        cursor2.close()
        # return snowDDL
        # snowDDL = snowDDL + "\n GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE " + tblName + " TO ROLE EDW_DATALAKE_ROLE;\n"
        return snowDDL

    except Exception as e:
        MSG = "\n  Below exception encountered while executing DDL_Generator script on line {}".format(
            sys.exc_info()[-1].tb_lineno) + " \n "
        MSG = MSG + "\n ************************************************************************************ \n"
        MSG = MSG + "\n   " + str(e) + "\n"
        MSG = MSG + "\n ************************************************************************************ \n"
        print(MSG)


## Main code starts here
def main(argv):
    try:
        set_env()
        try:
            opts, args = getopt.getopt(argv, "hd:s:t:m:", ["database=", "schema=", "table=", "mode="])
        except getopt.GetoptError:
            print('Invalid parameters.. Please refer below syntax and pass accordingly')
            print(
                'DDL_Generator.py -d DBName( ex: CG1PRD/CFNPRD/ODSPROD) -s Schema (ex: XTR/SLSORDDB) -t TableName (ex:OE_ORDER_LINES_ALL) -m Mode (ex:Replace/Create)')
            sys.exit(2)
        crmode = 'create'
        for opt, arg in opts:
            # print("opt is - " + opt + " , arg is - " + arg)
            if opt == '-h':
                print(
                    'DDL_Generator.py -h help -d DBName( ex: CG1PRD/CFNPRD/ODSPROD) -s Schema (ex: XTR/SLSORDDB) -t TableName (ex:OE_ORDER_LINES_ALL)  -m Mode (ex:Replace/Create)')
                sys.exit()
            elif opt in ("-d", "--database"):
                dbName = arg
            elif opt in ("-s", "--schema"):
                schemaName = arg
            elif opt in ("-t", "--table"):
                tblName = arg
            elif opt in ("-m", "--mode"):
                crmode = arg
        out = genDDL(dbName, schemaName, tblName, crmode)
        print(out)
    except Exception as e:
        MSG = "\n  Below exception encountered while executing DDL_Generator script on line {}".format(
            sys.exc_info()[-1].tb_lineno) + " \n "
        MSG = MSG + "\n ************************************************************************************ \n"
        MSG = MSG + "\n   " + str(e) + "\n"
        MSG = MSG + "\n ************************************************************************************ \n"
        print(MSG)

## Dhunder statement starts here
if __name__ == "__main__":
    main(sys.argv[1:])


