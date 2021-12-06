CREATE OR REPLACE FUNCTION EDS_DATA_CATALOG.FN_GET_COLUMN_LIST_NEW
(
  I_ENVIRONMENT_NAME IN VARCHAR2,
  I_DB_INSTANCE_NAME VARCHAR2,
  I_DB_SCHEMA_NAME VARCHAR2,
  I_TABLE_NAME IN VARCHAR2,
  I_BATCH_ID IN NUMBER DEFAULT 1
) RETURN CLOB AS
v_COLUMN_NAME  VARCHAR2(200);
v_COLUMN_NAME2 VARCHAR2(200);
v_COLUMN_ALIAS  VARCHAR2(200);
    v_DB_TYPE edw_db_instance.db_type%TYPE;
O_COLUMN_LIST  CLOB;
v_WHO_COL VARCHAR(1000);
    v_COLUMN_CNT NUMBER := 0;
v_DB_SCHEMA_NAME VARCHAR2(200) := FN_GET_SCHEMA(I_DB_INSTANCE_NAME => I_DB_INSTANCE_NAME,
I_DB_SCHEMA_NAME => I_DB_SCHEMA_NAME,
I_TABLE_NAME => I_TABLE_NAME);
BEGIN
--Verify if data available in table columns with all information
    v_COLUMN_CNT := FN_CHECK_COLUMN_EXISTANCE(I_ENVIRONMENT_NAME => I_ENVIRONMENT_NAME,
  I_DB_INSTANCE_NAME => I_DB_INSTANCE_NAME,
  I_DB_SCHEMA_NAME => v_DB_SCHEMA_NAME,
  I_TABLE_NAME => I_TABLE_NAME);
IF v_COLUMN_CNT > 0 THEN
FOR col_cur_rec in
(SELECT
edi.db_type,
etc.data_type,
etc.column_name
FROM
edw_db_instance edi,
edw_table_column etc
WHERE  edi.environment_name = etc.environment_name
AND edi.db_instance_name = etc.db_instance_name
AND edi.environment_name = I_ENVIRONMENT_NAME
AND edi.db_instance_name = I_DB_INSTANCE_NAME
AND etc.db_schema_name = v_DB_SCHEMA_NAME
AND etc.table_name = I_TABLE_NAME
ORDER BY etc.column_sequence)
LOOP
         v_DB_TYPE := col_cur_rec.db_type;
 v_COLUMN_NAME := '"'||col_cur_rec.column_name||'"';
 v_COLUMN_NAME2 := v_COLUMN_NAME;
 v_COLUMN_NAME := CASE WHEN col_cur_rec.db_type = 'TERADATA' THEN
  CASE WHEN col_cur_rec.data_type = 'DATE' THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD'')' || ' AS ' || v_COLUMN_NAME2
   WHEN col_cur_rec.data_type IN ('TIMESTAMP','TIMESTAMP(0)') THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS'')' || ' AS ' || v_COLUMN_NAME2
   WHEN col_cur_rec.data_type IN ('TIMESTAMP(6)','TIMESTAMP(9)') THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS.FF'')' || ' AS ' || v_COLUMN_NAME2
   ELSE
v_COLUMN_NAME
   END
       WHEN col_cur_rec.db_type = 'ORACLE' THEN
   CASE WHEN col_cur_rec.data_type IN ('DATE','TIMESTAMP','TIMESTAMP(0)') THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS'')' || ' AS ' || v_COLUMN_NAME2
        WHEN col_cur_rec.data_type IN ('TIMESTAMP(6)','TIMESTAMP(9)','TIMESTAMP(6) WITH LOCAL TIME ZONE','TIMESTAMP(9) WITH LOCAL TIME ZONE') THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS.FF'')' || ' AS ' || v_COLUMN_NAME2
--                            WHEN col_cur_rec.data_type IN ('CLOB') THEN
--                                            'DBMS_LOB.SUBSTR(lob_loc => '||v_COLUMN_NAME||',amount =>4000,offset => 1))'
--                                            'SUBSTR('||v_COLUMN_NAME||',1,4000)'
    ELSE
 v_COLUMN_NAME
   END
       WHEN col_cur_rec.db_type = 'HANA' THEN
   CASE WHEN col_cur_rec.data_type = 'DATE' THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS'')' || ' AS ' || v_COLUMN_NAME2
WHEN col_cur_rec.data_type IN ('TIMESTAMP','TIMESTAMP(0)') THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS.FF3'')' || ' AS ' || v_COLUMN_NAME2
        WHEN col_cur_rec.data_type IN ('TIMESTAMP(6)','TIMESTAMP(9)') THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS.FF'')' || ' AS ' || v_COLUMN_NAME2
    ELSE
 v_COLUMN_NAME
   END
   WHEN col_cur_rec.db_type = 'HANANRT' THEN
   CASE WHEN col_cur_rec.data_type = 'DATE' THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS'')' || ' AS ' || v_COLUMN_NAME2
WHEN col_cur_rec.data_type IN ('TIMESTAMP','TIMESTAMP(0)') THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS.FF3'')' || ' AS ' || v_COLUMN_NAME2
        WHEN col_cur_rec.data_type IN ('TIMESTAMP(6)','TIMESTAMP(9)') THEN
'TO_CHAR('||v_COLUMN_NAME||',''YYYY-MM-DD HH24:MI:SS.FF'')' || ' AS ' || v_COLUMN_NAME2
    ELSE
 v_COLUMN_NAME
   END
       WHEN col_cur_rec.db_type = 'SQL_SERVER' THEN
  CASE WHEN col_cur_rec.data_type IN ('date','datetime') THEN
     'CONVERT(VARCHAR,'||v_COLUMN_NAME||',20)' || ' AS ' || v_COLUMN_NAME2
       WHEN col_cur_rec.data_type = 'datetime2' THEN
     'CONVERT(VARCHAR,'||v_COLUMN_NAME||',21)' || ' AS ' || v_COLUMN_NAME2
                                       ELSE
                                            v_COLUMN_NAME
                                  END
       WHEN col_cur_rec.db_type = 'MYSQL' THEN
   CASE WHEN col_cur_rec.data_type IN ('date') THEN
'DATE_FORMAT('||col_cur_rec.column_name||',''%Y-%m-%d %H:%i:%s'')' || ' AS ' || v_COLUMN_NAME2
        WHEN col_cur_rec.data_type IN ('timestamp') THEN
'DATE_FORMAT('||col_cur_rec.column_name||',''%Y-%m-%d %H:%i:%s.%f'')' || ' AS ' || v_COLUMN_NAME2
    ELSE
 col_cur_rec.column_name
   END
       WHEN col_cur_rec.db_type = 'SFDC' THEN
col_cur_rec.column_name
  END;
         --CONVERT(VARCHAR,''"+context.FROM_EXTRACT_DTM+"'',20)
 --v_COLUMN_ALIAS := REGEXP_REPLACE(REGEXP_REPLACE(col_cur_rec.column_name,'^[0-9]','_'),'[$,#,@]','_');
 --O_COLUMN_LIST := CASE WHEN col_cur_rec.db_type = 'SFDC' THEN
--O_COLUMN_LIST||v_COLUMN_NAME||','
                  --             -- WHEN col_cur_rec.db_type = 'MYSQL' THEN
--   --O_COLUMN_LIST||v_COLUMN_NAME||' AS "'||v_COLUMN_ALIAS||'"'||','
--   ELSE
--O_COLUMN_LIST||v_COLUMN_NAME||' AS '||v_COLUMN_ALIAS||','
--  END;
   O_COLUMN_LIST := O_COLUMN_LIST||v_COLUMN_NAME||',';
END LOOP;
        IF v_DB_TYPE = 'SFDC' THEN
            v_WHO_COL := '';
            O_COLUMN_LIST := CASE WHEN INSTR(O_COLUMN_LIST,',',LENGTH(O_COLUMN_LIST)-1) = LENGTH(O_COLUMN_LIST) THEN
                                                  SUBSTR(O_COLUMN_LIST,1,LENGTH(O_COLUMN_LIST)-1)
                                             ELSE O_COLUMN_LIST
                                        END||' FROM '||I_TABLE_NAME;
        ELSE
            v_WHO_COL := ''''||TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD HH24:MI:SS')||''' AS EDWSF_CREATE_DTM,'''
                             ||sys_context('USERENV','OS_USER')||''' AS EDWSF_CREATE_USER,'''
                             ||TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS')||''' AS EDWSF_UPDATE_DTM,'''
                             ||sys_context('USERENV','OS_USER')||''' AS EDWSF_UPDATE_USER ';
            IF v_DB_TYPE IN ('TERADATA','ORACLE','HANA','SQL_SERVER','MYSQL','HANANRT') THEN
            O_COLUMN_LIST := O_COLUMN_LIST||v_WHO_COL||',';
            END IF;
        END IF;
ELSE
o_COLUMN_LIST := 'NO METADATA FOR '||I_ENVIRONMENT_NAME||'.'||I_DB_INSTANCE_NAME||'.'||I_DB_SCHEMA_NAME||'.'||I_TABLE_NAME;
END IF;
-- Below change is to handle column which has $ on it
-- Commenting below since column name comes in double quotes
--O_COLUMN_LIST := replace(O_COLUMN_LIST,'$','\$');
RETURN O_COLUMN_LIST;
END FN_GET_COLUMN_LIST_NEW;
/
