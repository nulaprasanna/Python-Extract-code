CREATE OR REPLACE FUNCTION EDS_DATA_CATALOG.FN_GET_SS_MERGE_NEW2
(
  I_ENVIRONMENT_NAME IN VARCHAR2,
  I_SRC_DB_INSTANCE_NAME VARCHAR2,
  I_SRC_DB_SCHEMA_NAME VARCHAR2,
  I_SRC_TABLE_NAME IN VARCHAR2,
  I_SF_SRC_DB_INSTANCE_NAME VARCHAR2,
  I_SF_SRC_DB_SCHEMA_NAME VARCHAR2,
  I_SF_SRC_TABLE_NAME IN VARCHAR2,
  I_SF_TGT_DB_INSTANCE_NAME VARCHAR2,
  I_SF_TGT_DB_SCHEMA_NAME VARCHAR2,
  I_SF_TGT_TABLE_NAME IN VARCHAR2,
  I_MERGE_TYPE  IN VARCHAR2 DEFAULT 'MERGE_ACML'
) RETURN CLOB AS
  O_SS_MERGE  CLOB :='MERGE INTO '||I_SF_TGT_DB_INSTANCE_NAME||'.'||I_SF_TGT_DB_SCHEMA_NAME||'.'||I_SF_TGT_TABLE_NAME||' AS TGT USING (SELECT ';
  v_UK_COL_LIST VARCHAR2(4000);
  v_JOIN_CLAUSE VARCHAR2(4000);
  v_WHO_COL VARCHAR2(200) := 'EDWSF_CREATE_DTM,EDWSF_CREATE_USER,EDWSF_UPDATE_DTM,EDWSF_UPDATE_USER,EDWSF_BATCH_ID,''N'' as EDWSF_SOURCE_DELETED_FLAG';
  v_WHO_COL_NO_VAL VARCHAR2(200) := 'EDWSF_CREATE_DTM,EDWSF_CREATE_USER,EDWSF_UPDATE_DTM,EDWSF_UPDATE_USER,EDWSF_BATCH_ID,EDWSF_SOURCE_DELETED_FLAG';
  v_SELECT_COLUMN_NAME VARCHAR2(200);
  v_SRC_COLUMN_NAME VARCHAR2(200);
  v_TGT_COLUMN_NAME VARCHAR2(200);
  v_SELECT_COL_LIST CLOB;
  v_SRC_COL_LIST CLOB;
  v_UPDATE_COL_LIST CLOB;
  v_TGT_COL_LIST CLOB;
  v_UNIQUE_REC_COL VARCHAR2(100) := 'REFRESH_DATETIME';
  v_ROWNUM_COL VARCHAR2(4000) := ',ROW_NUMBER() OVER (PARTITION BY ';
  v_SRC_DB_SCHEMA_NAME VARCHAR2(200) := CASE WHEN I_SRC_DB_INSTANCE_NAME IN ('TDPROD','TDPROD2','TDDEV','TDTEST')
                                             THEN REPLACE(REPLACE(I_SRC_DB_SCHEMA_NAME,'ETLVWDB','ETLONLYDB'),'VWDB','DB')
                                             ELSE I_SRC_DB_SCHEMA_NAME END;
BEGIN
-- Getting Unique Key List and creating join condition from other databases
FOR uk_cur_rec in
(
SELECT
upper(eukc.column_name) column_name
FROM
eds_data_catalog.edw_unique_key_column eukc,
eds_data_catalog.edw_unique_key euk
where
  eukc.environment_name = euk.environment_name
  and eukc.db_instance_name = euk.db_instance_name
  and eukc.db_schema_name   = euk.db_schema_name
  and eukc.table_name       = euk.table_name
  and eukc.unique_key_name  = euk.unique_key_name
  AND euk.environment_name  = I_ENVIRONMENT_NAME
  AND euk.db_instance_name  = I_SRC_DB_INSTANCE_NAME
--  AND euk.db_schema_name    = I_SRC_DB_SCHEMA_NAME
  AND euk.db_schema_name    = v_SRC_DB_SCHEMA_NAME
  AND euk.table_name        = I_SRC_TABLE_NAME
  and euk.active_flag       = 'A'
  and eukc.column_name      <> NVL(v_UNIQUE_REC_COL,'UNKNOWN')
  ORDER BY eukc.column_position
)
LOOP
 v_JOIN_CLAUSE := v_JOIN_CLAUSE||'nvl(nullif(SRC.' || uk_cur_rec.COLUMN_NAME || ',''''),''0'') = nvl(TGT.' || uk_cur_rec.COLUMN_NAME||',''0'') AND ';
 v_UK_COL_LIST := v_UK_COL_LIST||uk_cur_rec.COLUMN_NAME||',';
END LOOP;
--Getting column LIst and creating insert and select statement
FOR col_cur_rec in
(
SELECT
upper(etc.column_name) column_name,
etc.nullable_flag,
etc.data_type
FROM
eds_data_catalog.edw_table_column etc
WHERE
  etc.environment_name = I_ENVIRONMENT_NAME
  and etc.db_instance_name = I_SRC_DB_INSTANCE_NAME
--  and etc.db_schema_name = I_SRC_DB_SCHEMA_NAME
  and etc.db_schema_name = v_SRC_DB_SCHEMA_NAME
  and etc.table_name = I_SRC_TABLE_NAME
  and etc.active_flag ='A'
ORDER BY
etc.column_sequence
)
LOOP
 --v_SELECT_COLUMN_NAME := FN_GET_STG_COLUMN_NAME(col_cur_rec.COLUMN_NAME,col_cur_rec.data_type,col_cur_rec.nullable_flag);
 if col_cur_rec.nullable_flag = 'N'
 then
	v_SELECT_COLUMN_NAME := 'NVL("'||col_cur_rec.COLUMN_NAME||'",'''')' || ' "' || col_cur_rec.COLUMN_NAME || '"';
 else
	v_SELECT_COLUMN_NAME := '"' || col_cur_rec.COLUMN_NAME || '"';
 end if;
 --v_SRC_COLUMN_NAME := REGEXP_REPLACE(REGEXP_REPLACE(col_cur_rec.COLUMN_NAME,'^[0-9]','_'),'[$,#,@]','_');
 v_SRC_COLUMN_NAME := '"'||col_cur_rec.COLUMN_NAME||'"';
 v_TGT_COLUMN_NAME := '"'||col_cur_rec.COLUMN_NAME||'"';
 v_SELECT_COL_LIST := v_SELECT_COL_LIST||v_SELECT_COLUMN_NAME||',';
 v_SRC_COL_LIST := v_SRC_COL_LIST||'SRC.'||v_SRC_COLUMN_NAME||',';
 v_TGT_COL_LIST := v_TGT_COL_LIST||v_TGT_COLUMN_NAME||',';
 v_UPDATE_COL_LIST :=  v_UPDATE_COL_LIST||CASE WHEN INSTR(v_UK_COL_LIST,col_cur_rec.COLUMN_NAME) > 0 THEN '' ELSE 'TGT.'||v_TGT_COLUMN_NAME||' = SRC.'||v_SRC_COLUMN_NAME||',' END;
END LOOP;
v_JOIN_CLAUSE := REGEXP_REPLACE(v_JOIN_CLAUSE,'(AND)','',LENGTH(v_JOIN_CLAUSE)-4);
v_UK_COL_LIST := REGEXP_REPLACE(v_UK_COL_LIST,'(,)','',LENGTH(v_UK_COL_LIST)-1);
v_ROWNUM_COL := v_ROWNUM_COL
 ||v_UK_COL_LIST
 ||' ORDER BY '
 ||v_UNIQUE_REC_COL
 ||' DESC) ROW_NUM FROM '
 ||I_SF_SRC_DB_INSTANCE_NAME||'.'
 ||I_SF_SRC_DB_SCHEMA_NAME||'.'
 ||I_SF_SRC_TABLE_NAME
 ||') IN_SRC WHERE IN_SRC.ROW_NUM = 1) AS SRC ON (';
--                 ||' WHERE SOURCE_DML_TYPE <> ''DELETE'') IN_SRC WHERE IN_SRC.ROW_NUM = 1) AS SRC ON (';

--  v_COL_LIST := REGEXP_REPLACE(v_COL_LIST,'(,)','',LENGTH(v_COL_LIST)-1);
v_UPDATE_COL_LIST := v_UPDATE_COL_LIST||'TGT.EDWSF_UPDATE_DTM = SRC.EDWSF_UPDATE_DTM,TGT.EDWSF_UPDATE_USER = SRC.EDWSF_UPDATE_USER,TGT.EDWSF_BATCH_ID = SRC.EDWSF_BATCH_ID,TGT.EDWSF_SOURCE_DELETED_FLAG = SRC.EDWSF_SOURCE_DELETED_FLAG  ';
IF I_MERGE_TYPE= 'MERGE_ACML' THEN
  O_SS_MERGE :=  O_SS_MERGE
   ||v_SELECT_COL_LIST||v_WHO_COL
   ||' FROM (SELECT '||v_SELECT_COL_LIST||v_WHO_COL
   ||v_ROWNUM_COL
   ||v_JOIN_CLAUSE||') WHEN MATCHED THEN UPDATE SET '
   ||v_UPDATE_COL_LIST
   ||'WHEN NOT MATCHED THEN INSERT ('
   ||v_TGT_COL_LIST||v_WHO_COL||') VALUES ('
   ||v_SRC_COL_LIST||v_WHO_COL||')';
ELSIF I_MERGE_TYPE= 'MERGE_ONE2ONE' THEN
  O_SS_MERGE :=  O_SS_MERGE
   ||v_SELECT_COL_LIST||v_WHO_COL
   ||' FROM '
   ||I_SF_SRC_DB_INSTANCE_NAME||'.'
   ||I_SF_SRC_DB_SCHEMA_NAME||'.'
   ||I_SF_SRC_TABLE_NAME
   ||') AS SRC ON ('
   ||v_JOIN_CLAUSE||') WHEN MATCHED THEN UPDATE SET '
   ||v_UPDATE_COL_LIST
   ||'WHEN NOT MATCHED THEN INSERT ('
   ||v_TGT_COL_LIST||v_WHO_COL_NO_VAL||') VALUES ('
   ||v_SRC_COL_LIST||v_WHO_COL_NO_VAL||')';
END IF;
RETURN O_SS_MERGE;
END FN_GET_SS_MERGE_NEW2;
/
