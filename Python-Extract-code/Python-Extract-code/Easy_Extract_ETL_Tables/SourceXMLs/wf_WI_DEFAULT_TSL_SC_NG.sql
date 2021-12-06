ETL Name:	wf_WI_DEFAULT_TSL_SC_NG.xml


Session 1: 	s_m_WI_DEFAULT_TSL_SC_NG
Mapping 1: 	m_WI_DEFAULT_TSL_SC_NG


Source1 Name : SQ_WI_DEFAULT_TSL_SC


Pre SQL : 
DELETE FROM $$NRTSTGDB.WI_DEFAULT_TSL_SC_NG;


SQL Query : 
SELECT
  -(ROW_NUMBER() OVER (ORDER BY TMP1.SO_SBSCRPTN_ITM_SLS_TRX_KEY ASC)) + TMP2.MAX_VAL AS  TRX_SPLIT_SC_ID ,
  TMP1.SO_SBSCRPTN_ITM_SLS_TRX_KEY, 
  TMP1.SHIP_TO_CUST_ACCT_LOC_USE_KEY,
  TMP1.SK_TRX_ID_INT,
  TMP1.TSL_PROCESSED_DTM  ,
  TMP1.DV_TSL_PROCESSED_DT   ,
  TMP1.SOURCE_COMMIT_DTM 
  FROM
  (
   SELECT 
   TSL.SO_SBSCRPTN_ITM_SLS_TRX_KEY, 
   TSL.SHIP_TO_CUST_ACCT_LOC_USE_KEY ,
   TSL.SK_TRX_ID_INT,
   TSL.TSL_PROCESSED_DTM,
   TSL.DV_TSL_PROCESSED_DT,
   TSL.SOURCE_COMMIT_DTM              
   FROM $$NRTNCRVWDB.N_XAAS_SO_SBCR_ITM_STRX_NRT_TV TSL WHERE  TSL.EDW_UPDATE_DTM > (SELECT LAST_EXTRACT_DATE FROM $$ETLVWDB.DW_JOB_STREAMS_NRT 
          WHERE JOB_STREAM_ID = 'wf_WI_DEFAULT_TSL_SC_NG' )
   AND TSL.SO_SBSCRPTN_ITM_SLS_TRX_KEY <> -7777 
   AND TSL.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0))
   AND NOT EXISTS
    (
     SELECT 1 FROM $$NRTNCRDB.N_XAAS_SCA_SLS_TRX_NG_NRT NXSAS 
     WHERE TSL.SO_SBSCRPTN_ITM_SLS_TRX_KEY = NXSAS.SO_SBSCRPTN_ITM_SLS_TRX_KEY  
     AND  NXSAS.EFFECTIVE_END_DTM='3500-01-01 00:00:00'  
    ) 
  ) TMP1,
  (SELECT COALESCE(MIN(SK_TRX_SPLIT_SC_ID_INT),0) MAX_VAL FROM $$TRANSLATIONDB.SM_XAAS_SCA_SLS_TRX_NG_NRT
 WHERE SK_TRX_SPLIT_SC_ID_INT>-1000000000) TMP2


Post SQL : 



Target1 Name : WI_DEFAULT_TSL_SC1


Pre SQL : 



Post SQL : 
COLLECT STATS ON $$NRTSTGDB.WI_DEFAULT_TSL_SC_NG;


Source2 Name : SQ_SM_XAAS_SLSCR_ASN_SLSTRX_NRT_NG


Pre SQL : 



SQL Query : 
SELECT 
  RANK() OVER (ORDER BY TRX_SPLIT_SC_ID) + MAX_KEY SLS_CREDIT_ASGNMT_SLS_TRX_KEY , 
  TRX_SPLIT_SC_ID    AS SK_TRX_SPLIT_SC_ID_INT,
  CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM,
  USER AS  EDW_CREATE_USER
  FROM 
  $$NRTSTGDB.WI_DEFAULT_TSL_SC_NG   SOSCTX,
  (SELECT COALESCE(MAX(SLS_CREDIT_ASGNMT_SLS_TRX_KEY ),0 )   MAX_KEY FROM  $$TRANSLATIONDB.SM_XAAS_SCA_SLS_TRX_NG_NRT) SMXSB
  WHERE  NOT EXISTS 
  ( SELECT 1 FROM $$TRANSLATIONDB.SM_XAAS_SCA_SLS_TRX_NG_NRT SMXSCA
  WHERE SOSCTX.TRX_SPLIT_SC_ID=SMXSCA.SK_TRX_SPLIT_SC_ID_INT
  )


Post SQL : 



Target2 Name : SM_XAAS_SLSCR_ASN_SLSTRX_NRT_NG1


Pre SQL : 



Post SQL : 
UPDATE $$ETLVWDB.DW_JOB_STREAMS_NRT 
  SET LAST_EXTRACT_DATE=(SELECT MAX(EDW_UPDATE_DTM) FROM $$NRTNCRVWDB.N_XAAS_SO_SBSCR_ITM_SLSTRX_NRT)
  WHERE JOB_STREAM_ID = 'wf_WI_DEFAULT_TSL_SC_NG';