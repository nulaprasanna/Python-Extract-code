ETL Name:	wf_WI_DRVD_XAAS_BKGS_TRX_NRT_F_NG.xml


Session 1: 	s_m_WI_DRVD_XAAS_BKGS_TRX_NRT_F_NG
Mapping 1: 	m_WI_DRVD_XAAS_BKGS_TRX_NRT_F_NG


Source1 Name : SQ_WI_XAAS_ITM_SLS_TRX_INCR_NRT_F_NG


Pre SQL : 



SQL Query : 
SELECT 
    NXSCAST.SO_SBSCRPTN_ITM_SLS_TRX_KEY SO_SBSCRPTN_ITM_SLS_TRX_KEY   
    FROM
    $$NRTNCRDB.N_XAAS_SCA_SLS_TRX_NG_NRT NXSCAST
    INNER JOIN $$NRTNCRVWDB.N_XAAS_SO_SBCR_ITM_STRX_NRT_TV NXSSIST
    ON NXSSIST.SO_SBSCRPTN_ITM_SLS_TRX_KEY=NXSCAST.SO_SBSCRPTN_ITM_SLS_TRX_KEY
    AND NXSSIST.XAAS_TRX_METRIC_TYPE_NAME='BOOKING'
  AND NXSSIST.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0)),
    (SELECT LAST_EXTRACT_DATE FROM $$ETLVWDB.DW_JOB_STREAMS_NRT WHERE JOB_STREAM_ID = 'wf_N_XAAS_SLSCR_ASN_SLSTRX_NRT_NG' 
    /* AND JOB_GROUP_ID = 'EDWTD_XAAS_BK2' */
    )T
    WHERE 
    NXSCAST.EDW_UPDATE_DTM > T.LAST_EXTRACT_DATE
    AND EFFECTIVE_END_DTM='3500-01-01 00:00:00'
    AND NXSCAST.BK_SLS_TERR_ASGNMT_TYPE_CD = 'DR'
    UNION
    
    SELECT 
    NXSSIST.SO_SBSCRPTN_ITM_SLS_TRX_KEY  SO_SBSCRPTN_ITM_SLS_TRX_KEY   
    FROM
    $$NRTNCRVWDB.N_XAAS_SO_SBCR_ITM_STRX_NRT_TV  NXSSIST,
    ( SELECT LAST_EXTRACT_DATE FROM $$ETLVWDB.DW_JOB_STREAMS_NRT WHERE JOB_STREAM_ID = 'wf_N_XAAS_SO_SBSCR_ITM_SLS_TRX_NRT' 
    /* AND JOB_GROUP_ID = 'EDWTD_XAAS_BK2' */
    )T
    WHERE 
    NXSSIST.EDW_UPDATE_DTM > T.LAST_EXTRACT_DATE AND NXSSIST.XAAS_TRX_METRIC_TYPE_NAME='BOOKING'
 AND NXSSIST.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0))  
    AND NXSSIST.DV_NET_CHANGE_FLG='Y'     
    
    /* Added logic to get the incremental records from XAAS custom table OA-Q1FY16*/
    UNION
    
    SELECT NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY SO_SBSCRPTN_ITM_SLS_TRX_KEY
    FROM 
    (
    SELECT NXSAN.* FROM 
    $$NRTNCRVWDB.N_XAAS_SO_SBSCR_ATRB_NG_NRT_TV NXSAN
    INNER JOIN $$NRTNCRVWDB.N_XAAS_SO_SBCR_ITM_STRX_NRT_TV  NXSSIST
    ON NXSSIST.SO_SBSCRPTN_ITM_SLS_TRX_KEY=NXSAN.PARENT_SO_SBSCRP_ITMSLSTRX_KEY
    AND NXSSIST.XAAS_TRX_METRIC_TYPE_NAME ='BOOKING'
    AND NXSSIST.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0)) 
    AND NXSAN.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0)) 
    )NXSSA,
    (SELECT LAST_EXTRACT_DATE FROM $$ETLVWDB.DW_JOB_STREAMS_NRT WHERE JOB_STREAM_ID ='wf_N_XAAS_SO_SBSCRPTN_ATTRBTN_NRT_NG'
    )T
    WHERE NXSSA.EDW_UPDATE_DTM > T.LAST_EXTRACT_DATE
    AND NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY>0       
   AND NXSSA.DV_NET_CHANGE_FLG='Y'


Post SQL : 



Target1 Name : WI_XAAS_ITM_SLS_TRX_INCR_NRT_F1_NG


Pre SQL : 
DELETE  FROM  $$NRTSTGDB.WI_XAAS_ITM_SLS_TRX_INCR_NRT_F_NG ALL;


Post SQL : 
CALL COLLECT_STATS_WRAP('$$NRTSTGDB','WI_XAAS_ITM_SLS_TRX_INCR_NRT_F_NG','D');


Source2 Name : SQ_WI_DRVD_XAAS_BKGS_TRX_NRT_F_NG


Pre SQL : 



SQL Query : 
SELECT 
           TMP1.SLS_CREDIT_ASGNMT_SLS_TRX_KEY ,
           TMP1.BK_SALES_REP_NUM ,
           TMP1.SO_SBSCRPTN_ITM_SLS_TRX_KEY ,
           TMP1.SALES_TERRITORY_KEY ,
           TMP1.BK_SALES_CREDIT_TYPE_CODE ,
           TMP1.SPLIT_PCT ,
           TMP1.BK_SALES_ADJ_CD ,
           TMP1.BK_SALES_ADJ_TYPE_CD ,
           TMP1.SALES_VALUE_TRXL_AMT ,
           TMP1.BK_TRXL_CURRENCY_CD ,
           TMP1.BOOKED_DT ,
     ROW_NUMBER() OVER (ORDER BY TMP1.SO_SBSCRPTN_ITM_SLS_TRX_KEY,TRANSACTION_DATETIME ASC) + TMP2.MAX_VAL AS  TRANSACTION_SEQUENCE_ID_INT,
     TMP1.FORWARD_REVERSE_CODE,
     /* Commented as part of OA - Q1FY16
     CASE  WHEN (TMP1.SLS_COMMIT_DTM>TMP1.TSL_COMMIT_DTM)
     THEN
         TMP1.SLS_COMMIT_DTM
      ELSE
       TMP1.TSL_COMMIT_DTM
       END AS TRANSACTION_DATETIME , */
     /* derived the   TRANSACTION_DATETIME  as part of -OA Q1FY16 */
      MAX((TMP1.TRANSACTION_DATETIME )) OVER (PARTITION BY TMP1.DV_SO_SBSCRPTN_ITM_SLS_TRX_KEY) AS TRANSACTION_DATETIME,
     XAAS.PROCESS_DT AS  PROCESS_DATE,
     TMP1.TRANSACTION_QTY ,
     TMP1.SUBSCRIPTION_PRODUCT_KEY ,
     TMP1.SLS_COMMIT_DTM AS RBK_SC_SOURCE_COMMIT_DTM ,
     /* Added below 4 columns as part of OA- Q1FY16*/
     TMP1.DV_ATTRIBUTION_CD ,
     TMP1.SK_OFFER_ATTRIBUTION_ID_INT ,
     TMP1.DV_SO_SBSCRPTN_ITM_SLS_TRX_KEY ,
     TMP1.DV_PRODUCT_KEY 	
      /* ADDED AS PART OF Q2FY18 XAAS */
     ,TMP1.BOOKING_PCT AS BOOKING_PCT   
  ,TMP1.RECOG_SALES_VALUE_LOCAL_AMT  AS RECOG_SALES_VALUE_LOCAL_AMT
    /* END Q2FY18 XAAS */
    ,COALESCE(TMP1.POB_TYPE_CD,'UNKNOWN') /* NRS MPOB Change */
	,TMP1.SALES_MOTION_CD
     FROM 
     (SELECT 
     NXSCAST.SLS_CREDIT_ASGNMT_SLS_TRX_KEY ,
     NXSCAST.BK_SALES_REP_NUM  ,            
     NXSCAST.SO_SBSCRPTN_ITM_SLS_TRX_KEY   ,
     NXSCAST.BK_SLS_TERR_ASGNMT_TYPE_CD,
     NXSCAST.SALES_TERRITORY_KEY           ,
     NXSCAST.BK_SALES_CREDIT_TYPE_CODE     ,
     NXSCAST.SPLIT_PCT               ,
     NXSSIS.BK_SALES_ADJ_CD ,
     NXSSIS.BK_SALES_ADJ_TYPE_CD ,
     NXSSIS.SALES_VALUE_TRXL_AMT ,
     NXSSIS.BK_TRXL_CURRENCY_CD ,
     NXSSIS.BOOKED_DT ,
     'F' AS FORWARD_REVERSE_CODE,
     /* OA Q1FY16 */
        CASE WHEN NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY IS NULL THEN 
           (CASE 
              WHEN (NXSCAST.SOURCE_COMMIT_DTM >= NXSSIS.SOURCE_COMMIT_DTM ) THEN NXSCAST.SOURCE_COMMIT_DTM
             ELSE NXSSIS.SOURCE_COMMIT_DTM  END)
          ELSE (CASE WHEN (NXSCAST.SOURCE_COMMIT_DTM >= NXSSIS.SOURCE_COMMIT_DTM 
        AND NXSCAST.SOURCE_COMMIT_DTM >=NXSSA.SOURCE_COMMIT_DTM) THEN NXSCAST.SOURCE_COMMIT_DTM 
        WHEN (NXSSIS.SOURCE_COMMIT_DTM   >=NXSSA.SOURCE_COMMIT_DTM) THEN NXSSIS.SOURCE_COMMIT_DTM 
        ELSE NXSSA.SOURCE_COMMIT_DTM  END)   
           END TRANSACTION_DATETIME,
     TRANSACTION_QTY,
     NXSCAST.SOURCE_COMMIT_DTM AS SLS_COMMIT_DTM,
     NXSSIS.SOURCE_COMMIT_DTM AS  TSL_COMMIT_DTM ,
     NXSSIS.SUBSCRIPTION_PRODUCT_KEY,
     /* Added below 4 columns as part of OA-Q1FY16*/   
      CASE  WHEN ( NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY IS NOT NULL OR NXSSA_MPOB.PARENT_SO_SBSCRP_ITMSLSTRX_KEY IS NOT NULL)THEN 'BUNDLE' 
                     ELSE 'STANDALONE' END AS DV_ATTRIBUTION_CD,       
      CAST(-9999 AS INT) AS SK_OFFER_ATTRIBUTION_ID_INT,
           NXSSIS.SO_SBSCRPTN_ITM_SLS_TRX_KEY AS DV_SO_SBSCRPTN_ITM_SLS_TRX_KEY,   
           COALESCE(NXSSIS.SUBSCRIPTION_PRODUCT_KEY,-999) AS DV_PRODUCT_KEY	 
      /* ADDED AS PART OF Q2FY18 XAAS */
     ,NXSSIS.BOOKING_PCT AS BOOKING_PCT 
 ,NXSSIS.RECOG_SALES_VALUE_LOCAL_AMT AS RECOG_SALES_VALUE_LOCAL_AMT 
    /* END Q2FY18 XAAS */
    ,COALESCE(NXSSA_MPOB.POB_TYPE_CD, 'UNKNOWN') AS POB_TYPE_CD/* NRS MPOB Change */
	,'UNKNOWN' AS SALES_MOTION_CD
     FROM 
     $$NRTNCRDB.N_XAAS_SLSCDT_ASGN_SLSTRX_NRT NXSCAST
     INNER JOIN $$NRTSTGDB.WI_XAAS_ITM_SLS_TRX_INCR_NRT_F_NG  XAAS_ITM_SLS_TRX
     ON NXSCAST.SO_SBSCRPTN_ITM_SLS_TRX_KEY=XAAS_ITM_SLS_TRX.SO_SBSCRPTN_ITM_SLS_TRX_KEY
     AND NXSCAST.EFFECTIVE_END_DTM='3500-01-01 00:00:00'
     INNER JOIN $$NRTNCRVWDB.N_XAAS_SO_SBCR_ITM_STRX_NRT_TV NXSSIS
     ON NXSSIS.SO_SBSCRPTN_ITM_SLS_TRX_KEY=XAAS_ITM_SLS_TRX.SO_SBSCRPTN_ITM_SLS_TRX_KEY
AND NXSSIS.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0))
     LEFT OUTER JOIN (SELECT PARENT_SO_SBSCRP_ITMSLSTRX_KEY, MAX(SOURCE_COMMIT_DTM)  SOURCE_COMMIT_DTM 
     FROM  $$NRTNCRVWDB.N_XAAS_SO_SBSCR_ATRB_NG_NRT_TV WHERE
     END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0))AND PARENT_SO_SBSCRP_ITMSLSTRX_KEY>0 AND POB_TYPE_CD = 'UNKNOWN'  GROUP BY 1) NXSSA  
           ON NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY = NXSSIS.SO_SBSCRPTN_ITM_SLS_TRX_KEY
     
     LEFT OUTER JOIN (SELECT PARENT_SO_SBSCRP_ITMSLSTRX_KEY,POB_TYPE_CD   
     FROM  $$NRTNCRVWDB.N_XAAS_SO_SBSCR_ATRB_NG_NRT_TV WHERE
     END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0))AND PARENT_SO_SBSCRP_ITMSLSTRX_KEY>0 
     AND  ATTRIBUTION_CD = 'OFFSET' AND POB_TYPE_CD = 'MPOB'GROUP BY 1,2) NXSSA_MPOB  
           ON NXSSA_MPOB.PARENT_SO_SBSCRP_ITMSLSTRX_KEY = NXSSIS.SO_SBSCRPTN_ITM_SLS_TRX_KEY
     
        /* Added below XAAS attribution logic as part of  OA- Q1FY16*/
       UNION ALL
     
      SELECT 
      NXSCAST.SLS_CREDIT_ASGNMT_SLS_TRX_KEY ,
      NXSCAST.BK_SALES_REP_NUM  ,            
      NXSSA.SO_SBSCRPTN_ITM_SLS_TRX_KEY   ,
      NXSCAST.BK_SLS_TERR_ASGNMT_TYPE_CD,
      NXSCAST.SALES_TERRITORY_KEY           ,
      NXSCAST.BK_SALES_CREDIT_TYPE_CODE     ,
      NXSCAST.SPLIT_PCT               ,
      NXSSIS.BK_SALES_ADJ_CD ,
      NXSSIS.BK_SALES_ADJ_TYPE_CD ,
      NXSSA.SALES_VALUE_TRXL_AMT ,
      NXSSIS.BK_TRXL_CURRENCY_CD ,
      NXSSIS.BOOKED_DT ,
      'F' AS FORWARD_REVERSE_CODE,
     CASE WHEN (NXSCAST.SOURCE_COMMIT_DTM >= NXSSIS.SOURCE_COMMIT_DTM 
        AND NXSCAST.SOURCE_COMMIT_DTM >=NXSSA.SOURCE_COMMIT_DTM) THEN NXSCAST.SOURCE_COMMIT_DTM 
        WHEN (NXSSIS.SOURCE_COMMIT_DTM   >=NXSSA.SOURCE_COMMIT_DTM) THEN NXSSIS.SOURCE_COMMIT_DTM 
        ELSE NXSSA.SOURCE_COMMIT_DTM  END TRANSACTION_DATETIME,
      NXSSA.SKU_QTY AS TRANSACTION_QTY,
      NXSCAST.SOURCE_COMMIT_DTM AS SLS_COMMIT_DTM,
      NXSSIS.SOURCE_COMMIT_DTM AS  TSL_COMMIT_DTM ,
      NXSSA.ORDERED_PRODUCT_KEY,
      /* Added below 4 columns as part of OA-Q1FY16*/
      NXSSA.ATTRIBUTION_CD AS DV_ATTRIBUTION_CD,
      NXSSA.BK_OFFER_ATTRIBUTION_ID_INT AS SK_OFFER_ATTRIBUTION_ID_INT,
      NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY AS DV_SO_SBSCRPTN_ITM_SLS_TRX_KEY,   
      COALESCE(NXSSIS.SUBSCRIPTION_PRODUCT_KEY,-999) AS DV_PRODUCT_KEY
     /* ADDED AS PART OF Q2FY18 XAAS */
     ,NXSSA.BOOKINGS_PCT AS BOOKING_PCT   
  ,NXSSA.RECOGNIZED_SALES_VALUE_LOCAL_AMT  AS RECOG_SALES_VALUE_LOCAL_AMT
    /* END Q2FY18 XAAS */
     ,NXSSA.POB_TYPE_CD /* NRS MPOB Change */
	 ,'UNKNOWN' AS SALES_MOTION_CD
      FROM 
      $$NRTNCRDB.N_XAAS_SLSCDT_ASGN_SLSTRX_NRT NXSCAST
      INNER JOIN $$NRTSTGDB.WI_XAAS_ITM_SLS_TRX_INCR_NRT_F_NG  XAAS_ITM_SLS_TRX
     ON NXSCAST.SO_SBSCRPTN_ITM_SLS_TRX_KEY=XAAS_ITM_SLS_TRX.SO_SBSCRPTN_ITM_SLS_TRX_KEY
     AND NXSCAST.EFFECTIVE_END_DTM='3500-01-01 00:00:00'
      INNER JOIN $$NRTNCRVWDB.N_XAAS_SO_SBCR_ITM_STRX_NRT_TV NXSSIS
      ON NXSSIS.SO_SBSCRPTN_ITM_SLS_TRX_KEY=XAAS_ITM_SLS_TRX.SO_SBSCRPTN_ITM_SLS_TRX_KEY 
   AND NXSSIS.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0))     
      INNER JOIN  (SELECT * FROM $$NRTNCRVWDB.N_XAAS_SO_SBSCR_ATRB_NG_NRT_TV WHERE ATTRIBUTION_CD <> 'ATTRIBUTED' )  NXSSA 
      ON NXSSIS.SO_SBSCRPTN_ITM_SLS_TRX_KEY=NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY
      AND NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY>0
 AND NXSSA.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0))     
 
 UNION ALL
     
      SELECT 
      NXSCAST.SLS_CREDIT_ASGNMT_SLS_TRX_KEY ,
      NXSCAST.BK_SALES_REP_NUM  ,            
      NXSSA.SO_SBSCRPTN_ITM_SLS_TRX_KEY   ,
      NXSCAST.BK_SLS_TERR_ASGNMT_TYPE_CD,
      NXSCAST.SALES_TERRITORY_KEY           ,
      NXSCAST.BK_SALES_CREDIT_TYPE_CODE     ,
     (100* NXSCAST.TOTAL_SPLIT_PCT) AS SPLIT_PCT               ,
      NXSSIS.BK_SALES_ADJ_CD ,
      NXSSIS.BK_SALES_ADJ_TYPE_CD ,
      CASE WHEN NXSCAST.BK_OFFER_ATTRIBUTION_ID_INT>0  THEN NXSSIS.SALES_VALUE_TRXL_AMT ELSE NXSSA.SALES_VALUE_TRXL_AMT END AS SALES_VALUE_TRXL_AMT,
      NXSSIS.BK_TRXL_CURRENCY_CD ,
      NXSSIS.BOOKED_DT ,
      'F' AS FORWARD_REVERSE_CODE,
     CASE WHEN (NXSCAST.SOURCE_COMMIT_DTM >= NXSSIS.SOURCE_COMMIT_DTM 
        AND NXSCAST.SOURCE_COMMIT_DTM >=NXSSA.SOURCE_COMMIT_DTM) THEN NXSCAST.SOURCE_COMMIT_DTM 
        WHEN (NXSSIS.SOURCE_COMMIT_DTM   >=NXSSA.SOURCE_COMMIT_DTM) THEN NXSSIS.SOURCE_COMMIT_DTM 
        ELSE NXSSA.SOURCE_COMMIT_DTM  END TRANSACTION_DATETIME,
      NXSSA.SKU_QTY AS TRANSACTION_QTY,
      NXSCAST.SOURCE_COMMIT_DTM AS SLS_COMMIT_DTM,
      NXSSIS.SOURCE_COMMIT_DTM AS  TSL_COMMIT_DTM ,
      NXSSA.ORDERED_PRODUCT_KEY,
      /* Added below 4 columns as part of OA-Q1FY16*/
      NXSSA.ATTRIBUTION_CD AS DV_ATTRIBUTION_CD,
      NXSSA.BK_OFFER_ATTRIBUTION_ID_INT AS SK_OFFER_ATTRIBUTION_ID_INT,
      NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY AS DV_SO_SBSCRPTN_ITM_SLS_TRX_KEY,   
      COALESCE(NXSSIS.SUBSCRIPTION_PRODUCT_KEY,-999) AS DV_PRODUCT_KEY
     /* ADDED AS PART OF Q2FY18 XAAS */
     ,NXSSA.BOOKINGS_PCT AS BOOKING_PCT   
  ,CASE WHEN NXSCAST.BK_OFFER_ATTRIBUTION_ID_INT>0 THEN NXSSIS.RECOG_SALES_VALUE_LOCAL_AMT ELSE NXSSA.RECOGNIZED_SALES_VALUE_LOCAL_AMT  END AS RECOG_SALES_VALUE_LOCAL_AMT
    /* END Q2FY18 XAAS */
     ,NXSSA.POB_TYPE_CD /* NRS MPOB Change */
	 ,NXSCAST.SALES_MOTION_CD
      FROM 
      $$NRTNCRDB.N_XAAS_SCA_SLS_TRX_NG_NRT NXSCAST
      INNER JOIN $$NRTSTGDB.WI_XAAS_ITM_SLS_TRX_INCR_NRT_F_NG  XAAS_ITM_SLS_TRX
     ON NXSCAST.SO_SBSCRPTN_ITM_SLS_TRX_KEY=XAAS_ITM_SLS_TRX.SO_SBSCRPTN_ITM_SLS_TRX_KEY
     AND NXSCAST.EFFECTIVE_END_DTM='3500-01-01 00:00:00'
      INNER JOIN $$NRTNCRVWDB.N_XAAS_SO_SBCR_ITM_STRX_NRT_TV NXSSIS
      ON NXSSIS.SO_SBSCRPTN_ITM_SLS_TRX_KEY=XAAS_ITM_SLS_TRX.SO_SBSCRPTN_ITM_SLS_TRX_KEY 
   AND NXSSIS.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0))     
      INNER JOIN (SELECT * FROM $$NRTNCRVWDB.N_XAAS_SO_SBSCR_ATRB_NG_NRT_TV WHERE ATTRIBUTION_CD = 'ATTRIBUTED') NXSSA 
      ON NXSSIS.SO_SBSCRPTN_ITM_SLS_TRX_KEY=NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY
	  AND NXSCAST.BK_OFFER_ATTRIBUTION_ID_INT=CASE WHEN NXSCAST.BK_OFFER_ATTRIBUTION_ID_INT>0 THEN NXSSA.BK_OFFER_ATTRIBUTION_ID_INT ELSE -9999 END
      AND NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY>0
 AND NXSSA.END_TV_DTM=CAST('3500-01-01 00:00:00' AS TIMESTAMP(0)) 
      
                       
 
     ) TMP1
     , (SELECT COALESCE(MAX(TRANSACTION_SEQUENCE_ID_INT),0)  MAX_VAL FROM $$ETLVWDB.SM_XAAS_BKG_TRX_NRT_NG) TMP2
       ,(SELECT PROCESS_DT FROM $$ETLVWDB.EL_XAAS_BKG_PRCS_DT_CTRL_NRT_NG 
       WHERE SS_CD ='XAAS') XAAS
     WHERE  BK_SLS_TERR_ASGNMT_TYPE_CD = 'DR'


Post SQL : 



Target2 Name : WI_DRVD_XAAS_BKGS_TRX_NRT_F1_NG


Pre SQL : 
DELETE  FROM  $$NRTSTGDB.WI_DRVD_XAAS_BKGS_TRX_NRT_F_NG ALL;


Post SQL : 
CALL COLLECT_STATS_WRAP('$$NRTSTGDB','WI_DRVD_XAAS_BKGS_TRX_NRT_F_NG','D');

/*   NRS PI3 CHANGE */
UPDATE   $$NRTSTGDB.WI_DRVD_XAAS_BKGS_TRX_NRT_F_NG WIDXBTNF 
SET NRS_TRANSITION_FLG = 'Y'  
WHERE EXISTS 
(SELECT 1 FROM $$NRTNCRVWDB.N_XAAS_SO_SBSCR_ATRB_NG_NRT NXSSA WHERE NXSSA.NRS_TRANSITION_FLG = 'Y' 
AND NXSSA.PARENT_SO_SBSCRP_ITMSLSTRX_KEY  = WIDXBTNF.DV_SO_SBSCRPTN_ITM_SLS_TRX_KEY ) ;


Source3 Name : SQ_SM_XAAS_BKG_TRX_NRT_NG


Pre SQL : 



SQL Query : 
SELECT
  RANK() OVER (ORDER BY WIMDNBTF.TRANSACTION_SEQUENCE_ID_INT)+ SMDNBTKR_MAX.MAX_DRVD_BKG_VAL DRVD_NCR_BKG_TRX_KEY,
  WIMDNBTF.TRANSACTION_SEQUENCE_ID_INT TRANSACTION_SEQUENCE_ID_INT,
  USER  EDW_CREATE_USER,
  CURRENT_TIMESTAMP(0) EDW_CREATE_DTM 
  FROM
  (
  SELECT 
  WI1.TRANSACTION_SEQUENCE_ID_INT TRANSACTION_SEQUENCE_ID_INT,
  WI1.FORWARD_REVERSE_CODE FORWARD_REVERSE_FLAG
  FROM 
 $$NRTSTGDB.WI_DRVD_XAAS_BKGS_TRX_NRT_F_NG  WI1 
  )WIMDNBTF,
  (SELECT COALESCE(MAX(DRVD_XAAS_BKG_TRX_KEY ),0) MAX_DRVD_BKG_VAL FROM $$ETLVWDB.SM_XAAS_BKG_TRX_NRT_NG)SMDNBTKR_MAX
  WHERE
  NOT EXISTS
  (
  SELECT 1 FROM $$ETLVWDB.SM_XAAS_BKG_TRX_NRT_NG SMDNBTKR WHERE SMDNBTKR.TRANSACTION_SEQUENCE_ID_INT = WIMDNBTF.TRANSACTION_SEQUENCE_ID_INT)


Post SQL : 



Target3 Name : SM_XAAS_BKG_TRX_NRT1_NG


Pre SQL : 



Post SQL : 
CALL COLLECT_STATS_WRAP ('$$TRANSLATIONDB','SM_XAAS_BKG_TRX_NRT_NG','D') ;