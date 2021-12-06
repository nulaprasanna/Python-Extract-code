


Source1 Name : SQ_MT_RTNR_SMC_ALLOCATION


Pre SQL : 



SQL Query : 
SELECT
   SMC_ALLOC.SALES_ORDER_LINE_KEY
  ,AR.PRODUCT_KEY AS ENTERPRISE_OR_INVOICE_SKU
  ,SMC_ALLOC.SALES_MOTION_CD 
  ,SMC_ALLOC.DV_ALLOCATION_PCT
  ,'SWSS' AS SERVICE_CATEGORY_CD
  ,'N' AS OA_FLG
  ,SMC_ALLOC.START_TV_DT                  
  ,SMC_ALLOC.END_TV_DT 
  ,'RTNR' AS SOURCE_TYPE  
  ,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM
  ,USER AS EDW_CREATE_USER
  ,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM
  ,USER AS EDW_UPDATE_USER
 FROM $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
 INNER JOIN ( SELECT DV_SALES_ORDER_LINE_KEY AS SALES_ORDER_LINE_KEY, 
      DV_PRODUCT_KEY AS PRODUCT_KEY
                FROM $$FINLGLVWDB.N_DRVD_NCR_REV_TRX_FOR_BKG DRVD, 
                     $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION EL 
               WHERE SOURCE_SYSTEM_CODE = 'CG'  
                 AND LAST_RECORD_FLAG = 'Y' 
                 AND DRVD.DV_SALES_ORDER_LINE_KEY > 0 
                 AND DRVD.DV_SALES_ORDER_LINE_KEY = EL.SALES_ORDER_LINE_KEY
                 AND EL.END_TV_DT = '3500-01-01'
                 AND EL.EDW_UPDATE_DTM > ( SELECT LAST_EXTRACT_DATE FROM $$ETLVWDB.CTL_ETL_LAST_EXTRACT_DATES 
              WHERE JOB_STREAM_ID = 'wf_MT_AR_SLS_MOTION_ATTRIB' 
                                              AND TABLE_NAME = 'MT_RTNR_SMC_ALLOCATION')
               GROUP BY 1,2                                                                                                          
             ) AR                                                                                                                                                                                                
  ON SMC_ALLOC.SALES_ORDER_LINE_KEY = AR.SALES_ORDER_LINE_KEY 
  WHERE SMC_ALLOC.END_TV_DT = '3500-01-01'


Post SQL : 



Target1 Name : WI_EL_AR_SLS_MOTION_ATTRIB1


Pre SQL : 
DELETE FROM $$STGDB.WI_EL_AR_SLS_MOTION_ATTRIB;


Post SQL : 
CALL COLLECT_STATS_WRAP('$$STGDB','WI_EL_AR_SLS_MOTION_ATTRIB','D');


Source2 Name : SQ_WI_EL_AR_SLS_MOTION_ATTRIB


Pre SQL : 



SQL Query : 
SELECT
    DRVD.SALES_ORDER_LINE_KEY
   ,DRVD.PRODUCT_KEY AS ENTERPRISE_OR_INVOICE_SKU
   ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.SALES_MOTION_CD 
         ELSE NSOL.SALES_MOTION_CD
     END SALES_MOTION_CD
   ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_ALLOCATION_PCT
         ELSE 1.000000
     END AS DV_ALLOCATION_PCT
   ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN 'SWSS'
         ELSE 'TS' 
     END AS SERVICE_CATEGORY_CD
   ,'N' AS OA_FLG
   ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.START_TV_DT                 
         ELSE CAST(NSOL.START_TV_DATETIME AS DATE) 
     END AS START_DATE
   ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.END_TV_DT                  
         ELSE CAST('3500-01-01' AS DATE)
     END AS END_DATE                    
   ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN 'RTNR' 
         ELSE 'NON-RTNR' 
     END AS SOURCE_TYPE  
   ,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM
   ,USER AS EDW_CREATE_USER
   ,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM
   ,USER AS EDW_UPDATE_USER
 FROM ( SELECT DV_SALES_ORDER_LINE_KEY AS SALES_ORDER_LINE_KEY, 
               DV_PRODUCT_KEY AS PRODUCT_KEY 
    FROM $$FINLGLVWDB.N_DRVD_NCR_REV_TRX_FOR_BKG DRVD 
   WHERE DRVD.SOURCE_SYSTEM_CODE = 'CG'  
     AND DRVD.LAST_RECORD_FLAG = 'Y' 
     AND DRVD.DV_SALES_ORDER_LINE_KEY > 0 
        AND DRVD.EDW_UPDATE_DATETIME > ( SELECT LAST_EXTRACT_DATE FROM $$ETLVWDB.CTL_ETL_LAST_EXTRACT_DATES 
                                          WHERE JOB_STREAM_ID = 'wf_MT_AR_SLS_MOTION_ATTRIB' 
                                        AND TABLE_NAME = 'N_DRVD_NCR_REV_TRX_FOR_BKG' )
      GROUP BY 1,2
      ) DRVD 
  LEFT JOIN $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
       ON SMC_ALLOC.SALES_ORDER_LINE_KEY = DRVD.SALES_ORDER_LINE_KEY
      AND SMC_ALLOC.END_TV_DT = '3500-01-01'
 INNER JOIN $$SLSORDVWDB.N_SALES_ORDER_LINE_TV NSOL
       ON DRVD.SALES_ORDER_LINE_KEY = NSOL.SALES_ORDER_LINE_KEY
      AND NSOL.SS_CODE = 'CG'
      AND NSOL.END_TV_DATETIME = '3500-01-01 00:00:00'
 WHERE NOT EXISTS ( SELECT 1 FROM $$STGDB.WI_EL_AR_SLS_MOTION_ATTRIB WI WHERE WI.SALES_ORDER_LINE_KEY = DRVD.SALES_ORDER_LINE_KEY AND DV_SOURCE_TYPE = 'RTNR')
   AND NOT EXISTS ( SELECT 1 FROM $$SLSORDVWDB.MT_AR_SLS_MOTION_ATTRIB MT WHERE MT.SALES_ORDER_LINE_KEY = DRVD.SALES_ORDER_LINE_KEY AND MT.DV_SOURCE_TYPE = 'RTNR')


Post SQL : 



Target2 Name : WI_EL_AR_SLS_MOTION_ATTRIB2


Pre SQL : 



Post SQL : 
CALL COLLECT_STATS_WRAP('$$STGDB','WI_EL_AR_SLS_MOTION_ATTRIB','D');


Source3 Name : SQ_WI_EL_AR_SLS_MOTION_ATTRIB1


Pre SQL : 



SQL Query : 
SELECT 
                 SALES_ORDER_LINE_KEY, 
                 DV_ENTERPRISE_INV_SKU_ID, 
                 SALES_MOTION_CD, 
                 DV_ALLOCATION_PCT, 
                 DV_SERVICE_CATEGORY_CD, 
                 DV_OA_FLG, 
                 START_TV_DT, 
                 END_TV_DT, 
                 DV_SOURCE_TYPE, 
                 EDW_CREATE_DTM, 
                 EDW_CREATE_USER, 
                 EDW_UPDATE_DTM, 
                 EDW_UPDATE_USER 
 FROM $$STGDB.WI_EL_AR_SLS_MOTION_ATTRIB WI
 WHERE NOT EXISTS ( SELECT 1 FROM $$SLSORDVWDB.MT_AR_SLS_MOTION_ATTRIB MT
                                 WHERE WI.SALES_ORDER_LINE_KEY = MT.SALES_ORDER_LINE_KEY
                                   AND WI.DV_ENTERPRISE_INV_SKU_ID = MT.DV_ENTERPRISE_INV_SKU_ID
                                   AND WI.START_TV_DT = MT.START_TV_DT
           AND WI.DV_SOURCE_TYPE = 'RTNR' )


Post SQL : 



Target3 Name : MT_AR_SLS_MOTION_ATTRIB


Pre SQL : 
DELETE FROM $$SLSORDVWDB.MT_AR_SLS_MOTION_ATTRIB MT
 WHERE EXISTS ( SELECT 1 FROM $$STGDB.WI_EL_AR_SLS_MOTION_ATTRIB WI WHERE MT.SALES_ORDER_LINE_KEY = WI.SALES_ORDER_LINE_KEY 
  AND MT.DV_ENTERPRISE_INV_SKU_ID = WI.DV_ENTERPRISE_INV_SKU_ID AND MT.DV_SOURCE_TYPE = 'NON-RTNR' );   
 
 UPDATE MT
 FROM $$SLSORDVWDB.MT_AR_SLS_MOTION_ATTRIB MT , 
      ( SELECT SALES_ORDER_LINE_KEY, DV_ENTERPRISE_INV_SKU_ID , MAX(START_TV_DT) AS START_TV_DT
          FROM $$STGDB.WI_EL_AR_SLS_MOTION_ATTRIB 
         WHERE DV_SOURCE_TYPE = 'RTNR' 
         GROUP BY 1,2
       ) WI
 SET END_TV_DT = WI.START_TV_DT - 1 , 
     EDW_UPDATE_DTM = CURRENT_TIMESTAMP(0)
 WHERE MT.SALES_ORDER_LINE_KEY = WI.SALES_ORDER_LINE_KEY
   AND MT.DV_ENTERPRISE_INV_SKU_ID = WI.DV_ENTERPRISE_INV_SKU_ID
   AND MT.START_TV_DT <> WI.START_TV_DT
   AND MT.END_TV_DT = '3500-01-01' ;


Post SQL : 
CALL COLLECT_STATS_WRAP('$$SLSORDDB','MT_AR_SLS_MOTION_ATTRIB','D');
 
 UPDATE CTL 
 FROM $$ETLVWDB.CTL_ETL_LAST_EXTRACT_DATES CTL,
      ( SELECT MAX(EDW_UPDATE_DTM) MAX_UPDATE_DTM 
      FROM $$STGDB.WI_EL_AR_SLS_MOTION_ATTRIB
        WHERE DV_SOURCE_TYPE = 'RTNR' ) WI  
 SET LAST_EXTRACT_DATE = COALESCE(WI.MAX_UPDATE_DTM,CTL.LAST_EXTRACT_DATE)  
 WHERE JOB_STREAM_ID = 'wf_MT_AR_SLS_MOTION_ATTRIB' 
 AND TABLE_NAME = 'MT_RTNR_SMC_ALLOCATION' ;  
 
 UPDATE CTL 
 FROM $$ETLVWDB.CTL_ETL_LAST_EXTRACT_DATES CTL,
      ( SELECT MAX(EDW_UPDATE_DTM) MAX_UPDATE_DTM 
      FROM $$STGDB.WI_EL_AR_SLS_MOTION_ATTRIB 
        WHERE DV_SOURCE_TYPE = 'NON-RTNR' ) WI  
 SET LAST_EXTRACT_DATE = COALESCE(WI.MAX_UPDATE_DTM,CTL.LAST_EXTRACT_DATE) 
 WHERE JOB_STREAM_ID = 'wf_MT_AR_SLS_MOTION_ATTRIB' 
 AND TABLE_NAME = 'N_DRVD_NCR_REV_TRX_FOR_BKG' ;