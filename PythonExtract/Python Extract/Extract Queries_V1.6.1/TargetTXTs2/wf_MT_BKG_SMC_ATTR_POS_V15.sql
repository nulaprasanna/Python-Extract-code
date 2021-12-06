


Source1 Name : SQ_WI_N_POS_TRX_LN_NEW_RENEW_INCR2


Pre SQL : 



SQL Query : 
SELECT
 NVR_TV.BK_POS_TRANSACTION_ID_INT,
 NVR_TV.SALES_ORDER_LINE_KEY,             
 'POS' AS DV_TRX_TYPE,
 NVR_TV.SALES_MOTION_CD,
 CURRENT_TIMESTAMP(0) AS START_TV_DTM       ,
 NVR_TV.END_TV_DTM,
 POS_TRX.PRODUCT_KEY ,
 NVR_TV.SPLIT_PCT AS DV_ALLOCATION_PCT ,            
 'TS' AS DV_SERVICE_CATEGORY_CD  ,      
 'N' DV_OA_FLG     ,
 'UNKNOWN' AS DV_SOURCE_TYPE ,
 NVR_TV.SALES_MOTION_TIMING_CD,
NVR_TV.SLS_MTN_CORRECTION_REASON_DESC,
 /*dbommise- Added below columns as part of Q3FY20 March rel*/
 NVR_TV.MANUAL_OVERRIDE_ROLE          ,
 NVR_TV.REQUESTING_CSCO_WRKR_PRTY_KEY ,
 NVR_TV.SLS_MTN_CORRECTION_CASE_NUM   ,
 NVR_TV.SLS_MTN_CORRECTION_CMNT ,  
 NVR_TV.SK_OFFER_ATTRIBUTION_ID_INT
 FROM
 ( SELECT * FROM $$SLSORDVWDB.N_POS_TRX_LN_AS_NEW_OR_RNWL_TV WHERE END_TV_DTM = '3500-01-01 00:00:00' AND  BK_POS_TRANSACTION_ID_INT IN (
SEL BK_POS_TRANSACTION_ID_INT FROM $$SLSORDVWDB.N_POS_TRX_LN_AS_NEW_OR_RNWL_TV 
 WHERE EDW_UPDATE_DTM > '$$LastExtractDate'
 ))NVR_TV
 INNER JOIN (
  SELECT BK_POS_TRANSACTION_ID_INT, PRODUCT_KEY
   FROM $$SLSORDVWDB.N_POS_TRANSACTION_LINE
  UNION
  SELECT BK_POS_TRANSACTION_ID_INT, PRODUCT_KEY
   FROM $$SLSORDVWDB.N_BKG_POS_TRANSACTION_LINE
       ) POS_TRX
 ON POS_TRX.BK_POS_TRANSACTION_ID_INT = NVR_TV.BK_POS_TRANSACTION_ID_INT
 AND NVR_TV.END_TV_DTM = '3500-01-01 00:00:00'
 AND NOT EXISTS 
 /*Performance recommandation*/ 
		(SELECT	1 FROM ( SELECT SALES_ORDER_LINE_KEY
		FROM	$$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION MT GROUP BY 1 ) MT  
		WHERE	MT.SALES_ORDER_LINE_KEY = NVR_TV.SALES_ORDER_LINE_KEY )


Post SQL : 



Target1 Name : WI_N_POS_TRX_LN_NEW_RENEW_INCR1


Pre SQL : 
DELETE FROM $$STGDB.WI_N_POS_TRX_LN_NEW_RENEW_INCR ALL;


Post SQL : 
CALL COLLECT_STATS_WRAP('$$STGDB','WI_N_POS_TRX_LN_NEW_RENEW_INCR','D');


Source2 Name : SQ_WI_N_POS_TRX_LN_NEW_RENEW_INCR


Pre SQL : 



SQL Query : 
SELECT
 NVR_TV.BK_POS_TRANSACTION_ID_INT,
 NVR_TV.SALES_ORDER_LINE_KEY,             
 'POS' AS DV_TRX_TYPE,
 NVR_TV.SALES_MOTION_CD,
 CURRENT_TIMESTAMP(0) AS START_TV_DTM,
 NVR_TV.END_TV_DTM,
 POS_TRX.PRODUCT_KEY ,
 NVR_TV.SPLIT_PCT AS DV_ALLOCATION_PCT ,            
 MT.DV_SERVICE_CATEGORY_CD  ,      
 'N' DV_OA_FLG     ,
 /* 'RTNR' AS DV_SOURCE_TYPE, */ /* DV SOURCE TYPE derivation change to accommodate SQ */
 MT.DV_SOURCE_TYPE, 
  NVR_TV.SALES_MOTION_TIMING_CD,
 NVR_TV.SLS_MTN_CORRECTION_REASON_DESC,
 /*dbommise- Added below columns as part of Q3FY20 March rel*/
 NVR_TV.MANUAL_OVERRIDE_ROLE          ,
 NVR_TV.REQUESTING_CSCO_WRKR_PRTY_KEY ,
 NVR_TV.SLS_MTN_CORRECTION_CASE_NUM   ,
 NVR_TV.SLS_MTN_CORRECTION_CMNT  ,
 NVR_TV.SK_OFFER_ATTRIBUTION_ID_INT
FROM
 ( SELECT * FROM $$SLSORDVWDB.N_POS_TRX_LN_AS_NEW_OR_RNWL_TV 
WHERE END_TV_DTM = '3500-01-01 00:00:00'
AND  BK_POS_TRANSACTION_ID_INT IN (
SEL BK_POS_TRANSACTION_ID_INT FROM $$SLSORDVWDB.N_POS_TRX_LN_AS_NEW_OR_RNWL_TV 
WHERE EDW_UPDATE_DTM > '$$LastExtractDate'
 )) NVR_TV
 INNER JOIN (
  SELECT BK_POS_TRANSACTION_ID_INT, PRODUCT_KEY
   FROM $$SLSORDVWDB.N_POS_TRANSACTION_LINE
  UNION
  SELECT BK_POS_TRANSACTION_ID_INT, PRODUCT_KEY
   FROM $$SLSORDVWDB.N_BKG_POS_TRANSACTION_LINE
       ) POS_TRX
 ON POS_TRX.BK_POS_TRANSACTION_ID_INT = NVR_TV.BK_POS_TRANSACTION_ID_INT
 INNER JOIN ( SELECT SALES_ORDER_LINE_KEY, DV_SERVICE_CATEGORY_CD, DV_SOURCE_TYPE /* DV SOURCE TYPE derivation change to accommodate SQ */
			FROM $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION
                    WHERE END_TV_DTM='3500-01-01 00:00:00'
			QUALIFY ROW_NUMBER() OVER( PARTITION BY SALES_ORDER_LINE_KEY ORDER BY DV_SERVICE_CATEGORY_CD DESC ) = 1
			) MT 
 ON MT.SALES_ORDER_LINE_KEY = NVR_TV.SALES_ORDER_LINE_KEY


Post SQL : 



Target2 Name : WI_N_POS_TRX_LN_NEW_RENEW_INCR3


Pre SQL : 



Post SQL : 
DELETE FROM $$STGDB.WI_N_POS_TRX_LN_NEW_RENEW_INCR WI
WHERE WI.SALES_MOTION_CD IN ( 'UNKNOWN' ) /* Excluding N/A from this to allow NEW/RENEW to N/A conversion */
AND EXISTS 
  	  (
   SELECT 1 FROM $$SLSORDVWDB.MT_BKG_SLS_MOTION_ATTRIBUTION MT
    WHERE DV_TRX_TYPE = 'POS'
      AND END_TV_DTM > CURRENT_TIMESTAMP(0) 
      AND SALES_MOTION_CD IN ('NEW','RENEW' )
  AND MT.DV_TRANSACTION_ID = WI.BK_POS_TRANSACTION_ID_INT 
  	  ) ;

CALL COLLECT_STATS_WRAP('$$STGDB','WI_N_POS_TRX_LN_NEW_RENEW_INCR','D');


Source3 Name : SQ_SM_BKG_SLS_MOTION_ATTRIBUTION


Pre SQL : 



SQL Query : 
SELECT
SM_MAX.MAX_SLS_MOTION_ATTRIB_KEY + RANK() OVER (ORDER BY DV_TRANSACTION_ID,DV_TRX_TYPE,DV_ENTERPRISE_INV_SKU_ID,SK_OFFER_ATTRIBUTION_ID_INT) AS SK_SALES_MOTION_ATTRIB_KEY 
,DV_TRANSACTION_ID          
,DV_TRX_TYPE                                   
,DV_ENTERPRISE_INV_SKU_ID   
,SK_OFFER_ATTRIBUTION_ID_INT
,CURRENT_TIMESTAMP(0)EDW_CREATE_DTM   
,USER AS EDW_CREATE_USER
FROM
  (
	SELECT
	 BK_POS_TRANSACTION_ID_INT AS DV_TRANSACTION_ID
	 ,DV_TRX_TYPE
	 ,PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID
	 ,SK_OFFER_ATTRIBUTION_ID_INT
	FROM $$STGDB.WI_N_POS_TRX_LN_NEW_RENEW_INCR WI 
	GROUP BY 1,2,3,4
  ) SRC
,(SELECT COALESCE(MAX(SK_SALES_MOTION_ATTRIB_KEY),0) MAX_SLS_MOTION_ATTRIB_KEY 
    FROM $$ETLVWDB.SM_BKG_SLS_MOTION_ATTRIBUTION) SM_MAX                  
WHERE NOT EXISTS( SELECT 1 FROM $$ETLVWDB.SM_BKG_SLS_MOTION_ATTRIBUTION SM
	WHERE SRC.DV_TRANSACTION_ID=SM.DV_TRANSACTION_ID
	  AND SRC.DV_ENTERPRISE_INV_SKU_ID=SM.DV_ENTERPRISE_INV_SKU_ID
	  AND SRC.DV_TRX_TYPE = SM.DV_TRX_TYPE
	  AND SRC.SK_OFFER_ATTRIBUTION_ID_INT = SM.SK_OFFER_ATTRIBUTION_ID_INT )


Post SQL : 



Target3 Name : SM_BKG_SLS_MOTION_ATTRIBUTION1


Pre SQL : 



Post SQL : 
CALL COLLECT_STATS_WRAP('$$TRANSLATIONDB','SM_BKG_SLS_MOTION_ATTRIBUTION','D');

DELETE FROM $$STGDB.WI_MT_BKG_SPLIT_CHECK WHERE DV_TRX_TYPE = 'POS' ;

INSERT INTO $$STGDB.WI_MT_BKG_SPLIT_CHECK
SELECT 
SM.SK_SALES_MOTION_ATTRIB_KEY SK_SALES_MOTION_ATTRIB_KEY,
WI.DV_TRX_TYPE,
SUM(DV_ALLOCATION_PCT) DV_ALLOCATION_PCT_SUM
FROM $$STGDB.WI_N_POS_TRX_LN_NEW_RENEW_INCR WI
INNER JOIN $$ETLVWDB.SM_BKG_SLS_MOTION_ATTRIBUTION SM
  ON WI.BK_POS_TRANSACTION_ID_INT = SM.DV_TRANSACTION_ID
  AND WI.DV_TRX_TYPE = SM.DV_TRX_TYPE
  AND WI.PRODUCT_KEY = SM.DV_ENTERPRISE_INV_SKU_ID
AND WI.SK_OFFER_ATTRIBUTION_ID_INT = SM.SK_OFFER_ATTRIBUTION_ID_INT
 /* INNER JOIN $$SLSORDVWDB.N_POS_TRX_LN_AS_NEW_OR_RNWL_TV NVR_TV
  ON WI.BK_POS_TRANSACTION_ID_INT= NVR_TV.BK_POS_TRANSACTION_ID_INT
 AND NVR_TV.END_TV_DTM = '3500-01-01 00:00:00'
 WHERE SOFTWARE_FLG='N'*/
GROUP BY 1,2 HAVING ROUND(SUM(DV_ALLOCATION_PCT),4) <> 1.0000 ;

COLLECT STATS ON $$STGDB.WI_MT_BKG_SPLIT_CHECK ;

DELETE FROM $$EXCEPDB.EX_MT_BKG_SLS_MOTION_ATTRIB EX
WHERE EXISTS ( SEL 1 FROM $$STGDB.WI_N_POS_TRX_LN_NEW_RENEW_INCR WI
				WHERE WI.BK_POS_TRANSACTION_ID_INT = EX.DV_TRANSACTION_ID
                           AND WI.DV_TRX_TYPE= EX.DV_TRX_TYPE
						   AND WI.PRODUCT_KEY = EX.DV_ENTERPRISE_INV_SKU_ID
			  ) ;

INSERT INTO $$EXCEPDB.EX_MT_BKG_SLS_MOTION_ATTRIB
SELECT 
SM.SK_SALES_MOTION_ATTRIB_KEY    
, CASE WHEN INCR.SALES_ORDER_LINE_KEY= -8888 THEN RANDOM(1005000,1006800)*-1 WHEN INCR.SALES_ORDER_LINE_KEY= -9999 THEN RANDOM(1000000,1001800)*-1 WHEN INCR.SALES_ORDER_LINE_KEY= -7777 THEN RANDOM(1010000,1011800)*-1 WHEN INCR.SALES_ORDER_LINE_KEY=-6666 THEN RANDOM(1015000,1016800)*-1 ELSE INCR.SALES_ORDER_LINE_KEY END SALES_ORDER_LINE_KEY
, INCR.BK_POS_TRANSACTION_ID_INT AS DV_TRANSACTION_ID             
, INCR.DV_TRX_TYPE                   
, INCR.SALES_MOTION_CD               
, INCR.START_TV_DTM                   
, INCR.END_TV_DTM                     
, INCR.PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID    
, INCR.DV_ALLOCATION_PCT             
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN ITEM_CATEGORY_NAME ELSE INCR.DV_SERVICE_CATEGORY_CD END AS DV_SERVICE_CATEGORY_CD
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN 'Y' ELSE INCR.DV_OA_FLG END AS DV_OA_FLG
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN 'RTNR' ELSE INCR.DV_SOURCE_TYPE END AS DV_SOURCE_TYPE             
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.BK_AS_ARCHITECTURE_NAME ELSE 'UNKNOWN' END AS AS_ARCHITECTURE_NAME
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.BK_TECH_GROUP_ID ELSE 'UNKNOWN' END AS TECHNOLOGY_GROUP_ID              
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.BK_DV_ATTR_PRDT_OFFER_TYPE_NAME ELSE 'UNKNOWN' END AS ATTR_PRDT_OFFER_TYPE_NAME
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.AS_TS_CD ELSE 'UNK' END AS AS_TS_CODE
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SK_RENEW_CONTRACT_LINE_ID ELSE -999 END AS SK_RENEW_CONTRACT_LINE_ID
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SK_AS_PARENT_INVENTORY_ITEM_ID ELSE -999 END AS  SK_AS_PARENT_INVENTORY_ITEM_ID
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SK_ATTRIBUTION_ID_INT ELSE INCR.SK_OFFER_ATTRIBUTION_ID_INT END AS SK_OFFER_ATTRIBUTION_ID_INT
,USER AS EDW_UPDATE_USER     
,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM   
,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM  
,USER AS EDW_CREATE_USER               
,COALESCE(CASE WHEN NSOL.SALES_ORDER_KEY= -8888 THEN RANDOM(1005000,1006800)*-1 WHEN NSOL.SALES_ORDER_KEY= -9999 THEN RANDOM(1000000,1001800)*-1 WHEN NSOL.SALES_ORDER_KEY= -7777 THEN RANDOM(1010000,1011800)*-1 WHEN NSOL.SALES_ORDER_KEY=-6666 THEN RANDOM(1015000,1016800)*-1 ELSE NSOL.SALES_ORDER_KEY  END, RANDOM(1010000,1011800)*-1) AS SALES_ORDER_KEY    
,CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.ATTR_PRDT_KEY ELSE INCR.PRODUCT_KEY END AS SRC_ENTERPRISE_INV_SKU_ID
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.ATTR_PRDT_CLASS_NAME ELSE 'UNKNOWN' END AS PRODUCT_CLASS
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.TRANSACTION_CR_PARTY_KEY ELSE -999 END AS TRANSACTION_CR_PARTY_KEY
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.HQ_CR_PRTY_KEY ELSE -999 END AS HQ_CR_PRTY_KEY
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.RENEWAL_REF_ID ELSE 'UNKNOWN' END AS RENEWAL_REF_ID
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.RENEWAL_REF_CD ELSE 'UNKNOWN' END AS RENEWAL_REF_CD
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SMR_TAGGING_FAILURE_RSN_CD ELSE INCR.SLS_MTN_CORRECTION_REASON_DESC END AS SMR_TAGGING_FAILURE_RSN_CD
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SALES_MOTION_TIMING_CD ELSE INCR.SALES_MOTION_TIMING_CD END AS SALES_MOTION_TIMING_CD
/*dbommise added below columns as part of Q3FY20 March release */
, INCR.MANUAL_OVERRIDE_ROLE
, INCR.REQUESTING_CSCO_WRKR_PRTY_KEY
, INCR.SLS_MTN_CORRECTION_CASE_NUM
, INCR.SLS_MTN_CORRECTION_CMNT
,CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.DV_RENEWAL_GAP_DAYS_CNT ELSE -9999 END AS RENEWAL_GAP_DAYS
,'N' AS  BUNDLE_FLG
FROM
$$STGDB.WI_N_POS_TRX_LN_NEW_RENEW_INCR INCR
INNER JOIN $$ETLVWDB.SM_BKG_SLS_MOTION_ATTRIBUTION SM
  ON INCR.BK_POS_TRANSACTION_ID_INT = SM.DV_TRANSACTION_ID
  AND INCR.DV_TRX_TYPE = SM.DV_TRX_TYPE
  AND INCR.PRODUCT_KEY = SM.DV_ENTERPRISE_INV_SKU_ID
  AND INCR.SK_OFFER_ATTRIBUTION_ID_INT=SM.SK_OFFER_ATTRIBUTION_ID_INT
  
INNER JOIN $$SLSORDVWDB.N_SALES_ORDER_LINE_TV NSOL
  ON INCR.SALES_ORDER_LINE_KEY = NSOL.SALES_ORDER_LINE_KEY
  AND CAST(NSOL.END_TV_DATETIME AS DATE) =  CAST( '3500-01-01' AS DATE) 
  AND NSOL.SS_CODE IN ('CG','OPL')

   LEFT JOIN $$SLSORDVWDB.N_ATTR_PRDT_NEW_RNWL_ACV_TV NATTR
  ON INCR.BK_POS_TRANSACTION_ID_INT = NATTR.POS_TRANSACTION_ID_INT
  AND INCR.SK_OFFER_ATTRIBUTION_ID_INT = NATTR.SK_ATTRIBUTION_ID_INT
  AND INCR.SALES_MOTION_CD = (CASE WHEN NATTR.SALES_MOTION_CD = 'UNKNOWN' THEN INCR.SALES_MOTION_CD ELSE NATTR.SALES_MOTION_CD END)
AND NATTR.END_TV_DTM = '3500-01-01 00:00:00'


WHERE  EXISTS ( SEL 1 FROM $$STGDB.WI_MT_BKG_SPLIT_CHECK CHK
                   WHERE SM.SK_SALES_MOTION_ATTRIB_KEY = CHK.SK_SALES_MOTION_ATTRIB_KEY 
		    ) ;

COLLECT STATS ON $$EXCEPDB.EX_MT_BKG_SLS_MOTION_ATTRIB;

DELETE FROM $$STGDB.WI_N_POS_TRX_LN_NEW_RENEW_INCR WI
WHERE EXISTS ( SEL 1 FROM $$EXCEPDB.EX_MT_BKG_SLS_MOTION_ATTRIB EX
                           WHERE WI.BK_POS_TRANSACTION_ID_INT = EX.DV_TRANSACTION_ID
                           AND WI.DV_TRX_TYPE= EX.DV_TRX_TYPE
						   AND WI.PRODUCT_KEY = EX.DV_ENTERPRISE_INV_SKU_ID
              ) ;


Source4 Name : SQ_MT_BKG_SLS_MOTION_ATTRIBUTION


Pre SQL : 



SQL Query : 
SELECT
SM.SK_SALES_MOTION_ATTRIB_KEY    
, CASE WHEN INCR.SALES_ORDER_LINE_KEY= -8888 THEN RANDOM(1005000,1006800)*-1 WHEN INCR.SALES_ORDER_LINE_KEY= -9999 THEN RANDOM(1000000,1001800)*-1 WHEN INCR.SALES_ORDER_LINE_KEY= -7777 THEN RANDOM(1010000,1011800)*-1 WHEN INCR.SALES_ORDER_LINE_KEY=-6666 THEN RANDOM(1015000,1016800)*-1 ELSE INCR.SALES_ORDER_LINE_KEY END SALES_ORDER_LINE_KEY
, INCR.BK_POS_TRANSACTION_ID_INT AS DV_TRANSACTION_ID             
, INCR.DV_TRX_TYPE                   
, INCR.SALES_MOTION_CD               
, INCR.START_TV_DTM                   
, INCR.END_TV_DTM                     
, INCR.PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID      
, INCR.DV_ALLOCATION_PCT             
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN ITEM_CATEGORY_NAME ELSE INCR.DV_SERVICE_CATEGORY_CD END AS DV_SERVICE_CATEGORY_CD
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN 'Y' ELSE INCR.DV_OA_FLG END AS DV_OA_FLG
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN 'RTNR' ELSE INCR.DV_SOURCE_TYPE END AS DV_SOURCE_TYPE             
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.BK_AS_ARCHITECTURE_NAME ELSE 'UNKNOWN' END AS AS_ARCHITECTURE_NAME
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.BK_TECH_GROUP_ID ELSE 'UNKNOWN' END AS TECHNOLOGY_GROUP_ID              
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.BK_DV_ATTR_PRDT_OFFER_TYPE_NAME ELSE 'UNKNOWN' END AS ATTR_PRDT_OFFER_TYPE_NAME
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.AS_TS_CD ELSE 'UNK' END AS AS_TS_CODE
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SK_RENEW_CONTRACT_LINE_ID ELSE -999 END AS SK_RENEW_CONTRACT_LINE_ID
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SK_AS_PARENT_INVENTORY_ITEM_ID ELSE -999 END AS  SK_AS_PARENT_INVENTORY_ITEM_ID
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SK_ATTRIBUTION_ID_INT ELSE INCR.SK_OFFER_ATTRIBUTION_ID_INT END AS SK_OFFER_ATTRIBUTION_ID_INT
,USER AS EDW_UPDATE_USER     
,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM   
,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM  
,USER AS EDW_CREATE_USER               
,COALESCE(CASE WHEN NSOL.SALES_ORDER_KEY= -8888 THEN RANDOM(1005000,1006800)*-1 WHEN NSOL.SALES_ORDER_KEY= -9999 THEN RANDOM(1000000,1001800)*-1 WHEN NSOL.SALES_ORDER_KEY= -7777 THEN RANDOM(1010000,1011800)*-1 WHEN NSOL.SALES_ORDER_KEY=-6666 THEN RANDOM(1015000,1016800)*-1 ELSE NSOL.SALES_ORDER_KEY  END, RANDOM(1010000,1011800)*-1) AS SALES_ORDER_KEY    
,CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.ATTR_PRDT_KEY ELSE INCR.PRODUCT_KEY END AS SRC_ENTERPRISE_INV_SKU_ID
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.ATTR_PRDT_CLASS_NAME ELSE 'UNKNOWN' END AS PRODUCT_CLASS
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.TRANSACTION_CR_PARTY_KEY ELSE -999 END AS TRANSACTION_CR_PARTY_KEY
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.HQ_CR_PRTY_KEY ELSE -999 END AS HQ_CR_PRTY_KEY
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.RENEWAL_REF_ID ELSE 'UNKNOWN' END AS RENEWAL_REF_ID
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.RENEWAL_REF_CD ELSE 'UNKNOWN' END AS RENEWAL_REF_CD
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SMR_TAGGING_FAILURE_RSN_CD ELSE INCR.SLS_MTN_CORRECTION_REASON_DESC END AS SMR_TAGGING_FAILURE_RSN_CD
, CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.SALES_MOTION_TIMING_CD ELSE INCR.SALES_MOTION_TIMING_CD END AS SALES_MOTION_TIMING_CD
/*dbommise added below columns as part of Q3FY20 March release */
, INCR.MANUAL_OVERRIDE_ROLE
, INCR.REQUESTING_CSCO_WRKR_PRTY_KEY
, INCR.SLS_MTN_CORRECTION_CASE_NUM
, INCR.SLS_MTN_CORRECTION_CMNT
,OFFER_ATTRIB_PRDT_KEY
,CASE WHEN NATTR.POS_TRANSACTION_ID_INT IS NOT NULL THEN NATTR.DV_RENEWAL_GAP_DAYS_CNT ELSE -9999 END AS RENEWAL_GAP_DAYS
FROM
$$STGDB.WI_N_POS_TRX_LN_NEW_RENEW_INCR INCR
INNER JOIN $$ETLVWDB.SM_BKG_SLS_MOTION_ATTRIBUTION SM
  ON INCR.BK_POS_TRANSACTION_ID_INT = SM.DV_TRANSACTION_ID
  AND INCR.DV_TRX_TYPE = SM.DV_TRX_TYPE
  AND INCR.PRODUCT_KEY = SM.DV_ENTERPRISE_INV_SKU_ID
  AND INCR.SK_OFFER_ATTRIBUTION_ID_INT=SM.SK_OFFER_ATTRIBUTION_ID_INT
INNER JOIN $$SLSORDVWDB.N_SALES_ORDER_LINE_TV NSOL
  ON INCR.SALES_ORDER_LINE_KEY = NSOL.SALES_ORDER_LINE_KEY
  AND CAST(NSOL.END_TV_DATETIME AS DATE) =  CAST( '3500-01-01' AS DATE) 
  AND NSOL.SS_CODE IN ('CG','OPL')
  
  LEFT JOIN $$SLSORDVWDB.N_ATTR_PRDT_NEW_RNWL_ACV_TV NATTR
  ON INCR.BK_POS_TRANSACTION_ID_INT = NATTR.POS_TRANSACTION_ID_INT
  AND INCR.SK_OFFER_ATTRIBUTION_ID_INT = NATTR.SK_ATTRIBUTION_ID_INT
  AND INCR.SALES_MOTION_CD = (CASE WHEN NATTR.SALES_MOTION_CD = 'UNKNOWN' THEN INCR.SALES_MOTION_CD ELSE NATTR.SALES_MOTION_CD END)
AND NATTR.END_TV_DTM = '3500-01-01 00:00:00'
  
WHERE NOT EXISTS ( SELECT 1 FROM 
                $$SLSORDVWDB.MT_BKG_SLS_MOTION_ATTRIBUTION MT
                WHERE MT.DV_TRANSACTION_ID = INCR.BK_POS_TRANSACTION_ID_INT
                AND MT.DV_ENTERPRISE_INV_SKU_ID = INCR.PRODUCT_KEY
                AND MT.START_TV_DTM = INCR.START_TV_DTM
                )


Post SQL : 



Target4 Name : MT_BKG_SLS_MOTION_ATTRIBUTION1


Pre SQL : 
UPDATE MT_BKG
FROM $$SLSORDVWDB.MT_BKG_SLS_MOTION_ATTRIBUTION MT_BKG,
     ( SELECT BK_POS_TRANSACTION_ID_INT, PRODUCT_KEY,SK_OFFER_ATTRIBUTION_ID_INT
	 , MAX(START_TV_DTM) AS START_TV_DTM
        FROM $$STGDB.WI_N_POS_TRX_LN_NEW_RENEW_INCR WI
       GROUP BY 1,2,3
      ) WI        
SET END_TV_DTM = WI.START_TV_DTM - INTERVAL '1' SECOND,
    EDW_UPDATE_USER = USER,
    EDW_UPDATE_DTM = CURRENT_TIMESTAMP(0)
WHERE WI.BK_POS_TRANSACTION_ID_INT = MT_BKG.DV_TRANSACTION_ID
  AND WI.PRODUCT_KEY = MT_BKG.DV_ENTERPRISE_INV_SKU_ID
AND WI.SK_OFFER_ATTRIBUTION_ID_INT=MT_BKG.SK_OFFER_ATTRIBUTION_ID_INT
  AND MT_BKG.DV_TRX_TYPE = 'POS' 
  AND WI.START_TV_DTM <> MT_BKG.START_TV_DTM 
  AND MT_BKG.END_TV_DTM = '3500-01-01 00:00:00';


Post SQL : 
CALL COLLECT_STATS_WRAP('$$SLSORDDB','MT_BKG_SLS_MOTION_ATTRIBUTION','D');

UPDATE MT_BKG FROM
$$SLSORDVWDB.MT_BKG_SLS_MOTION_ATTRIBUTION MT_BKG,
(SELECT DV_RENEWAL_GAP_DAYS_CNT,POS_TRANSACTION_ID_INT,SK_ATTRIBUTION_ID_INT,RENEWAL_REF_CD,RENEWAL_REF_ID
FROM $$SLSORDVWDB.N_ATTR_PRDT_NEW_RNWL_ACV_TV NATTR
WHERE NATTR.END_TV_DTM='3500-01-01 00:00:00'
AND NATTR.ORDER_ORIGIN_CD='DISTI-DSV'
AND NATTR.DV_RENEWAL_GAP_DAYS_CNT<>-9999
GROUP BY 1,2,3,4,5) MT1
SET RENEWAL_GAP_DAYS=MT1.DV_RENEWAL_GAP_DAYS_CNT
WHERE MT1.POS_TRANSACTION_ID_INT=MT_BKG.DV_TRANSACTION_ID
AND MT1.SK_ATTRIBUTION_ID_INT=MT_BKG.SK_OFFER_ATTRIBUTION_ID_INT
AND MT_BKG.END_TV_DTM='3500-01-01 00:00:00'
AND MT_BKG.DV_TRX_TYPE='POS'
AND MT_BKG.RENEWAL_REF_CD=MT1.RENEWAL_REF_CD
AND MT_BKG.RENEWAL_REF_ID=MT1.RENEWAL_REF_ID
AND MT_BKG.RENEWAL_GAP_DAYS=-9999
AND MT_BKG.BUNDLE_FLG='N'
AND MT_BKG.RENEWAL_GAP_DAYS<>MT1.DV_RENEWAL_GAP_DAYS_CNT;