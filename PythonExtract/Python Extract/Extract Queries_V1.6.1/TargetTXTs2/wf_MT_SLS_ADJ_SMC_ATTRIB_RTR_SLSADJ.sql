


Source1 Name : SQ_WI_MT_SLS_ADJ_SMC_ATTRIB_RTR2


Pre SQL : 



SQL Query : 
SELECT
    MNL_TRX.MANUAL_TRX_KEY
   ,SMC_ALLOC.SALES_ORDER_LINE_KEY
   ,MNL_TRX.RU_PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID
   ,SMC_ALLOC.SALES_MOTION_CD 
   ,SMC_ALLOC.DV_ALLOCATION_PCT
   ,SMC_ALLOC.DV_SERVICE_CATEGORY_CD AS DV_SERVICE_CATEGORY_CD
   ,'N' AS DV_OA_FLG
   ,CURRENT_TIMESTAMP(0) AS START_TV_DTM 
   ,SMC_ALLOC.END_TV_DTM 
   /*,'RTNR' AS DV_SOURCE_TYPE  */ /* DV SOURCE TYPE derivation change to accommodate SQ */
   ,SMC_ALLOC.DV_SOURCE_TYPE 
   ,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM
   ,USER AS EDW_CREATE_USER
   ,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM
   ,USER AS EDW_UPDATE_USER
   ,CAST(-7777 AS BIGINT) AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
   ,SMC_ALLOC.SALES_MOTION_TIMING_CD 
  FROM (
  SELECT 
  SALES_ORDER_LINE_KEY, 
  CAST(SK_OFFER_ATTRIBUTION_ID_INT AS VARCHAR(15) ) AS SK_OFFER_ATTRIBUTION_ID_INT,
  SALES_MOTION_CD,
  SALES_MOTION_TIMING_CD,
  DV_SOURCE_TYPE,
  DV_SERVICE_CATEGORY_CD,
  END_TV_DTM,
SUM(DV_ALLOCATION_PCT) AS DV_ALLOCATION_PCT
  FROM $$SLSORDVWDB.MT_SLS_MOTION_ATTRIBUTION MT1
  WHERE SK_OFFER_ATTRIBUTION_ID_INT > 0
  AND END_TV_DTM = '3500-01-01 00:00:00'
  AND EDW_UPDATE_DTM >  '$$LastExtractDate'
  AND BUNDLE_FLG='N'
  GROUP BY 1,2,3,4,5,6,7 ) SMC_ALLOC
  INNER JOIN $$SLSORDVWDB.N_SALES_ADJ_REBOK_MNL_TRX_TV MNL_TRX 
     ON MNL_TRX.RU_NEW_RENEW_SOL_KEY = SMC_ALLOC.SALES_ORDER_LINE_KEY
    AND MNL_TRX.SRC_RPTD_CMNT_4_TXT = SMC_ALLOC.SK_OFFER_ATTRIBUTION_ID_INT 
    AND MNL_TRX.END_TV_DTM = '3500-01-01 00:00:00'
    AND MNL_TRX.RU_NEW_RENEW_SOL_KEY > 0


Post SQL : 



Target1 Name : WI_MT_SLS_ADJ_SMC_ATTRIB_RTR6


Pre SQL : 
DELETE FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR;


Post SQL : 
INSERT INTO $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR
SELECT
MNL_TRX.MANUAL_TRX_KEY
,SMC_ALLOC.SALES_ORDER_LINE_KEY
,MNL_TRX.RU_PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID
,SMC_ALLOC.SALES_MOTION_CD 
,SMC_ALLOC.DV_ALLOCATION_PCT
,SMC_ALLOC.DV_SERVICE_CATEGORY_CD AS DV_SERVICE_CATEGORY_CD
,'N' AS DV_OA_FLG
,CURRENT_TIMESTAMP(0) AS START_TV_DTM 
,SMC_ALLOC.END_TV_DTM  
,SMC_ALLOC.DV_SOURCE_TYPE 
,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM
,USER AS EDW_CREATE_USER
,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM
,USER AS EDW_UPDATE_USER
,CAST(-7777 AS BIGINT) AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
,SMC_ALLOC.SALES_MOTION_TIMING_CD 
FROM (
SELECT 
TRX.MINOR_LN_EXAAS_SUBSCR_SOL_KEY AS SALES_ORDER_LINE_KEY,
CAST(MT.SK_OFFER_ATTRIBUTION_ID_INT AS VARCHAR(15) ) AS SK_OFFER_ATTRIBUTION_ID_INT,
MT.SALES_MOTION_CD,
MT.SALES_MOTION_TIMING_CD,
MT.DV_SOURCE_TYPE,
MT.DV_SERVICE_CATEGORY_CD,
MT.END_TV_DTM,
SUM(MT.DV_ALLOCATION_PCT) AS DV_ALLOCATION_PCT,
CAST(TRX.SK_TRX_ID_INT  AS VARCHAR(15)) AS SK_TRX_ID_INT
FROM  $$SLSORDVWDB.MT_XAAS_SLS_MOTION_ATTRIB MT
INNER JOIN $$NRTNCRVWDB.N_XAAS_SO_SBCR_ITM_STRX_NRT_TV TRX
ON MT.SO_SBSCRPTN_ITM_SLS_TRX_KEY=TRX.SO_SBSCRPTN_ITM_SLS_TRX_KEY
WHERE MT.END_TV_DTM = '3500-01-01 00:00:00'
AND SK_OFFER_ATTRIBUTION_ID_INT > 0 
AND MT.EDW_UPDATE_DTM >  '$$LastExtractDate'
AND MT.BUNDLE_FLG='N'
GROUP BY 1,2,3,4,5,6,7 ,9) SMC_ALLOC
INNER JOIN   $$SLSORDVWDB.N_SALES_ADJ_REBOK_MNL_TRX_TV  MNL_TRX 
ON   MNL_TRX.SRC_RPTD_CMNT_6_TXT= SMC_ALLOC.SK_TRX_ID_INT
AND  MNL_TRX.SRC_RPTD_CMNT_4_TXT = SMC_ALLOC.SK_OFFER_ATTRIBUTION_ID_INT 
AND MNL_TRX.END_TV_DTM = '3500-01-01 00:00:00' ;

DELETE FROM $$STGDB.WI_MTK_WITH_ZERO_SPLIT;

INSERT INTO $$STGDB.WI_MTK_WITH_ZERO_SPLIT
SELECT MANUAL_TRX_KEY, 
DV_ENTERPRISE_INV_SKU_ID,
1.000000 AS DV_ALLOCATION_PCT , 
COUNT(*) CNT_REC 
FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR
WHERE SALES_ORDER_LINE_KEY> 0
AND AR_TRX_LINE_KEY < 0
GROUP BY 1,2,3
HAVING SUM(DV_ALLOCATION_PCT) = 0 ;

COLLECT STATISTICS COLUMN (MANUAL_TRX_KEY,DV_ENTERPRISE_INV_SKU_ID ) ON $$STGDB.WI_MTK_WITH_ZERO_SPLIT;

UPDATE WI 
FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI ,
$$STGDB.WI_MTK_WITH_ZERO_SPLIT ST
SET DV_ALLOCATION_PCT = ST.DV_ALLOCATION_PCT/CNT_REC
WHERE WI.MANUAL_TRX_KEY = ST.MANUAL_TRX_KEY
AND WI.DV_ENTERPRISE_INV_SKU_ID = ST.DV_ENTERPRISE_INV_SKU_ID
AND WI.SALES_ORDER_LINE_KEY > 0
AND AR_TRX_LINE_KEY < 0;

CALL COLLECT_STATS_WRAP('$$STGDB','WI_MT_SLS_ADJ_SMC_ATTRIB_RTR','D');


Source2 Name : SQ_MT_RTNR_SMC_ALLOCATION


Pre SQL : 



SQL Query : 
SELECT
    MNL_TRX.MANUAL_TRX_KEY
   ,SMC_ALLOC.SALES_ORDER_LINE_KEY
   ,MNL_TRX.RU_PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID
   ,SMC_ALLOC.SALES_MOTION_CD 
   ,SMC_ALLOC.DV_ALLOCATION_PCT
   ,SMC_ALLOC.DV_SERVICE_CATEGORY_CD AS DV_SERVICE_CATEGORY_CD
   ,'N' AS DV_OA_FLG
   ,CURRENT_TIMESTAMP(0) AS START_TV_DTM 
   ,SMC_ALLOC.END_TV_DTM 
   /*,'RTNR' AS DV_SOURCE_TYPE  */ /* DV SOURCE TYPE derivation change to accommodate SQ */
   ,SMC_ALLOC.DV_SOURCE_TYPE 
   ,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM
   ,USER AS EDW_CREATE_USER
   ,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM
   ,USER AS EDW_UPDATE_USER
   ,CAST(-7777 AS BIGINT) AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
   ,SMC_ALLOC.SALES_MOTION_TIMING_CD 
  FROM $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
  INNER JOIN $$SLSORDVWDB.N_SALES_ADJ_REBOK_MNL_TRX_TV MNL_TRX 
     ON MNL_TRX.RU_NEW_RENEW_SOL_KEY = SMC_ALLOC.SALES_ORDER_LINE_KEY
    AND MNL_TRX.END_TV_DTM = '3500-01-01 00:00:00'
    AND MNL_TRX.RU_NEW_RENEW_SOL_KEY > 0
    AND SMC_ALLOC.END_TV_DTM = '3500-01-01 00:00:00'  
   WHERE SMC_ALLOC.EDW_UPDATE_DTM >  '$$LastExtractDate'
AND NOT EXISTS (SELECT 1 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI WHERE WI.MANUAL_TRX_KEY  = MNL_TRX.MANUAL_TRX_KEY )

 UNION ALL

   SELECT
     MNL_TRX.MANUAL_TRX_KEY
    ,SMC_ALLOC.SALES_ORDER_LINE_KEY
    --,MNL_TRX.RU_PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID
	,MNL_TRX.ATTRIBUTED_PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID/*REBATE CHANGES*/
    ,SMC_ALLOC.SALES_MOTION_CD 
    ,SMC_ALLOC.DV_ALLOCATION_PCT
    ,SMC_ALLOC.DV_SERVICE_CATEGORY_CD AS DV_SERVICE_CATEGORY_CD
    ,'N' AS DV_OA_FLG
    ,CURRENT_TIMESTAMP(0) AS START_TV_DTM 
    ,SMC_ALLOC.END_TV_DTM
    /*,'RTNR' AS DV_SOURCE_TYPE */ /* DV SOURCE TYPE derivation change to accommodate SQ */
	,SMC_ALLOC.DV_SOURCE_TYPE
    ,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM
    ,USER AS EDW_CREATE_USER
    ,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM
    ,USER AS EDW_UPDATE_USER
	,AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
	,SMC_ALLOC.SALES_MOTION_TIMING_CD 
   FROM $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
   INNER JOIN (SELECT BK_RBT_ADJ_LINE_NUMBER_INT
				,RU_NEW_RENEW_SOL_KEY
				,MANUAL_TRX_KEY
				,RU_BK_NEW_RENEW_POS_TRX_ID_INT
				,ATTRIBUTED_PRODUCT_KEY
				,REBATE_ADJUSTMENT_DATETIME
				,EDW_UPDATE_DATETIME
				,SALES_MOTION_CD
				,AR_TRX_LINE_KEY
			FROM  $$NRTNCRVWDB.N_REBATE_ADJUSTMENT_LINE_NRT
		QUALIFY ROW_NUMBER() OVER ( PARTITION BY MANUAL_TRX_KEY,RU_BK_NEW_RENEW_POS_TRX_ID_INT,ATTRIBUTED_PRODUCT_KEY,RU_NEW_RENEW_SOL_KEY          
		ORDER BY BK_RBT_ADJ_LINE_NUMBER_INT DESC)=1 ) MNL_TRX 
      ON MNL_TRX.RU_NEW_RENEW_SOL_KEY = SMC_ALLOC.SALES_ORDER_LINE_KEY
     --AND MNL_TRX.END_TV_DTM = '3500-01-01 00:00:00'
     AND MNL_TRX.RU_NEW_RENEW_SOL_KEY > 0
     AND SMC_ALLOC.END_TV_DTM = '3500-01-01 00:00:00'  
	 WHERE SMC_ALLOC.EDW_UPDATE_DTM >  '$$LastExtractDate'


Post SQL : 



Target2 Name : WI_MT_SLS_ADJ_SMC_ATTRIB_RTR2


Pre SQL : 
/* DELETE FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR; */


Post SQL : 
CALL COLLECT_STATS_WRAP('$$STGDB','WI_MT_SLS_ADJ_SMC_ATTRIB_RTR','D');

CREATE MULTISET VOLATILE TABLE MT_XAAS_RTNR_SMC_ALLOCATION
AS
(SELECT 
SMC_FINAL.SO_SBSCRPTN_ITM_SLS_TRX_KEY,
SMC_FINAL.SK_TRX_ID_INT,
SMC_FINAL.SALES_ORDER_LINE_KEY,
SMC_FINAL.SALES_MOTION_CD,
SMC_FINAL.DV_ALLOCATION_PCT/SMC_FINAL.SUM_SPLIT AS DV_ALLOCATION_PCT,
SMC_FINAL.DV_SERVICE_CATEGORY_CD,
SMC_FINAL.START_TV_DTM , 
SMC_FINAL.END_TV_DTM,
SMC_FINAL.SALES_MOTION_TIMING_CD SALES_MOTION_TIMING_CD,
SMC_FINAL.DV_SOURCE_TYPE DV_SOURCE_TYPE,
RENEWAL_GAP_DAYS,
EDW_UPDATE_DTM
FROM (SELECT SO_SBSCRPTN_ITM_SLS_TRX_KEY,
SK_TRX_ID_INT,
SMC.SALES_ORDER_LINE_KEY,
SMC.SALES_MOTION_CD,
SUM(SMC.DV_ALLOCATION_PCT ) AS DV_ALLOCATION_PCT,
SMC.SUM_SPLIT ,
smc.DV_SERVICE_CATEGORY_CD,
SMC.START_TV_DTM , 
SMC.END_TV_DTM,
coalesce(SMC.SALES_MOTION_TIMING_CD,'UNKNOWN') SALES_MOTION_TIMING_CD,
SMC.DV_SOURCE_TYPE DV_SOURCE_TYPE,/* DV SOURCE TYPE ADDITION to accommodate SQ */
RENEWAL_GAP_DAYS,EDW_UPDATE_DTM
FROM
(
SELECT SO_SBSCRPTN_ITM_SLS_TRX_KEY,SK_TRX_ID_INT,
SALES_ORDER_LINE_KEY,
SALES_MOTION_CD,
DV_ALLOCATION_PCT,
CASE WHEN SUM_SPLIT=0 THEN 1 ELSE SUM_SPLIT END AS SUM_SPLIT,
DV_SERVICE_CATEGORY_CD,
START_TV_DTM,
END_TV_DTM,
SALES_MOTION_TIMING_CD,
CAST( 'RTNR' AS VARCHAR(10)) AS DV_SOURCE_TYPE,
RENEWAL_GAP_DAYS,EDW_UPDATE_DTM
FROM 
(SELECT SO_SBSCRPTN_ITM_SLS_TRX_KEY,SK_TRX_ID_INT,
  MINOR_LN_EXAAS_SUBSCR_SOL_KEY AS  SALES_ORDER_LINE_KEY,
SALES_MOTION_CD,
DV_ALLOCATION_PCT,
SUM(DV_ALLOCATION_PCT) OVER(PARTITION BY SK_TRX_ID_INT) SUM_SPLIT,
DV_SERVICE_CATEGORY_CD,  
MAX(START_TV_DTM) OVER(PARTITION BY SK_TRX_ID_INT) START_TV_DTM, /* Changed logic as part of May 2nd release */
    MAX(END_TV_DTM) OVER(PARTITION BY SK_TRX_ID_INT) END_TV_DTM,
SALES_MOTION_TIMING_CD,
RENEWAL_GAP_DAYS,EDW_UPDATE_DTM
FROM (SELECT MT.*,MINOR_LN_EXAAS_SUBSCR_SOL_KEY,
CAST(TRX.SK_TRX_ID_INT  AS VARCHAR(15)) AS SK_TRX_ID_INT 
FROM $$SLSORDVWDB.MT_XAAS_SLS_MOTION_ATTRIB MT
INNER JOIN $$NRTNCRVWDB.N_XAAS_SO_SBCR_ITM_STRX_NRT_TV TRX
ON MT.SO_SBSCRPTN_ITM_SLS_TRX_KEY=TRX.SO_SBSCRPTN_ITM_SLS_TRX_KEY
WHERE MT.END_TV_DTM = '3500-01-01 00:00:00'
AND TRX.END_TV_DTM = '3500-01-01 00:00:00'
AND SK_OFFER_ATTRIBUTION_ID_INT > 0 
AND MT.EDW_UPDATE_DTM >  '$$LastExtractDate'
AND MT.BUNDLE_FLG='N')INCR
) SMC1
) SMC
GROUP BY 1,2,3,4,6,7,8,9,10,11,12,13
) SMC_FINAL) WITH DATA AND STATS PRIMARY INDEX(SO_SBSCRPTN_ITM_SLS_TRX_KEY) ON COMMIT PRESERVE ROWS;

INSERT INTO $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR
SELECT
    MNL_TRX.MANUAL_TRX_KEY
   ,SMC_ALLOC.SALES_ORDER_LINE_KEY
   ,MNL_TRX.RU_PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID
   ,SMC_ALLOC.SALES_MOTION_CD 
   ,SMC_ALLOC.DV_ALLOCATION_PCT
   ,SMC_ALLOC.DV_SERVICE_CATEGORY_CD AS DV_SERVICE_CATEGORY_CD
   ,'N' AS DV_OA_FLG
   ,CURRENT_TIMESTAMP(0) AS START_TV_DTM 
   ,SMC_ALLOC.END_TV_DTM 
   /*,'RTNR' AS DV_SOURCE_TYPE  */ /* DV SOURCE TYPE derivation change to accommodate SQ */
   ,SMC_ALLOC.DV_SOURCE_TYPE 
   ,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM
   ,USER AS EDW_CREATE_USER
   ,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM
   ,USER AS EDW_UPDATE_USER
   ,CAST(-7777 AS BIGINT) AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
   ,SMC_ALLOC.SALES_MOTION_TIMING_CD 
  FROM  MT_XAAS_RTNR_SMC_ALLOCATION SMC_ALLOC
  INNER JOIN $$SLSORDVWDB.N_SALES_ADJ_REBOK_MNL_TRX_TV MNL_TRX 
     ON MNL_TRX.SRC_RPTD_CMNT_6_TXT= SMC_ALLOC.SK_TRX_ID_INT
    AND MNL_TRX.END_TV_DTM = '3500-01-01 00:00:00' 
   WHERE SMC_ALLOC.EDW_UPDATE_DTM >  '$$LastExtractDate'
AND NOT EXISTS (SELECT 1 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI WHERE WI.MANUAL_TRX_KEY  = MNL_TRX.MANUAL_TRX_KEY );


Source3 Name : SQ_MT_RTNR_SMC_ALLOCATION1


Pre SQL : 



SQL Query : 
SELECT
    MNL_TRX.MANUAL_TRX_KEY
   ,SMC_ALLOC.SALES_ORDER_LINE_KEY
   ,MNL_TRX.RU_PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID
   ,SMC_ALLOC.SALES_MOTION_CD 
   ,SMC_ALLOC.DV_ALLOCATION_PCT
   ,SMC_ALLOC.DV_SERVICE_CATEGORY_CD AS DV_SERVICE_CATEGORY_CD
   ,'N' AS DV_OA_FLG 
   ,CURRENT_TIMESTAMP(0) AS START_TV_DTM                 
   ,SMC_ALLOC.END_TV_DTM 
   /*,'RTNR' AS DV_SOURCE_TYPE  */ /* DV SOURCE TYPE derivation change to accommodate SQ */
   ,SMC_ALLOC.DV_SOURCE_TYPE 
   ,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM
   ,USER AS EDW_CREATE_USER
   ,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM
   ,USER AS EDW_UPDATE_USER
   ,CAST(-7777 AS BIGINT) AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
   ,SMC_ALLOC.SALES_MOTION_TIMING_CD 
  FROM $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
  INNER JOIN ( SELECT * FROM $$SLSORDVWDB.N_POS_TRX_LINE_AS_NEW_OR_RNWL 
               QUALIFY ROW_NUMBER() OVER(PARTITION BY BK_POS_TRANSACTION_ID_INT ORDER BY SALES_ORDER_LINE_KEY ) = 1 ) POS  
      ON POS.SALES_ORDER_LINE_KEY = SMC_ALLOC.SALES_ORDER_LINE_KEY
  INNER JOIN $$SLSORDVWDB.N_SALES_ADJ_REBOK_MNL_TRX_TV MNL_TRX 
      ON MNL_TRX.RU_BK_NEW_RENEW_POS_TRX_ID_INT = POS.BK_POS_TRANSACTION_ID_INT
     AND MNL_TRX.END_TV_DTM = '3500-01-01 00:00:00'
     AND MNL_TRX.RU_BK_NEW_RENEW_POS_TRX_ID_INT > 0 
     AND SMC_ALLOC.END_TV_DTM = '3500-01-01 00:00:00'
   WHERE /*SMC_ALLOC.EDW_UPDATE_DTM >  '$$LastExtractDate'*/
   1=2

UNION ALL

SELECT
     MNL_TRX.MANUAL_TRX_KEY
    ,SMC_ALLOC.SALES_ORDER_LINE_KEY
    --,MNL_TRX.RU_PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID
	,MNL_TRX.ATTRIBUTED_PRODUCT_KEY AS DV_ENTERPRISE_INV_SKU_ID/*REBATE CHANGES*/
    ,SMC_ALLOC.SALES_MOTION_CD 
    ,SMC_ALLOC.DV_ALLOCATION_PCT
    ,SMC_ALLOC.DV_SERVICE_CATEGORY_CD AS DV_SERVICE_CATEGORY_CD
    ,'N' AS DV_OA_FLG 
    ,CURRENT_TIMESTAMP(0) AS START_TV_DTM
    ,SMC_ALLOC.END_TV_DTM 
    /*,'RTNR' AS DV_SOURCE_TYPE  */ /* DV SOURCE TYPE derivation change to accommodate SQ */
	,SMC_ALLOC.DV_SOURCE_TYPE
    ,CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM
    ,USER AS EDW_CREATE_USER
    ,CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM
    ,USER AS EDW_UPDATE_USER
	,AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
	,SMC_ALLOC.SALES_MOTION_TIMING_CD 
   FROM $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
   INNER JOIN ( SELECT * FROM $$SLSORDVWDB.N_POS_TRX_LINE_AS_NEW_OR_RNWL 
                QUALIFY ROW_NUMBER() OVER(PARTITION BY BK_POS_TRANSACTION_ID_INT ORDER BY SALES_ORDER_LINE_KEY ) = 1 ) POS  
       ON POS.SALES_ORDER_LINE_KEY = SMC_ALLOC.SALES_ORDER_LINE_KEY
   INNER JOIN (SELECT BK_RBT_ADJ_LINE_NUMBER_INT
				,RU_NEW_RENEW_SOL_KEY
				,MANUAL_TRX_KEY
				,RU_BK_NEW_RENEW_POS_TRX_ID_INT
				,ATTRIBUTED_PRODUCT_KEY
				,REBATE_ADJUSTMENT_DATETIME
				,EDW_UPDATE_DATETIME
				,SALES_MOTION_CD
				,AR_TRX_LINE_KEY 
		FROM  $$NRTNCRVWDB.N_REBATE_ADJUSTMENT_LINE_NRT
		QUALIFY ROW_NUMBER() OVER (PARTITION BY MANUAL_TRX_KEY,RU_BK_NEW_RENEW_POS_TRX_ID_INT,ATTRIBUTED_PRODUCT_KEY,RU_NEW_RENEW_SOL_KEY          
		ORDER BY BK_RBT_ADJ_LINE_NUMBER_INT DESC)=1) MNL_TRX 
       ON MNL_TRX.RU_BK_NEW_RENEW_POS_TRX_ID_INT = POS.BK_POS_TRANSACTION_ID_INT
      --AND MNL_TRX.END_TV_DTM = '3500-01-01 00:00:00'
      AND MNL_TRX.RU_BK_NEW_RENEW_POS_TRX_ID_INT > 0 
      AND SMC_ALLOC.END_TV_DTM = '3500-01-01 00:00:00'
    WHERE /*SMC_ALLOC.EDW_UPDATE_DTM >  '$$LastExtractDate'*/
	1=2


Post SQL : 



Target3 Name : WI_MT_SLS_ADJ_SMC_ATTRIB_RTR3


Pre SQL : 



Post SQL : 
CALL COLLECT_STATS_WRAP('$$STGDB','WI_MT_SLS_ADJ_SMC_ATTRIB_RTR','D');


Source4 Name : SQ_WI_MT_SLS_ADJ_SMC_ATTRIB_RTR


Pre SQL : 



SQL Query : 
SELECT     
   MANUAL_TRX_KEY
     ,SALES_ORDER_LINE_KEY
     ,DV_ENTERPRISE_INV_SKU_ID
     ,SALES_MOTION_CD
     ,DV_ALLOCATION_PCT
     ,DV_SERVICE_CATEGORY_CD
     ,DV_OA_FLG                                         
     ,START_TV_DTM
     ,END_TV_DTM
     ,DV_SOURCE_TYPE
     ,EDW_CREATE_DTM
     ,EDW_CREATE_USER
     ,EDW_UPDATE_DTM
     ,EDW_UPDATE_USER
     ,SALES_MOTION_TIMING_CD
,-9999 AS SK_OFFER_ATTRIBUTION_ID_INT
  FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI
  WHERE WI.AR_TRX_LINE_KEY < 0 /* Added condition to process AR rebates separately in next pipeline*/
  AND NOT EXISTS ( SELECT 1
                      FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT
         WHERE WI.MANUAL_TRX_KEY = MT.MANUAL_TRX_KEY
           AND WI.DV_ENTERPRISE_INV_SKU_ID = MT.DV_ENTERPRISE_INV_SKU_ID
           AND WI.START_TV_DTM = MT.START_TV_DTM
        )


Post SQL : 



Target4 Name : MT_SLS_ADJ_SLS_MOTION_ATTRIB1


Pre SQL : 
/* START: SMR Aug 16th changes - To prevent unnecessary changes going into MT table */
	DELETE FROM $$STGDB.WI_ADJ_SMC_ATTRIB_RTR_CHNG_CHK ;

	INSERT INTO $$STGDB.WI_ADJ_SMC_ATTRIB_RTR_CHNG_CHK
	SELECT
		MANUAL_TRX_KEY
		 ,SALES_ORDER_LINE_KEY
		 ,DV_ENTERPRISE_INV_SKU_ID
		 ,SALES_MOTION_CD
		 ,DV_ALLOCATION_PCT
		 ,DV_SERVICE_CATEGORY_CD
		 ,DV_OA_FLG                                         
		 ,START_TV_DTM
		 ,END_TV_DTM
		 ,DV_SOURCE_TYPE
		 ,SALES_MOTION_TIMING_CD
		 ,AR_TRX_LINE_KEY
	 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI
	WHERE EXISTS ( SEL 1 
					FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT_ADJ
					WHERE MT_ADJ.END_TV_DTM = '3500-01-01 00:00:00'
					  AND WI.MANUAL_TRX_KEY = MT_ADJ.MANUAL_TRX_KEY
					  AND WI.SALES_ORDER_LINE_KEY = MT_ADJ.SALES_ORDER_LINE_KEY
					  AND WI.DV_ENTERPRISE_INV_SKU_ID = MT_ADJ.DV_ENTERPRISE_INV_SKU_ID
					  AND WI.SALES_MOTION_CD = MT_ADJ.SALES_MOTION_CD
					  AND WI.DV_ALLOCATION_PCT = MT_ADJ.DV_ALLOCATION_PCT
					  AND WI.DV_SERVICE_CATEGORY_CD = MT_ADJ.DV_SERVICE_CATEGORY_CD
					  AND WI.DV_SOURCE_TYPE = MT_ADJ.DV_SOURCE_TYPE ) ;

	COLLECT STATS ON $$STGDB.WI_ADJ_SMC_ATTRIB_RTR_CHNG_CHK;

	DELETE FROM $$STGDB.WI_ADJ_SMC_ATTRIB_RTR_CHNG_CHK
	WHERE ( MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID ) IN
			( SEL MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID
				FROM $$STGDB.WI_ADJ_SMC_ATTRIB_RTR_CHNG_CHK
				 WHERE AR_TRX_LINE_KEY < 0
				GROUP BY 1,2 HAVING ROUND(SUM(DV_ALLOCATION_PCT),4) <> 1.0000		
			) ;

	DELETE FROM $$STGDB.WI_ADJ_SMC_ATTRIB_RTR_CHNG_CHK
	WHERE ( MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID, SALES_ORDER_LINE_KEY ) IN
			( SEL MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID, SALES_ORDER_LINE_KEY
				FROM $$STGDB.WI_ADJ_SMC_ATTRIB_RTR_CHNG_CHK
				 WHERE AR_TRX_LINE_KEY > 0
				GROUP BY 1,2,3 HAVING ROUND(SUM(DV_ALLOCATION_PCT),4) <> 1.0000		
			) ;

	DELETE FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI
	WHERE EXISTS ( SEL 1 
					FROM $$STGDB.WI_ADJ_SMC_ATTRIB_RTR_CHNG_CHK NO_CHNG
					WHERE NO_CHNG.AR_TRX_LINE_KEY < 0
					  AND WI.MANUAL_TRX_KEY = NO_CHNG.MANUAL_TRX_KEY
					  AND WI.DV_ENTERPRISE_INV_SKU_ID = NO_CHNG.DV_ENTERPRISE_INV_SKU_ID
				  ) ;
				  
	DELETE FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI
	WHERE EXISTS ( SEL 1 
					FROM $$STGDB.WI_ADJ_SMC_ATTRIB_RTR_CHNG_CHK NO_CHNG
					WHERE NO_CHNG.AR_TRX_LINE_KEY > 0
					  AND WI.MANUAL_TRX_KEY = NO_CHNG.MANUAL_TRX_KEY
					  AND WI.DV_ENTERPRISE_INV_SKU_ID = NO_CHNG.DV_ENTERPRISE_INV_SKU_ID
					  AND WI.SALES_ORDER_LINE_KEY = NO_CHNG.SALES_ORDER_LINE_KEY
				  ) ;

/* END: SMR Aug 16th changes - To prevent unnecessary changes going into MT table */

UPDATE MT
    FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT , 
         ( SELECT MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID, MAX(START_TV_DTM) MAX_START_DTM
                        FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR  
						WHERE AR_TRX_LINE_KEY < 0 /* Added condition to process AR rebates separately in next pipeline*/
                                  GROUP BY 1,2
                     ) WI
    SET END_TV_DTM = MAX_START_DTM - INTERVAL '1' SECOND,
        EDW_UPDATE_DTM = CURRENT_TIMESTAMP(0)
  WHERE MT.MANUAL_TRX_KEY = WI.MANUAL_TRX_KEY
  AND MT.DV_ENTERPRISE_INV_SKU_ID = WI.DV_ENTERPRISE_INV_SKU_ID
  AND MT.START_TV_DTM <> WI.MAX_START_DTM 
  AND MT.END_TV_DTM = '3500-01-01 00:00:00';


Post SQL : 
CALL COLLECT_STATS_WRAP('$$SLSORDDB','MT_SLS_ADJ_SLS_MOTION_ATTRIB','D');


Source5 Name : SQ_WI_MT_SLS_ADJ_SMC_ATTRIB_RTR1


Pre SQL : 



SQL Query : 
SELECT     
   MANUAL_TRX_KEY
     ,SALES_ORDER_LINE_KEY
     ,DV_ENTERPRISE_INV_SKU_ID
     ,SALES_MOTION_CD
     ,DV_ALLOCATION_PCT
     ,DV_SERVICE_CATEGORY_CD
     ,DV_OA_FLG                                         
     ,START_TV_DTM
     ,END_TV_DTM
     ,DV_SOURCE_TYPE
     ,EDW_CREATE_DTM
     ,EDW_CREATE_USER
     ,EDW_UPDATE_DTM
     ,EDW_UPDATE_USER
     ,SALES_MOTION_TIMING_CD
,-9999 AS SK_OFFER_ATTRIBUTION_ID_INT
  FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI
  WHERE WI.AR_TRX_LINE_KEY > 0 /* Added condition to process AR rebates separately here */
  AND NOT EXISTS ( SELECT 1
                      FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT
         WHERE WI.MANUAL_TRX_KEY = MT.MANUAL_TRX_KEY
           AND WI.DV_ENTERPRISE_INV_SKU_ID = MT.DV_ENTERPRISE_INV_SKU_ID
		   AND WI.SALES_ORDER_LINE_KEY = MT.SALES_ORDER_LINE_KEY /* Added condition to process AR rebates separately here */
           AND WI.START_TV_DTM = MT.START_TV_DTM
        )


Post SQL : 



Target5 Name : MT_SLS_ADJ_SLS_MOTION_ATTRIB2


Pre SQL : 
UPDATE MT
    FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT , 
         ( SELECT MANUAL_TRX_KEY, SALES_ORDER_LINE_KEY, DV_ENTERPRISE_INV_SKU_ID, MAX(START_TV_DTM) MAX_START_DTM
                        FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR  
						WHERE AR_TRX_LINE_KEY > 0 /* Added condition to process AR rebates separately here */
                                  GROUP BY 1,2,3
                     ) WI
    SET END_TV_DTM = MAX_START_DTM - INTERVAL '1' SECOND,
        EDW_UPDATE_DTM = CURRENT_TIMESTAMP(0)
  WHERE MT.MANUAL_TRX_KEY = WI.MANUAL_TRX_KEY
  AND MT.SALES_ORDER_LINE_KEY = WI.SALES_ORDER_LINE_KEY /* Added condition to process AR rebates separately here */
  AND MT.DV_ENTERPRISE_INV_SKU_ID = WI.DV_ENTERPRISE_INV_SKU_ID
  AND MT.START_TV_DTM <> WI.MAX_START_DTM 
  AND MT.END_TV_DTM = '3500-01-01 00:00:00';


Post SQL : 
CALL COLLECT_STATS_WRAP('$$SLSORDDB','MT_SLS_ADJ_SLS_MOTION_ATTRIB','D');