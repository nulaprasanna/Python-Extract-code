


Source1 Name : SQ_N_SALES_ADJ_REBOK_MNL_TRX_TV1


Pre SQL : 



SQL Query : 
SELECT 
   RBK.MANUAL_TRX_KEY,
   RBK.SALES_ORDER_LINE_KEY,
   RBK.ENTERPRISE_OR_INVOICE_SKU AS DV_ENTERPRISE_INV_SKU_ID,
   RBK.SALES_MOTION_CD,
   RBK.ALLOCATION_PCT AS DV_ALLOCATION_PCT,
   RBK.DV_SERVICE_CATEGORY_CD,
   RBK.OA_FLG AS DV_OA_FLG,
   RBK.START_DATE AS START_TV_DTM,
   RBK.END_DATE AS END_TV_DTM,
   RBK.SOURCE AS DV_SOURCE_TYPE,
   CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM , 
   USER AS EDW_CREATE_USER ,
   CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM , 
   USER AS EDW_UPDATE_USER,
   AR_TRX_LINE_KEY,
   RBK.SALES_MOTION_TIMING_CD
  FROM 
  (
   SELECT
     NSLSADJ.MANUAL_TRX_KEY
    ,COALESCE(POS.SALES_ORDER_LINE_KEY, -7777) AS SALES_ORDER_LINE_KEY
    ,NSLSADJ.RU_PRODUCT_KEY AS ENTERPRISE_OR_INVOICE_SKU         
    --,COALESCE(POS.SALES_MOTION_CD, 'UNKNOWN' ) AS SALES_MOTION_CD /* Commented as part of Q3FY20 apr rel */
	,COALESCE(POS.SALES_MOTION_CD, NSLSADJ.SALES_MOTION_CD ) AS SALES_MOTION_CD /* Added as part of Q3FY20 april rel to consider sls adj trx SM  as default SM */
    ,COALESCE(POS.SPLIT_PCT,1.00)AS ALLOCATION_PCT
    ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_SERVICE_CATEGORY_CD 
       ELSE 'TS' 
      END AS DV_SERVICE_CATEGORY_CD
    ,'N'  AS OA_FLG
    ,CURRENT_TIMESTAMP(0) AS START_DATE 
    ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.END_TV_DTM
       ELSE NSLSADJ.END_TV_DTM 
      END AS END_DATE                    
    ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_SOURCE_TYPE /* DV SOURCE TYPE derivation change to accommodate SQ */ 
       ELSE CAST( 'UNKNOWN' AS VARCHAR(10) )
      END AS SOURCE 
	,CAST(-7777 AS BIGINT) AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
	,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.SALES_MOTION_TIMING_CD
       ELSE COALESCE(NSOL.SALES_MOTION_TIMING_CD, 'UNKNOWN' )
      END AS SALES_MOTION_TIMING_CD 
   FROM $$SLSORDVWDB.N_SALES_ADJ_REBOK_MNL_TRX_TV NSLSADJ 
   LEFT JOIN (SELECT * FROM $$SLSORDVWDB.N_POS_TRX_LN_AS_NEW_OR_RNWL_TV 
   /*To get SALES_MOTION_CD and SPLIT_PCT from POS directly*/
--      QUALIFY ROW_NUMBER() OVER(PARTITION BY BK_POS_TRANSACTION_ID_INT ORDER BY SALES_ORDER_LINE_KEY DESC) = 1
WHERE END_TV_DTM='3500-01-01 00:00:00'
 ) POS  
      ON NSLSADJ.RU_BK_NEW_RENEW_POS_TRX_ID_INT = POS.BK_POS_TRANSACTION_ID_INT
        LEFT JOIN (SEL * FROM $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
WHERE  SMC_ALLOC.END_TV_DTM = '3500-01-01 00:00:00' 
AND SALES_MOTION_CD='RENEW'
		QUALIFY ROW_NUMBER() OVER(PARTITION BY SALES_ORDER_LINE_KEY ORDER BY EDW_UPDATE_DTM DESC ) = 1/* To	avoid data explosion in MT_RTNR_SMC_ALLOCATION*/
		)SMC_ALLOC
      ON POS.SALES_ORDER_LINE_KEY = SMC_ALLOC.SALES_ORDER_LINE_KEY
 LEFT JOIN $$STGDB.WI_TTS_SOWB_SOL_LINES NSOL
           ON POS.SALES_ORDER_LINE_KEY = NSOL.SALES_ORDER_LINE_KEY
	 /*LEFT JOIN ( SELECT NSOL.* FROM $$SLSORDVWDB.N_SALES_ORDER_LINE_TV NSOL 
--/*	 INNER JOIN $$SLSORDVWDB.N_SALES_ORDER NSO
	--		     ON NSO.SALES_ORDER_KEY = NSOL.SALES_ORDER_KEY 
      WHERE NSOL.SS_CODE = 'CG'
      AND NSOL.END_TV_DATETIME = '3500-01-01 00:00:00'
	  AND (EXISNSOL (
	  SELECT 1 FROM (SELECT 
		 P.ITEM_KEY
		 FROM
		 $$COMREFVWDB.N_PRODUCT P
		 INNER JOIN 
		 $$COMREFVWDB.MT_SVC_FINANCE_HIERARCHY SFH ON P.RU_BK_SERVICE_PROD_SUBGROUP_ID = SFH.BK_PRODUCT_SUBGROUP_ID
		 WHERE P.PRDT_SETUP_CLASSIFICATION_CD <> 'SOFTWARE' --EXCLUDE SW
		 AND SFH.BK_SERVICE_CATEGORY_ID = 'TECHNICAL SUPPORT SERVICES' 
		 AND SFH.BK_ALLOCATED_SERVC_GROUP_ID NOT IN
		 ( 'AS CORE' ,  'AS SUBSCRIPTION' , 'FOCUSED TECHNICAL SUPPORT SERVICES' , 'CLOUD MANAGED SERVICES' )--EXCLUDE FNSOL,CMS,AS
		 AND RU_BK_SERVICE_PROD_SUBGROUP_ID NOT IN (SELECT BK_PRDT_SUBGROUP_ID FROM ( SELECT * FROM $$SERVICEVWDB.N_GENERIC_SVC_PRDT_ATTR WHERE BK_GSP_ATTR_NAME = 'SW SERVICE CATEGORY' ) WI ) --EXCLUDE SWSS
		 ) WI
	   WHERE NSOL.PRODUCT_KEY = WI.ITEM_KEY
      )
	  OR ( NSOL.BK_SO_SRC_NAME IN ('Manual', 'Copy' ) AND NSOL.PRODUCT_KEY NOT IN ( SELECT P.ITEM_KEY FROM
     $$COMREFVWDB.N_PRODUCT P
     INNER JOIN 
     $$COMREFVWDB.MT_SVC_FINANCE_HIERARCHY SFH ON P.RU_BK_SERVICE_PROD_SUBGROUP_ID = SFH.BK_PRODUCT_SUBGROUP_ID
     AND SFH.BK_ALLOCATED_SERVC_GROUP_ID  IN ( 'AS FIXED', 'AS TRANSACTION', 'CLOUD MANAGED SERVICES' ) --EXCLUDE CMS IN SOWB
    )
                )
	  )
        ) NSOL
	ON NSOL.SALES_ORDER_LINE_KEY = POS.SALES_ORDER_LINE_KEY*/
    WHERE NSLSADJ.EDW_UPDATE_DTM > '$$LastExtractDate'
      AND NSLSADJ.END_TV_DTM = '3500-01-01 00:00:00'  
	  AND NSLSADJ.RU_BK_NEW_RENEW_POS_TRX_ID_INT > 0 

UNION ALL

SELECT
      NSLSADJ.MANUAL_TRX_KEY
     ,COALESCE(POS.SALES_ORDER_LINE_KEY, -7777) AS SALES_ORDER_LINE_KEY
     --,NSLSADJ.RU_PRODUCT_KEY AS ENTERPRISE_OR_INVOICE_SKU      
     ,NSLSADJ.ATTRIBUTED_PRODUCT_KEY AS ENTERPRISE_OR_INVOICE_SKU 	/*REBATE CHANGES*/ 
     --,COALESCE(POS.SALES_MOTION_CD, 'UNKNOWN' )AS SALES_MOTION_CD  /* Commented as part of Q3FY20 apr rel */
	 ,COALESCE(POS.SALES_MOTION_CD, NSLSADJ.SALES_MOTION_CD ) AS SALES_MOTION_CD /* Added as part of Q3FY20 april rel to consider sls adj trx SM  as default SM */
     ,COALESCE(POS.SPLIT_PCT,1.00 )AS ALLOCATION_PCT
     ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_SERVICE_CATEGORY_CD 
        ELSE 'TS' 
       END AS DV_SERVICE_CATEGORY_CD
     ,'N'  AS OA_FLG
     , CURRENT_TIMESTAMP(0) AS START_DATE 
     ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.END_TV_DTM
        --ELSE CAST(NSLSADJ.END_TV_DTM AS DATE)
		ELSE CAST('3500-01-01 00:00:00' AS TIMESTAMP(0)) /*REBATE CHANGES*/
       END AS END_DATE                    
     ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_SOURCE_TYPE /* DV SOURCE TYPE derivation change to accommodate SQ */ 
        ELSE CAST( 'NON-RTR-RB' AS VARCHAR(10) )
       END AS SOURCE 
	 ,AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
	 ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.SALES_MOTION_TIMING_CD
        ELSE COALESCE(NSOL.SALES_MOTION_TIMING_CD, 'UNKNOWN' )
       END AS SALES_MOTION_TIMING_CD 
    FROM (SELECT BK_RBT_ADJ_LINE_NUMBER_INT
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
		ORDER BY BK_RBT_ADJ_LINE_NUMBER_INT DESC)=1)NSLSADJ 
    LEFT JOIN ( SELECT * FROM $$SLSORDVWDB.N_POS_TRX_LN_AS_NEW_OR_RNWL_TV
   /*To get SALES_MOTION_CD and SPLIT_PCT from POS directly*/ 
       --QUALIFY ROW_NUMBER() OVER(PARTITION BY BK_POS_TRANSACTION_ID_INT ORDER BY SALES_ORDER_LINE_KEY DESC) = 1 
	   WHERE END_TV_DTM='3500-01-01 00:00:00'
	   ) POS  
       ON NSLSADJ.RU_BK_NEW_RENEW_POS_TRX_ID_INT = POS.BK_POS_TRANSACTION_ID_INT
         LEFT JOIN(SEL * FROM  $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
		  WHERE SMC_ALLOC.END_TV_DTM = '3500-01-01 00:00:00'
AND SALES_MOTION_CD='RENEW'
		     QUALIFY ROW_NUMBER() OVER(PARTITION BY SALES_ORDER_LINE_KEY ORDER BY EDW_UPDATE_DTM DESC ) = 1/* To	avoid data explosion in MT_RTNR_SMC_ALLOCATION*/
			)SMC_ALLOC			 
       ON POS.SALES_ORDER_LINE_KEY = SMC_ALLOC.SALES_ORDER_LINE_KEY
      LEFT JOIN $$STGDB.WI_TTS_SOWB_SOL_LINES NSOL
           ON POS.SALES_ORDER_LINE_KEY = NSOL.SALES_ORDER_LINE_KEY
   /*LEFT JOIN ( SELECT NSOL.* FROM $$SLSORDVWDB.N_SALES_ORDER_LINE_TV NSOL 
 --/*  INNER JOIN $$SLSORDVWDB.N_SALES_ORDER NSO
   --      ON NSO.SALES_ORDER_KEY = NSOL.SALES_ORDER_KEY 
       WHERE NSOL.SS_CODE = 'CG'
       AND NSOL.END_TV_DATETIME = '3500-01-01 00:00:00'
    AND (EXISNSOL (
    SELECT 1 FROM (SELECT 
    P.ITEM_KEY
    FROM
    $$COMREFVWDB.N_PRODUCT P
    INNER JOIN 
    $$COMREFVWDB.MT_SVC_FINANCE_HIERARCHY SFH ON P.RU_BK_SERVICE_PROD_SUBGROUP_ID = SFH.BK_PRODUCT_SUBGROUP_ID
    WHERE P.PRDT_SETUP_CLASSIFICATION_CD <> 'SOFTWARE' --EXCLUDE SW
    AND SFH.BK_SERVICE_CATEGORY_ID = 'TECHNICAL SUPPORT SERVICES' 
    AND SFH.BK_ALLOCATED_SERVC_GROUP_ID NOT IN
    ( 'AS CORE' ,  'AS SUBSCRIPTION' , 'FOCUSED TECHNICAL SUPPORT SERVICES' , 'CLOUD MANAGED SERVICES' )--EXCLUDE FNSOL,CMS,AS
    AND RU_BK_SERVICE_PROD_SUBGROUP_ID NOT IN (SELECT BK_PRDT_SUBGROUP_ID FROM ( SELECT * FROM $$SERVICEVWDB.N_GENERIC_SVC_PRDT_ATTR WHERE BK_GSP_ATTR_NAME = 'SW SERVICE CATEGORY' ) WI ) --EXCLUDE SWSS
    ) WI
     WHERE NSOL.PRODUCT_KEY = WI.ITEM_KEY
       )
    OR ( NSOL.BK_SO_SRC_NAME IN ('Manual', 'Copy' ) AND NSOL.PRODUCT_KEY NOT IN ( SELECT P.ITEM_KEY FROM
      $$COMREFVWDB.N_PRODUCT P
      INNER JOIN 
      $$COMREFVWDB.MT_SVC_FINANCE_HIERARCHY SFH ON P.RU_BK_SERVICE_PROD_SUBGROUP_ID = SFH.BK_PRODUCT_SUBGROUP_ID
      AND SFH.BK_ALLOCATED_SERVC_GROUP_ID  IN ( 'AS FIXED', 'AS TRANSACTION', 'CLOUD MANAGED SERVICES' ) --EXCLUDE CMS IN SOWB
     )
                 )
    )
         ) NSOL
  ON NSOL.SALES_ORDER_LINE_KEY = POS.SALES_ORDER_LINE_KEY*/
     WHERE NSLSADJ.EDW_UPDATE_DATETIME >= (SELECT LAST_EXTRACT_DATE FROM $$ETLVWDB.CTL_ETL_LAST_EXTRACT_DATES  
       WHERE JOB_STREAM_ID='wf_MT_SLS_ADJ_SMC_ATTRIB_NRTR' AND TABLE_NAME='N_REBATE_ADJUSTMENT_LINE_NRT')
      -- AND NSLSADJ.END_TV_DTM = '3500-01-01 00:00:00'  /*REMOVED AS PART OF REBATE CHANGE*/
    AND NSLSADJ.RU_BK_NEW_RENEW_POS_TRX_ID_INT > 0 	  
	  
	  
) RBK
    WHERE 
 NOT EXISTS  ( SELECT 1 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI WHERE WI.MANUAL_TRX_KEY = RBK.MANUAL_TRX_KEY )
 AND NOT EXISTS  ( SELECT 1 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_PNVR WI WHERE WI.MANUAL_TRX_KEY = RBK.MANUAL_TRX_KEY )
 AND NOT EXISTS  ( SELECT 1 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR WI WHERE WI.MANUAL_TRX_KEY = RBK.MANUAL_TRX_KEY )


Post SQL : 



Target1 Name : WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR3


Pre SQL : 
DELETE FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR;


Post SQL : 
CALL COLLECT_STATS_WRAP('$$STGDB','WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR','D');


Source2 Name : SQ_N_SALES_ADJ_REBOK_MNL_TRX_TV


Pre SQL : 



SQL Query : 
SELECT 
      RBK.MANUAL_TRX_KEY,
      RBK.SALES_ORDER_LINE_KEY,
      RBK.ENTERPRISE_OR_INVOICE_SKU AS DV_ENTERPRISE_INV_SKU_ID, 
      RBK.SALES_MOTION_CD,
      RBK.ALLOCATION_PCT AS DV_ALLOCATION_PCT,
      RBK.DV_SERVICE_CATEGORY_CD,
      RBK.OA_FLG AS DV_OA_FLG,
      RBK.START_DATE AS START_TV_DTM,
      RBK.END_DATE AS END_TV_DTM,
      RBK.SOURCE AS DV_SOURCE_TYPE,
      CURRENT_TIMESTAMP(0) AS EDW_CREATE_DTM , 
      USER AS EDW_CREATE_USER ,
      CURRENT_TIMESTAMP(0) AS EDW_UPDATE_DTM , 
      USER AS EDW_UPDATE_USER
	  ,AR_TRX_LINE_KEY
	  ,RBK.SALES_MOTION_TIMING_CD
  FROM 
     (
   SELECT
     NSLSADJ.MANUAL_TRX_KEY
    ,NSLSADJ.RU_NEW_RENEW_SOL_KEY AS SALES_ORDER_LINE_KEY
    ,NSLSADJ.RU_PRODUCT_KEY AS ENTERPRISE_OR_INVOICE_SKU         
    ,CASE --WHEN NSLSADJ.RU_NEW_RENEW_SOL_KEY <0 THEN NSLSADJ.SALES_MOTION_CD /* Commented as part of Q3FY20 rel to simplify code */
   WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.SALES_MOTION_CD
       --ELSE 'UNKNOWN' /* Commented as part of Q3FY20 rel to simplify code */
	   ELSE NSLSADJ.SALES_MOTION_CD /* Added as part of Q3FY20 rel to consider sls adj trx SM  as default SM */
      END AS SALES_MOTION_CD 
    ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_ALLOCATION_PCT
       ELSE 1.000000 
      END AS ALLOCATION_PCT
    ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_SERVICE_CATEGORY_CD 
          ELSE 'TS' 
      END AS DV_SERVICE_CATEGORY_CD
    ,'N' AS OA_FLG
    ,CURRENT_TIMESTAMP(0) AS START_DATE 
    ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.END_TV_DTM
       ELSE NSLSADJ.END_TV_DTM END AS END_DATE                    
    ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_SOURCE_TYPE /* DV SOURCE TYPE derivation change to accommodate SQ */
       ELSE CAST( 'UNKNOWN' AS VARCHAR(10))
       END AS SOURCE 
    ,CAST(-7777 AS BIGINT) AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
	,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.SALES_MOTION_TIMING_CD
       ELSE COALESCE(NSOL.SALES_MOTION_TIMING_CD, 'UNKNOWN' )
      END AS SALES_MOTION_TIMING_CD 
   FROM $$SLSORDVWDB.N_SALES_ADJ_REBOK_MNL_TRX_TV NSLSADJ 
   LEFT JOIN $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
       ON NSLSADJ.RU_NEW_RENEW_SOL_KEY = SMC_ALLOC.SALES_ORDER_LINE_KEY
       AND SMC_ALLOC.END_TV_DTM = '3500-01-01 00:00:00'
LEFT JOIN $$STGDB.WI_TTS_SOWB_SOL_LINES NSOL
           ON NSLSADJ.RU_NEW_RENEW_SOL_KEY = NSOL.SALES_ORDER_LINE_KEY
   /*LEFT JOIN ( SELECT NSOL.* FROM $$SLSORDVWDB.N_SALES_ORDER_LINE_TV NSOL 
--  /*INNER JOIN $$SLSORDVWDB.N_SALES_ORDER NSO
			   --  ON NSO.SALES_ORDER_KEY = NSOL.SALES_ORDER_KEY  
      WHERE NSOL.SS_CODE = 'CG'
      AND NSOL.END_TV_DATETIME = '3500-01-01 00:00:00'
	  AND (EXISTS (
  SELECT 1 FROM (SELECT 
     P.ITEM_KEY
     FROM
     $$COMREFVWDB.N_PRODUCT P
     INNER JOIN 
     $$COMREFVWDB.MT_SVC_FINANCE_HIERARCHY SFH ON P.RU_BK_SERVICE_PROD_SUBGROUP_ID = SFH.BK_PRODUCT_SUBGROUP_ID
     WHERE P.PRDT_SETUP_CLASSIFICATION_CD <> 'SOFTWARE' --EXCLUDE SW
     AND SFH.BK_SERVICE_CATEGORY_ID = 'TECHNICAL SUPPORT SERVICES' 
     AND SFH.BK_ALLOCATED_SERVC_GROUP_ID NOT IN
     ( 'AS CORE' ,  'AS SUBSCRIPTION' , 'FOCUSED TECHNICAL SUPPORT SERVICES' , 'CLOUD MANAGED SERVICES' )--EXCLUDE FTS,CMS,AS
     AND RU_BK_SERVICE_PROD_SUBGROUP_ID NOT IN (SELECT BK_PRDT_SUBGROUP_ID FROM ( SELECT * FROM $$SERVICEVWDB.N_GENERIC_SVC_PRDT_ATTR WHERE BK_GSP_ATTR_NAME = 'SW SERVICE CATEGORY' ) WI ) --EXCLUDE SWSS
     ) WI
   WHERE NSOL.PRODUCT_KEY = WI.ITEM_KEY
      )
	  OR (NSOL.BK_SO_SRC_NAME IN ('Manual', 'Copy' ) AND NSOL.PRODUCT_KEY NOT IN ( SELECT P.ITEM_KEY FROM
     $$COMREFVWDB.N_PRODUCT P
     INNER JOIN 
     $$COMREFVWDB.MT_SVC_FINANCE_HIERARCHY SFH ON P.RU_BK_SERVICE_PROD_SUBGROUP_ID = SFH.BK_PRODUCT_SUBGROUP_ID
     AND SFH.BK_ALLOCATED_SERVC_GROUP_ID  IN ( 'AS FIXED', 'AS TRANSACTION', 'CLOUD MANAGED SERVICES' ) --EXCLUDE CMS IN SOWB
    )
                )
	  )
        ) NSOL
	ON NSOL.SALES_ORDER_LINE_KEY = NSLSADJ.RU_NEW_RENEW_SOL_KEY*/
   WHERE NSLSADJ.EDW_UPDATE_DTM >  '$$LastExtractDate'
       AND NSLSADJ.END_TV_DTM = '3500-01-01 00:00:00' 
	   
UNION ALL

SELECT
      NSLSADJ.MANUAL_TRX_KEY
     ,NSLSADJ.RU_NEW_RENEW_SOL_KEY AS SALES_ORDER_LINE_KEY
     --,NSLSADJ.RU_PRODUCT_KEY AS ENTERPRISE_OR_INVOICE_SKU      
	 ,NSLSADJ.ATTRIBUTED_PRODUCT_KEY AS ENTERPRISE_OR_INVOICE_SKU 	/*REBATE CHANGES*/ 	 
     ,CASE --WHEN NSLSADJ.RU_NEW_RENEW_SOL_KEY <0 THEN NSLSADJ.SALES_MOTION_CD /* Commented as part of Q3FY20 rel to simplify code */
    WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.SALES_MOTION_CD
        --ELSE  'UNKNOWN' /* Commented as part of Q3FY20 rel to simplify code */
		ELSE NSLSADJ.SALES_MOTION_CD /* Added as part of Q3FY20 rel to consider sls adj trx SM as default SM */
       END AS SALES_MOTION_CD 
     ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_ALLOCATION_PCT
        ELSE 1.000000 
       END AS ALLOCATION_PCT
     ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_SERVICE_CATEGORY_CD 
           ELSE 'TS' 
       END AS DV_SERVICE_CATEGORY_CD
     ,'N' AS OA_FLG
     ,CURRENT_TIMESTAMP(0) AS START_DATE 
     ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.END_TV_DTM
       -- ELSE CAST(NSLSADJ.END_TV_DTM AS DATE)  END AS END_DATE  
		ELSE CAST('3500-01-01 00:00:00' AS TIMESTAMP(0)) END AS END_DATE/*REBATE CHANGES*/	   
     ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.DV_SOURCE_TYPE /* DV SOURCE TYPE derivation change to accommodate SQ */
        ELSE CAST( 'NON-RTR-RB' AS VARCHAR(10))
        END AS SOURCE 
	 ,AR_TRX_LINE_KEY  /* For identifiying AR Rebates before loading into MT */
	 ,CASE WHEN SMC_ALLOC.SALES_ORDER_LINE_KEY > 0 THEN SMC_ALLOC.SALES_MOTION_TIMING_CD
        ELSE COALESCE(NSOL.SALES_MOTION_TIMING_CD, 'UNKNOWN' )
       END AS SALES_MOTION_TIMING_CD 
FROM (SELECT BK_RBT_ADJ_LINE_NUMBER_INT
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
		ORDER BY BK_RBT_ADJ_LINE_NUMBER_INT DESC)=1)NSLSADJ 
		    LEFT JOIN $$SLSORDVWDB.MT_RTNR_SMC_ALLOCATION SMC_ALLOC
        ON NSLSADJ.RU_NEW_RENEW_SOL_KEY = SMC_ALLOC.SALES_ORDER_LINE_KEY
        AND SMC_ALLOC.END_TV_DTM = '3500-01-01 00:00:00'
		LEFT JOIN $$STGDB.WI_TTS_SOWB_SOL_LINES NSOL
           ON NSLSADJ.RU_NEW_RENEW_SOL_KEY = NSOL.SALES_ORDER_LINE_KEY
    /*LEFT JOIN ( SELECT NSOL.* FROM $$SLSORDVWDB.N_SALES_ORDER_LINE_TV NSOL 
 ---/*   INNER JOIN $$SLSORDVWDB.N_SALES_ORDER NSO
    ---     ON NSO.SALES_ORDER_KEY = NSOL.SALES_ORDER_KEY  
       WHERE NSOL.SS_CODE = 'CG'
       AND NSOL.END_TV_DATETIME = '3500-01-01 00:00:00'
    AND (EXISTS (
   SELECT 1 FROM (SELECT 
      P.ITEM_KEY
      FROM
      $$COMREFVWDB.N_PRODUCT P
      INNER JOIN 
      $$COMREFVWDB.MT_SVC_FINANCE_HIERARCHY SFH ON P.RU_BK_SERVICE_PROD_SUBGROUP_ID = SFH.BK_PRODUCT_SUBGROUP_ID
      WHERE P.PRDT_SETUP_CLASSIFICATION_CD <> 'SOFTWARE' --EXCLUDE SW
      AND SFH.BK_SERVICE_CATEGORY_ID = 'TECHNICAL SUPPORT SERVICES' 
      AND SFH.BK_ALLOCATED_SERVC_GROUP_ID NOT IN
      ( 'AS CORE' ,  'AS SUBSCRIPTION' , 'FOCUSED TECHNICAL SUPPORT SERVICES' , 'CLOUD MANAGED SERVICES' )--EXCLUDE FTS,CMS,AS
      AND RU_BK_SERVICE_PROD_SUBGROUP_ID NOT IN (SELECT BK_PRDT_SUBGROUP_ID FROM ( SELECT * FROM $$SERVICEVWDB.N_GENERIC_SVC_PRDT_ATTR WHERE BK_GSP_ATTR_NAME = 'SW SERVICE CATEGORY' ) WI ) --EXCLUDE SWSS
      ) WI
    WHERE NSOL.PRODUCT_KEY = WI.ITEM_KEY
       )
    OR (NSOL.BK_SO_SRC_NAME IN ('Manual', 'Copy' ) AND NSOL.PRODUCT_KEY NOT IN ( SELECT P.ITEM_KEY FROM
      $$COMREFVWDB.N_PRODUCT P
      INNER JOIN 
      $$COMREFVWDB.MT_SVC_FINANCE_HIERARCHY SFH ON P.RU_BK_SERVICE_PROD_SUBGROUP_ID = SFH.BK_PRODUCT_SUBGROUP_ID
      AND SFH.BK_ALLOCATED_SERVC_GROUP_ID  IN ( 'AS FIXED', 'AS TRANSACTION', 'CLOUD MANAGED SERVICES' ) --EXCLUDE CMS IN SOWB
     )
                 )
    )
         ) NSOL
  ON NSOL.SALES_ORDER_LINE_KEY = NSLSADJ.RU_NEW_RENEW_SOL_KEY*/
    WHERE NSLSADJ.EDW_UPDATE_DATETIME >=(SELECT LAST_EXTRACT_DATE FROM $$ETLVWDB.CTL_ETL_LAST_EXTRACT_DATES  
       WHERE JOB_STREAM_ID='wf_MT_SLS_ADJ_SMC_ATTRIB_NRTR' AND TABLE_NAME='N_REBATE_ADJUSTMENT_LINE_NRT')
     --   AND NSLSADJ.END_TV_DTM = '3500-01-01 00:00:00' /REBATE CHANGES*/	   
	   
   ) RBK
  WHERE 
 NOT EXISTS ( SELECT 1 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_RTR WI WHERE WI.MANUAL_TRX_KEY = RBK.MANUAL_TRX_KEY )
 AND NOT EXISTS ( SELECT 1 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_PNVR WI WHERE WI.MANUAL_TRX_KEY = RBK.MANUAL_TRX_KEY )
 AND NOT EXISTS ( SELECT 1 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR WI WHERE WI.MANUAL_TRX_KEY = RBK.MANUAL_TRX_KEY )


Post SQL : 



Target2 Name : WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR2


Pre SQL : 



Post SQL : 
CALL COLLECT_STATS_WRAP('$$STGDB','WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR','D');


Source3 Name : SQ_WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR


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
	 --,SALES_MOTION_TIMING_CD
,CASE WHEN SALES_MOTION_CD = 'RENEW' THEN
      SALES_MOTION_TIMING_CD
ELSE 'UNKNOWN'
END AS SALES_MOTION_TIMING_CD
  FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR WI
  WHERE WI.AR_TRX_LINE_KEY < 0 /* Added condition to process AR rebates separately in next pipeline*/
    AND NOT EXISTS ( SELECT 1
                      FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT
         WHERE WI.MANUAL_TRX_KEY = MT.MANUAL_TRX_KEY
           AND WI.DV_ENTERPRISE_INV_SKU_ID = MT.DV_ENTERPRISE_INV_SKU_ID
		   
           AND WI.START_TV_DTM = MT.START_TV_DTM
        )


Post SQL : 



Target3 Name : MT_SLS_ADJ_SLS_MOTION_ATTRIB1


Pre SQL : 
/* START: SMR Aug 16th changes - To prevent unnecessary changes going into MT table */
	DELETE FROM $$STGDB.WI_ADJ_SMC_ATTRIB_NRTR_CHNG_CHK ;

	INSERT INTO $$STGDB.WI_ADJ_SMC_ATTRIB_NRTR_CHNG_CHK
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
	 FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR WI
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

	COLLECT STATS ON $$STGDB.WI_ADJ_SMC_ATTRIB_NRTR_CHNG_CHK;

	DELETE FROM $$STGDB.WI_ADJ_SMC_ATTRIB_NRTR_CHNG_CHK
	WHERE ( MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID ) IN
			( SEL MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID
				FROM $$STGDB.WI_ADJ_SMC_ATTRIB_NRTR_CHNG_CHK
				 WHERE AR_TRX_LINE_KEY < 0
				GROUP BY 1,2 HAVING ROUND(SUM(DV_ALLOCATION_PCT),4) <> 1.0000		
			) ;

	DELETE FROM $$STGDB.WI_ADJ_SMC_ATTRIB_NRTR_CHNG_CHK
	WHERE ( MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID, SALES_ORDER_LINE_KEY ) IN
			( SEL MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID, SALES_ORDER_LINE_KEY
				FROM $$STGDB.WI_ADJ_SMC_ATTRIB_NRTR_CHNG_CHK
				 WHERE AR_TRX_LINE_KEY > 0
				GROUP BY 1,2,3 HAVING ROUND(SUM(DV_ALLOCATION_PCT),4) <> 1.0000		
			) ;

	DELETE FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR WI
	WHERE EXISTS ( SEL 1 
					FROM $$STGDB.WI_ADJ_SMC_ATTRIB_NRTR_CHNG_CHK NO_CHNG
					WHERE NO_CHNG.AR_TRX_LINE_KEY < 0
					  AND WI.MANUAL_TRX_KEY = NO_CHNG.MANUAL_TRX_KEY
					  AND WI.DV_ENTERPRISE_INV_SKU_ID = NO_CHNG.DV_ENTERPRISE_INV_SKU_ID
				  ) ;
				  
	DELETE FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR WI
	WHERE EXISTS ( SEL 1 
					FROM $$STGDB.WI_ADJ_SMC_ATTRIB_NRTR_CHNG_CHK NO_CHNG
					WHERE NO_CHNG.AR_TRX_LINE_KEY > 0
					  AND WI.MANUAL_TRX_KEY = NO_CHNG.MANUAL_TRX_KEY
					  AND WI.DV_ENTERPRISE_INV_SKU_ID = NO_CHNG.DV_ENTERPRISE_INV_SKU_ID
					  AND WI.SALES_ORDER_LINE_KEY = NO_CHNG.SALES_ORDER_LINE_KEY
				  ) ;

/* END: SMR Aug 16th changes - To prevent unnecessary changes going into MT table */

UPDATE MT
    FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT , 
         ( SELECT MANUAL_TRX_KEY, DV_ENTERPRISE_INV_SKU_ID, MAX(START_TV_DTM) MAX_START_DTM
             FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR  
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


Source4 Name : SQ_MT_SLS_ADJ_SLS_MOTION_ATTRIB_NRTR_RB


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
    /*,SALES_MOTION_TIMING_CD*/
,CASE WHEN SALES_MOTION_CD = 'RENEW' THEN
      SALES_MOTION_TIMING_CD
ELSE 'UNKNOWN'
END AS SALES_MOTION_TIMING_CD
  FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR WI
  WHERE WI.AR_TRX_LINE_KEY > 0 /* Added condition to process AR rebates separately here */
  AND NOT EXISTS ( SELECT 1
                      FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT
         WHERE WI.MANUAL_TRX_KEY = MT.MANUAL_TRX_KEY
           AND WI.DV_ENTERPRISE_INV_SKU_ID = MT.DV_ENTERPRISE_INV_SKU_ID
		   AND WI.SALES_ORDER_LINE_KEY = MT.SALES_ORDER_LINE_KEY /* Added condition to process AR rebates separately here */
           AND WI.START_TV_DTM = MT.START_TV_DTM
        )


Post SQL : 



Target4 Name : MT_SLS_ADJ_SLS_MOTION_ATTRIB3


Pre SQL : 
UPDATE MT
    FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT , 
         ( SELECT MANUAL_TRX_KEY, SALES_ORDER_LINE_KEY, DV_ENTERPRISE_INV_SKU_ID, MAX(START_TV_DTM) MAX_START_DTM
             FROM $$STGDB.WI_MT_SLS_ADJ_SMC_ATTRIB_NRTR  
			 WHERE AR_TRX_LINE_KEY > 0 /* Added condition to process AR rebates separately here */
               GROUP BY 1,2,3
                     ) WI
    SET END_TV_DTM = MAX_START_DTM - INTERVAL '1' SECOND ,
        EDW_UPDATE_DTM = CURRENT_TIMESTAMP(0)
  WHERE MT.MANUAL_TRX_KEY = WI.MANUAL_TRX_KEY
  AND MT.SALES_ORDER_LINE_KEY = WI.SALES_ORDER_LINE_KEY /* Added condition to process AR rebates separately here */
  AND MT.DV_ENTERPRISE_INV_SKU_ID = WI.DV_ENTERPRISE_INV_SKU_ID
  AND MT.START_TV_DTM <> WI.MAX_START_DTM 
  AND MT.END_TV_DTM = '3500-01-01 00:00:00';


Post SQL : 
/*UPDATE MT FROM
$$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB  MT
,$$STGDB.SMT_RETRO_UPD SMTR
SET SALES_MOTION_TIMING_CD = SMTR.SALES_MOTION_TIMING_CD,
    EDW_UPDATE_USER='CISCO_BOOKED_DATE_SMT_UPD' 
WHERE MT.SALES_ORDER_LINE_KEY = SMTR.SALES_ORDER_LINE_KEY
AND MT.SALES_MOTION_CD = 'RENEW'
AND MT.SALES_MOTION_TIMING_CD='UNKNOWN'
AND MT.END_TV_DT= '3500-01-01'
AND COALESCE( MT.SALES_MOTION_TIMING_CD, 'UNKNOWN')  <> COALESCE(SMTR.SALES_MOTION_TIMING_CD, 'UNKNOWN') ;
*/
/* Commented as SMT updates are taken care in wf_EL_SOL_SMT_SMC ETL as part of Aug 16th release */
/* UPDATE MT_ADJ
FROM $$SLSORDVWDB.MT_SLS_ADJ_SLS_MOTION_ATTRIB MT_ADJ,
 (SEL * FROM $$SLSORDVWDB.N_ATTR_PRDT_AS_NEW_OR_RNWL_TV O WHERE END_TV_DTM = '3500-01-01 00:00:00'
AND SALES_MOTION_CD = 'RENEW'
AND EXISTS (SEL 1 FROM $$SLSORDVWDB.N_ATTR_PRDT_AS_NEW_OR_RNWL_TV I WHERE I.START_TV_DTM > CURRENT_DATE-2 
     AND I.END_TV_DTM = '3500-01-01 00:00:00' 
	 AND I.SALES_MOTION_CD = 'RENEW'
	 AND I.SALES_ORDER_LINE_KEY = O.SALES_ORDER_LINE_KEY)
QUALIFY ROW_NUMBER()  OVER  (PARTITION BY  SALES_ORDER_LINE_KEY ORDER BY 
  ( CASE  WHEN  SALES_MOTION_TIMING_CD = 'ONTIME' THEN 6  WHEN SALES_MOTION_TIMING_CD= 'LATE IN QUARTER'  THEN 5  
  WHEN  SALES_MOTION_TIMING_CD = 'EARLY'  THEN 4 WHEN SALES_MOTION_TIMING_CD = 'LATE' THEN 3 
  WHEN SALES_MOTION_TIMING_CD= 'RECAPTURE'  THEN  2 WHEN SALES_MOTION_TIMING_CD= 'UNKNOWN'  THEN  1 END )
  DESC)= 1) NATTR
 SET SALES_MOTION_TIMING_CD = NATTR.SALES_MOTION_TIMING_CD
  WHERE MT_ADJ.SALES_ORDER_LINE_KEY = NATTR.SALES_ORDER_LINE_KEY
AND MT_ADJ.SALES_MOTION_CD = NATTR.SALES_MOTION_CD
 AND NATTR.SALES_MOTION_CD = 'RENEW'
AND MT_ADJ.END_TV_DTM = '3500-01-01 00:00:00'
AND COALESCE(MT_ADJ.SALES_MOTION_TIMING_CD, 'UNKNOWN') <> COALESCE(NATTR.SALES_MOTION_TIMING_CD, 'UNKNOWN');
*/

CALL COLLECT_STATS_WRAP('$$SLSORDDB','MT_SLS_ADJ_SLS_MOTION_ATTRIB','D');


UPDATE CTL 
FROM $$ETLVWDB.CTL_ETL_LAST_EXTRACT_DATES CTL,
      ( SELECT MAX(EDW_UPDATE_DATETIME) MAX_UPDATE_DTM 
      FROM $$NRTNCRVWDB.N_REBATE_ADJUSTMENT_LINE_NRT
      ) WI  
SET LAST_EXTRACT_DATE = COALESCE(WI.MAX_UPDATE_DTM,CTL.LAST_EXTRACT_DATE)  
WHERE JOB_STREAM_ID = 'wf_MT_SLS_ADJ_SMC_ATTRIB_NRTR' 
AND TABLE_NAME = 'N_REBATE_ADJUSTMENT_LINE_NRT';