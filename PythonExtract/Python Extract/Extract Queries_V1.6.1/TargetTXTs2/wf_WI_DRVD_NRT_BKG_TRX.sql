


Source1 Name : SQ_MT_DRVD_NCR_BKG_TRX1


Pre SQL : 
DELETE  FROM $$NRTSTGDB.MT_DRVD_NCR_BKG_TRX_INCR;


SQL Query : 
SELECT  
 MDNBT.DRVD_NCR_BKG_TRX_KEY        BOOKINGS_MEASURE_KEY,  
 MDNBT.SALES_REP_NUMBER        SALES_REP_NUMBER,    
 MDNBT.TRANSACTION_DATETIME       TRANSACTION_DATETIME,         
 MDNBT.TRANSACTION_SEQUENCE_ID_INT     TRANSACTION_SEQUENCE_ID_INT,
 COALESCE(MDNBT.SALES_CHANNEL_CODE ,'UNKNOWN')    SALES_CHANNEL_CODE,
 MDNBT.CDB_DATA_SOURCE_CODE       CDB_DATA_SOURCE_CODE,
 COALESCE(MDNBT.SPLIT_PERCENTAGE,0)                  SPLIT_PERCENTAGE,
 MDNBT.SOLD_TO_CUSTOMER_KEY                       SOLD_TO_CUSTOMER_KEY,
 MDNBT.BILL_TO_CUSTOMER_KEY                BILL_TO_CUSTOMER_KEY,          
 MDNBT.SHIP_TO_CUSTOMER_KEY                SHIP_TO_CUSTOMER_KEY,         
 MDNBT.SALES_ORDER_LINE_KEY                          SALES_ORDER_LINE_KEY, 
 MDNBT.FORWARD_REVERSE_CODE                          FORWARD_REVERSE_CODE,
 COALESCE(MDNBT.BOOKINGS_PERCENTAGE,100)             BOOKINGS_PERCENTAGE,
 COALESCE(MDNBT.NET_PRICE_AMOUNT,0)                  NET_PRICE_AMOUNT,
 COALESCE(MDNBT.TRANSACTION_QUANTITY,0)              TRANSACTION_QUANTITY,
 MDNBT.BK_ISO_CURRENCY_CODE                          BK_ISO_CURRENCY_CODE,         
 MDNBT.SALES_ORDER_KEY                               SALES_ORDER_KEY,   
 MDNBT.ES_LINE_SEQ_ID_INT        ES_LINE_SEQ_ID_INT ,
 MDNBT.SOURCE_SYSTEM_CODE                            SOURCE_SYSTEM_CODE, 
 MDNBT.PRODUCT_KEY                                   PRODUCT_KEY,
 MDNBT.SALES_TERRITORY_KEY                           SALES_TERRITORY_KEY,
 MDNBT.SALES_CREDIT_TYPE_CODE                        SALES_CREDIT_TYPE_CODE,
 MDNBT.EDW_UPDATE_DTM          EDW_UPDATE_DTM,
 MDNBT.DV_ATTRIBUTION_CD                                DV_ATTRIBUTION_CD  ,             
 MDNBT.SK_OFFER_ATTRIBUTION_ID_INT                   SK_OFFER_ATTRIBUTION_ID_INT,
 MDNBT.DV_SALES_ORDER_LINE_KEY                       DV_SALES_ORDER_LINE_KEY,
 MDNBT.DV_PRODUCT_KEY                                DV_PRODUCT_KEY,
  /*Added the columns as part of Q1FY15 - OFFER ATTRIBUTION */
  MDNBT.ATTRIBUTION_PCT                               ATTRIBUTION_PCT ,
  MDNBT.PROCESS_DATE                                  PROCESS_DATE,
  CASE WHEN DV_ATTRIBUTION_CD IN ('STANDALONE' , 'BUNDLE', 'BIB','BIB OFFSET') THEN TRANSACTION_QUANTITY /* ADDED ATTRIBUTION BIB AS PART OF NRS MPOB CHANGES*/
  ELSE CAST(-1 AS DECIMAL(18,6)) END   AS BUNDLE_TRANSACTION_QUANTITY
  ,MDNBT.SALES_MOTION_CD
  ,MDNBT.POB_TYPE_CD /* Added as part of NRS MPOB Changes*/
  ,MDNBT.NRS_TRANSITION_FLG /* Added as part of NRS PI3 Changes*/
  ,MDNBT.BOOKINGS_POLICY_CD
  ,MDNBT.SOURCE_REP_ANNUAL_TRXL_AMT
 FROM
 $$NRTNCRVWDB.MT_DRVD_NCR_BKG_TRX MDNBT 
 WHERE
 MDNBT.EDW_UPDATE_DTM > (SELECT LAST_EXTRACT_DATE FROM $$ETLVWDB.DW_JOB_STREAMS_NRT WHERE JOB_STREAM_ID = 'wf_WI_DRVD_NRT_BKG_TRX')


Post SQL : 



Target1 Name : MT_DRVD_NCR_BKG_TRX_INCR


Pre SQL : 



Post SQL : 
/*COLLECT STATS ON $$NRTSTGDB.MT_DRVD_NCR_BKG_TRX_INCR;*/

CALL COLLECT_STATS_WRAP('$$NRTSTGDB','MT_DRVD_NCR_BKG_TRX_INCR','D');

/* Added Post SQL to update the Bundle Quantity for Attributed Records - Q1FY15 - OA */



UPDATE INCR
FROM $$NRTSTGDB.MT_DRVD_NCR_BKG_TRX_INCR INCR, 
(
SELECT 
   DRVD_NCR_BKG_TRX_KEY AS BOOKINGS_MEASURE_KEY,
	DV_SALES_ORDER_LINE_KEY, 
	TRANSACTION_QUANTITY,
	FORWARD_REVERSE_CODE,
	PROCESS_DATE,
	SALES_REP_NUMBER,
	ES_LINE_SEQ_ID_INT
FROM $$NRTNCRVWDB.MT_DRVD_NCR_BKG_TRX TRX
WHERE 1=1
AND (TRX.DV_ATTRIBUTION_CD = 'BUNDLE' )
AND (DV_SALES_ORDER_LINE_KEY , PROCESS_DATE) IN 
(SELECT DV_SALES_ORDER_LINE_KEY , PROCESS_DATE
FROM $$NRTSTGDB.MT_DRVD_NCR_BKG_TRX_INCR GROUP BY 1,2)
)DRVD
SET BUNDLE_TRANSACTION_QUANTITY = (CASE WHEN INCR.DV_ATTRIBUTION_CD in ('OFFSET','BIB OFFSET') THEN (DRVD.TRANSACTION_QUANTITY * -1)
ELSE DRVD.TRANSACTION_QUANTITY END)
WHERE INCR.DV_SALES_ORDER_LINE_KEY = DRVD.DV_SALES_ORDER_LINE_KEY
AND INCR.FORWARD_REVERSE_CODE = DRVD.FORWARD_REVERSE_CODE
AND INCR.PROCESS_DATE = DRVD.PROCESS_DATE
AND INCR.SALES_REP_NUMBER = DRVD.SALES_REP_NUMBER
AND INCR.ES_LINE_SEQ_ID_INT = DRVD.ES_LINE_SEQ_ID_INT
AND INCR.DV_ATTRIBUTION_CD IN('ATTRIBUTED','OFFSET','BIB','BIB OFFSET');


Source2 Name : SQ_MT_DRVD_NCR_BKG_TRX


Pre SQL : 
DELETE  FROM $$NRTSTGDB.WI_DRVD_NRT_BKG_TRX;


SQL Query : 
SELECT 
     BOOKINGS_MEASURE_KEY
    , CONVERSION_DT
    , SALES_REP_NUMBER
    , TRANSACTION_DATETIME
    , TRANSACTION_SEQUENCE_ID_INT
    , SALES_CHANNEL_CODE
    , CDB_DATA_SOURCE_CODE
    , SPLIT_PERCENTAGE
    , DD_SERVICE_FLAG
    , SOLD_TO_CUSTOMER_KEY
    , BILL_TO_CUSTOMER_KEY
    , SHIP_TO_CUSTOMER_KEY
    , SALES_ORDER_LINE_KEY
    , BK_SO_NUMBER_INT
    , PURCHASE_ORDER_NUMBER
    , BK_SO_SRC_CRT_DATETIME
    , FORWARD_REVERSE_CODE
    , BOOKINGS_PERCENTAGE
    , NET_PRICE_AMOUNT
    , TRANSACTION_QUANTITY
    , BK_ISO_CURRENCY_CODE
    , SALES_ORDER_KEY
    , DD_ITEM_TYPE_CODE_FLAG
    , DD_RMA_FLAG
    , DD_INTERNATIONAL_DEMO_FLAG
    , DD_REPLACEMENT_DEMO_FLAG
    , DD_REVENUE_FLAG
    , DD_OVERLAY_FLAG
    , DD_SALESREP_FLAG
    , DD_IC_REVENUE_FLAG
    , DD_CHARGES_FLAG
    , DD_MISC_FLAG
    , DD_ACQUISITION_FLAG
    , ES_LINE_SEQ_ID_INT
    , CANCELLED_FLG
    , RU_CISCO_BOOKED_DATETIME
    , SALES_ORDER_CATEGORY_TYPE
    , SOURCE_SYSTEM_CODE
    , SALES_ORDER_OPERATING_UNIT
    , PRODUCT_KEY
    , SALES_TERRITORY_KEY
    , SALES_CREDIT_TYPE_CODE
    , CUSTOMER_PO_TYPE_CD
    , SHIPMENT_PRIORITY_CODE
    , BK_SO_SOURCE_NAME
    , PRICING_DATE
    , LINE_CREATION_DATE
    , UNIT_LIST_PRICE
    , SALES_ORDER_TYPE_NAME
    , EDW_CREATE_USER
    , EDW_UPDATE_USER
    , EDW_CREATE_DATETIME
    , EDW_UPDATE_DATETIME
    , ORDER_DTM
    , DV_ATTRIBUTION_CD
    , SK_OFFER_ATTRIBUTION_ID_INT
    , DV_SALES_ORDER_LINE_KEY
    , DV_PRODUCT_KEY
    , ATTRIBUTED_UNIT_NET_PRICE
    , BUNDLE_TRANSACTION_QUANTITY
    , ATTRIBUTION_PCT /*Added the column as part of Q1FY15 - OFFER ATTRIBUTION */
 , SALES_MOTION_CD
 ,POB_TYPE_CD /* Added as part of NRS MPOB Changes*/
 ,NRS_TRANSITION_FLG /* Added as part of NRS PI3 Changes*/
/* Added as part of June 2018 release AMY-NRT Scope*/
 ,RU_SERVICE_CONTRACT_START_DTM
 ,RU_SERVICE_CONTRACT_END_DTM
 ,RU_SRVC_CNTRCT_DRTN_MNTHS_CNT
 ,BOOKINGS_POLICY_CD
 ,SOURCE_REP_ANNUAL_TRXL_AMT 

   FROM
   (
   
   SELECT  
    MDNBT.DRVD_NCR_BKG_TRX_KEY        BOOKINGS_MEASURE_KEY,  
    CASE
    WHEN   NSOT.ORDER_DTM IS NULL 
    THEN CAST(NSOT.SO_SOURCE_CREATE_DTM AS DATE)
    ELSE       CAST(NSOT.ORDER_DTM AS DATE)
    END             CONVERSION_DT,
    MDNBT.SALES_REP_NUMBER        SALES_REP_NUMBER,    
    MDNBT.TRANSACTION_DATETIME       TRANSACTION_DATETIME,         
    MDNBT.TRANSACTION_SEQUENCE_ID_INT     TRANSACTION_SEQUENCE_ID_INT,
    COALESCE(MDNBT.SALES_CHANNEL_CODE ,'UNKNOWN')    SALES_CHANNEL_CODE,
    MDNBT.CDB_DATA_SOURCE_CODE       CDB_DATA_SOURCE_CODE,
    COALESCE(MDNBT.SPLIT_PERCENTAGE,0)                  SPLIT_PERCENTAGE,
    CASE
    WHEN   NP.GOODS_OR_SERVICE_TYPE ='SERVICE' 
    THEN 'Y'
    ELSE       'N'
    END             DD_SERVICE_FLAG, 
    MDNBT.SOLD_TO_CUSTOMER_KEY                       SOLD_TO_CUSTOMER_KEY,
    MDNBT.BILL_TO_CUSTOMER_KEY                BILL_TO_CUSTOMER_KEY,          
    MDNBT.SHIP_TO_CUSTOMER_KEY                SHIP_TO_CUSTOMER_KEY,         
    MDNBT.SALES_ORDER_LINE_KEY                          SALES_ORDER_LINE_KEY, 
    NSOT.BK_SALES_ORDER_NUM_INT                         BK_SO_NUMBER_INT,
    NSOT.CUSTOMER_PURCHASE_ORDER_NUM      PURCHASE_ORDER_NUMBER,
    NSOT.SO_SOURCE_CREATE_DTM        BK_SO_SRC_CRT_DATETIME,
    MDNBT.FORWARD_REVERSE_CODE                          FORWARD_REVERSE_CODE,
    COALESCE(MDNBT.BOOKINGS_PERCENTAGE,100)             BOOKINGS_PERCENTAGE,
    COALESCE(MDNBT.NET_PRICE_AMOUNT,0)                  NET_PRICE_AMOUNT,
    COALESCE(MDNBT.TRANSACTION_QUANTITY,0)              TRANSACTION_QUANTITY,
    MDNBT.BK_ISO_CURRENCY_CODE                          BK_ISO_CURRENCY_CODE,         
    MDNBT.SALES_ORDER_KEY                               SALES_ORDER_KEY,   
    'N'             DD_ITEM_TYPE_CODE_FLAG,
    CASE
    WHEN   NSOL.SALES_ORDER_LINE_CTGRY_TYPE_CD  ='RETURN'
    THEN 'Y'
    ELSE       'N'
    END                                          DD_RMA_FLAG,
    CASE 
     WHEN CDB_DATA_SOURCE_CODE IN ('UO','BO')  THEN       
      CASE 
       WHEN SUBSTR(NSOT.SALES_ORDER_TYPE_NAME,1,8) ='Standard' THEN
        CASE 
         WHEN   NSOT.SHIPMENT_PRIORITY_CD='Demo' THEN 'Y'
         WHEN   NSOT.SHIPMENT_PRIORITY_CD= 'Evaluation' THEN 'Y' 
         WHEN   NSOT.SHIPMENT_PRIORITY_CD= 'Int''l Demo' THEN 'Y'
         WHEN   NSOT.SHIPMENT_PRIORITY_CD='Intl Demo' THEN 'Y'
        ELSE       'N'
        END
      ELSE       'N'
      END
    ELSE
      '=' 
    END                                                 DD_INTERNATIONAL_DEMO_FLAG,
    
    CASE 
     WHEN CDB_DATA_SOURCE_CODE IN ('UO','BO')  THEN 
      CASE
       WHEN SUBSTR(NSOT.SALES_ORDER_TYPE_NAME,1,11) ='Replacement' THEN
        CASE
         WHEN   NSOT.SHIPMENT_PRIORITY_CD='Demo' THEN 'Y'
         WHEN   NSOT.SHIPMENT_PRIORITY_CD= 'Evaluation' THEN 'Y' 
         WHEN   NSOT.SHIPMENT_PRIORITY_CD= 'Int''l Demo' THEN 'Y'
         WHEN   NSOT.SHIPMENT_PRIORITY_CD='Intl Demo' THEN 'Y'
         ELSE       'N'
         END
      ELSE       'N'
      END 
    ELSE
      '=' 
    END              DD_REPLACEMENT_DEMO_FLAG,
       
    CASE
        WHEN   NSOTT.SO_TYPE_REVENUE_GEN_FLAG IS NULL 
                    THEN 'N' 
                    ELSE       NSOTT.SO_TYPE_REVENUE_GEN_FLAG
    END                            DD_REVENUE_FLAG,
    CASE
    WHEN   NST.BK_SALES_HIERARCHY_TYPE_CODE  IS NULL 
    THEN 'Y'
    ELSE       'N'
    END                                                 DD_OVERLAY_FLAG,
    CASE
    WHEN   MDNBT.SALES_REP_NUMBER IS NULL          
    THEN 'Y'
    ELSE       'N'
    END                                                 DD_SALESREP_FLAG,
    CASE
    WHEN   NCAT_IC.INTERNAL_CUSTOMER_FLAG= 'I' 
    THEN 'N'
    WHEN   NCAT_IC.INTERNAL_CUSTOMER_FLAG IS NULL 
    THEN 'N'
    ELSE       'Y'
    END                  DD_IC_REVENUE_FLAG,
    CASE
    WHEN   NP.BK_PRODUCT_TYPE_ID= 'CHARGES' 
    THEN 'Y'
    ELSE       'N'
    END                                                 DD_CHARGES_FLAG,  
    CASE
    WHEN   SUBSTR(NP.BK_PRODUCT_ID,1,4)= 'MISC' 
    THEN 'Y'
    ELSE       'N'
    END                                                 DD_MISC_FLAG,
    CASE
    WHEN   NSOT.SALES_ORDER_SOURCE_TYPE_CD ='EC ACQUISITION' 
    THEN 'Y'
    WHEN   NSOT.SALES_ORDER_SOURCE_TYPE_CD = 'ACQUISITION CONVERSION' 
    THEN 'Y'
    WHEN   NSOT.SALES_ORDER_SOURCE_TYPE_CD ='BACKLOG CONVERSION' 
    THEN 'Y'
    ELSE       'N'
    END                                          DD_ACQUISITION_FLAG,
    MDNBT.ES_LINE_SEQ_ID_INT        ES_LINE_SEQ_ID_INT ,
    COALESCE(NSOT.CANCELLED_FLG,'N')                    CANCELLED_FLG,
    NSOT.RU_CISCO_BOOKED_DTM          RU_CISCO_BOOKED_DATETIME,
    NSOL.SALES_ORDER_LINE_CTGRY_TYPE_CD       SALES_ORDER_CATEGORY_TYPE, /* modified from nost to nsol */
    MDNBT.SOURCE_SYSTEM_CODE                            SOURCE_SYSTEM_CODE, 
    NSOT.SO_OPERATING_UNIT_NAME_CD                       SALES_ORDER_OPERATING_UNIT,
    MDNBT.PRODUCT_KEY                                   PRODUCT_KEY,
    MDNBT.SALES_TERRITORY_KEY                           SALES_TERRITORY_KEY,
    MDNBT.SALES_CREDIT_TYPE_CODE                        SALES_CREDIT_TYPE_CODE,
    NSOT.CUSTOMER_PO_TYPE_CD                            CUSTOMER_PO_TYPE_CD   ,
    NSOT.SHIPMENT_PRIORITY_CD                           SHIPMENT_PRIORITY_CODE,
    NSOT.SALES_ORDER_SOURCE_TYPE_CD                     BK_SO_SOURCE_NAME,   
    NSOL.DV_PRICING_DT                                  PRICING_DATE,
    NSOL.SO_LINE_SOURCE_CREATE_DTM                      LINE_CREATION_DATE,
    /*added precision for attribution pct - Q1FY15 - OA*/
    CASE WHEN MDNBT.SK_OFFER_ATTRIBUTION_ID_INT IS  NOT  NULL  
    THEN((COALESCE(NSOL.UNIT_LIST_PRICE_LOCAL_AMT,0)  * COALESCE(MDNBT.ATTRIBUTION_PCT,0))/100.00000000) 
    ELSE NSOL.UNIT_LIST_PRICE_LOCAL_AMT
    END UNIT_LIST_PRICE,
    NSOL.SO_LINE_TYPE_NAME                              SALES_ORDER_TYPE_NAME,
    USER                                                EDW_CREATE_USER,                                      
    USER                                                EDW_UPDATE_USER,                                    
    CURRENT_TIMESTAMP(0)                                EDW_CREATE_DATETIME,                           
    CURRENT_TIMESTAMP(0)                                EDW_UPDATE_DATETIME,
    NSOT.ORDER_DTM                                      ORDER_DTM,
    MDNBT.DV_ATTRIBUTION_CD                                DV_ATTRIBUTION_CD  ,             
    MDNBT.SK_OFFER_ATTRIBUTION_ID_INT                   SK_OFFER_ATTRIBUTION_ID_INT,
    MDNBT.DV_SALES_ORDER_LINE_KEY                       DV_SALES_ORDER_LINE_KEY,
    MDNBT.DV_PRODUCT_KEY                                DV_PRODUCT_KEY,
  
      0.0 AS ATTRIBUTED_UNIT_NET_PRICE, /* not being used Q1FY15 OA */ 
  
    MDNBT.BUNDLE_TRANSACTION_QUANTITY AS BUNDLE_TRANSACTION_QUANTITY,  
    
   MDNBT.ATTRIBUTION_PCT  ATTRIBUTION_PCT /*Added the column as part of Q1FY15 - OFFER ATTRIBUTION */
   , MDNBT.SALES_MOTION_CD
    ,MDNBT.POB_TYPE_CD /* Added as part of NRS MPOB Changes*/
 ,MDNBT.NRS_TRANSITION_FLG /* Added as part of NRS PI3 Changes*/
 /* Take these details from NSOL and make sure the column names are correct */ --- Soundar
  /* Added as part of June 2018 release AMY-NRT Scope*/
 ,NSOL.RU_SERVICE_CONTRACT_START_DTM
 ,NSOL.RU_SERVICE_CONTRACT_END_DTM
 ,NSOL.RU_SRVC_CNTRCT_DRTN_MNTHS_CNT 
,MDNBT.BOOKINGS_POLICY_CD 
 ,MDNBT.SOURCE_REP_ANNUAL_TRXL_AMT
    FROM
    $$NRTSTGDB.MT_DRVD_NCR_BKG_TRX_INCR MDNBT  
 LEFT OUTER JOIN $$COMREFVWDB.N_SALES_TERRITORY NST
    ON (MDNBT.SALES_TERRITORY_KEY = NST.SALES_TERRITORY_KEY AND NST.BK_SALES_HIERARCHY_TYPE_CODE  lIKE 'CORP%.%REVENUE') 
 
    LEFT OUTER JOIN ( 
        $$COMREFVWDB.N_CUSTOMER_ACCOUNT NCAT_IC
        LEFT OUTER JOIN  $$COMREFVWDB.N_ERP_PARTY NPT
                     ON( NCAT_IC.ERP_CUSTOMER_NUMBER= NPT.ERP_PARTY_NUMBER AND NPT.ERP_PARTY_NAME NOT LIKE '%REGRESSION%TEST%' )
        )                
        ON MDNBT.SOLD_TO_CUSTOMER_KEY=NCAT_IC.CUSTOMER_ACCOUNT_KEY                     
    LEFT OUTER JOIN $$NRTNCRVWDB.N_SALES_ORDER_LINE_NRT NSOL
                    ON(MDNBT.DV_SALES_ORDER_LINE_KEY= NSOL.SALES_ORDER_LINE_KEY )
     LEFT OUTER JOIN $$NRTNCRVWDB.N_SALES_ORDER_NRT NSOT
                    ON( MDNBT.SALES_ORDER_KEY=NSOT.SALES_ORDER_KEY)
    LEFT OUTER JOIN $$SLSORDVWDB.N_SALES_ORDER_TYPE  NSOTT                  
        /*ON( NSOT.SALES_ORDER_TYPE_NAME=NSOTT.BK_ORDER_TYPE_NAME)*/
     ON( NSOL.SO_LINE_TYPE_NAME=NSOTT.BK_ORDER_TYPE_NAME) 
    INNER JOIN  $$COMREFVWDB.N_PRODUCT NP
                    ON(MDNBT.PRODUCT_KEY=NP.ITEM_KEY) 
    INNER JOIN $$COMREFVWDB.N_ITEM_CLASS  FND
    ON NP.BK_ITEM_CLASS_CODE = FND.BK_ITEM_CLASS_CODE
    AND FND.ITEM_CLASS_NAME  <> 'Option class'  
  /*WHERE COALESCE(NSOL.BOOKINGS_SOURCE_CD,'UNK') <> 'SBP' /*Q4FY15 XAAS*/   
  WHERE COALESCE(NSOL.BOOKINGS_SOURCE_CD,'UNK') NOT IN ('SBP', 'TSL')  /*Q4FY16 XAAS  */    
   )DRVD


Post SQL : 



Target2 Name : WI_DRVD_NRT_BKG_TRX


Pre SQL : 



Post SQL : 
UPDATE $$ETLVWDB.DW_JOB_STREAMS_NRT 
SET LAST_EXTRACT_DATE=(SELECT MAX(EDW_UPDATE_DTM) FROM $$NRTNCRVWDB.MT_DRVD_NCR_BKG_TRX)
WHERE JOB_STREAM_ID='wf_WI_DRVD_NRT_BKG_TRX';

/* COLLECT STATS ON $$NRTSTGDB.WI_DRVD_NRT_BKG_TRX; */

CALL COLLECT_STATS_WRAP('$$NRTSTGDB','WI_DRVD_NRT_BKG_TRX','D');