-- TODO: Reformat for prettiness
-- Data required for the model build
SELECT /*+ parallel (dh,10) use_nl(dh,dl)*/
      DISTINCT
      dl.function_area,
      REPLACE (dl.domain_name, '-', 'EDW')                       domain_name,
      dl.object_db_name ,
      dl.schema ,
      dl.obj_biz_name obj_name,
      dl.role                                                    target_role,
      NVL (
         (SELECT CASE
                    WHEN classification = 'Highly Confidential' THEN 2
                    WHEN classification = 'Confidential' THEN 1
                    WHEN classification = 'Restricted' THEN 3
                    WHEN classification = 'nan' THEN 0
                    ELSE 0
                 END
            FROM ODSHDPIN.SF_DATA_CLASSIFICATION DC
           WHERE 1 = 1
                      AND DL.DATA_OBJECT = DC.TABLE_NAME AND ROWNUM = 1),
         0)
         IS_RESTRICTED,
      CASE WHEN DL.ROLE LIKE '%SANDBOX%' THEN 0 ELSE 1 END
         Actual_Sandbox,
      CASE
         WHEN (SELECT COUNT (*)
                 FROM ODSHDPIN.SF_SUPV_HIERARCHY_CUR x
                WHERE     x.level3_cisco_email_address = 'shasunda'
                      AND x.cisco_email_address = dl.created_by) > 0
         THEN
            1
         ELSE
            0
      END
         requestor_org, -- DAO/Non-DAO
      CASE
         WHEN (SELECT COUNT (*)
                 FROM EDWDS.DS_REQUEST_LINE X
                WHERE     x.role = DL.ROLE
                      AND x.DOMAIN_NAME = DL.DOMAIN_NAME
                      AND X.LINE_STATUS IN ('REJECTED', 'GRANTED')
                      AND (   dl.object_db_name
                           || '.'
                           || dl.schema
                           || '.'
                           || dl.obj_biz_name) !=
                             (   x.object_db_name
                              || '.'
                              || x.schema
                              || '.'
                              || x.obj_biz_name)) = 0
         THEN
            0
         ELSE
            1
      END
         has_access_to_domain,
      CASE
         WHEN dl.LINE_STATUS IN ('GRANTED', 'APPROVED') THEN 1
         WHEN dl.LINE_STATUS IN ('EXPIRED') THEN -1
         ELSE 0
      END
         REQUEST_STATUS
 FROM EDWDS.DS_REQUEST_LINE          DL,
      EDWDS.DS_REQUEST_HEADER        DH,
      ODSHDPIN.SF_DATA_CLASSIFICATION DC
WHERE     1 = 1
      AND UPPER (DH.REQUEST_TYPE) = 'SNOWFLAKE'
      AND DH.REQUEST_ID = DL.REQUEST_ID
      AND DL.DATA_OBJECT IS NOT NULL
      AND DL.OBJECT_DB_NAME IS NOT NULL
      AND DL.SCHEMA IS NOT NULL
      AND DL.DS_APPROVAL_STATUS IN ('REJECTED', 'GRANTED', 'APPROVED')
      AND DL.OBJECT_DB_NAME LIKE 'EDW%'
      AND DL.DATA_OBJECT NOT LIKE 'PV%'
      AND DL.ROLE NOT IN
             ('EDW_OPS_PLT_ETL_ROLE',
              'SERVICES_AS_ETL_ROLE',
              'DSX_DLJCOE_BUS_ANALYST_ROLE')
      AND CATEGORY = 'dev'
