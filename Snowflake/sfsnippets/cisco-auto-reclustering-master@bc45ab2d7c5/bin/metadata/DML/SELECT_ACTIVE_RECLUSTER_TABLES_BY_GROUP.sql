-- Collect the tables which are registered for clustering in a particular group. They must be active.
SELECT
      ID
    , ACCOUNT
    , DB_NAME
    , SCHEMA_NAME
    , TABLE_NAME
    , RUN_SCHEDULE
    , DRIVING_MECH
    , TCPC
    , AVG_DEPTH
    , OTHER_DRIVER
    , MAX_RUNS
    , SIZE_OVERRIDE
    , WAREHOUSE_OVERRIDE
    , ACTIVE_FLAG
    , GROUP_NAME
    , PASSPHRASE
FROM CISCO_AUTO_RECLUSTER
WHERE
    ACTIVE_FLAG = 'YES'
        AND
    GROUP_NAME = '$$GROUP_NAME'