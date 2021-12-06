-- Collect all tables for the reconciliation list
select
      'CISCO.US-EAST-1' as ACCOUNT
    , TABLE_CATALOG
    , TABLE_SCHEMA
    , TABLE_NAME
    , AUTO_CLUSTERING_ON
FROM "SNOWFLAKE"."ACCOUNT_USAGE"."TABLES"
WHERE
    TABLE_CATALOG like 'EDW_%'
        AND
    CLUSTERING_KEY IS NOT NULL
        AND
    DELETED IS NULL
        AND
    TABLE_SCHEMA = 'BR'
