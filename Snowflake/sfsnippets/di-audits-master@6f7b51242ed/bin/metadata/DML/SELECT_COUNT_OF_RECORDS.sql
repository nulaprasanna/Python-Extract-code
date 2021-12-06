-- Select a count of a table's records
-- TODO: In some systems the exact record count can be referenced and not aggregated. NR!
SELECT
    COUNT(1)
FROM
    $$DB_SCHEMA_NAME.$$TABLE_NAME