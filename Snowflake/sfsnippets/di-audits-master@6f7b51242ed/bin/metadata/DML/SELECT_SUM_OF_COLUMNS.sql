-- For a provided list of columns this query will calculate their sum aggregates.
-- The expected input is such as "sum(column_a), sum(column_b)"
SELECT
    $$SUM_COLUMNS
FROM
    $$DB_SCHEMA_NAME.$$TABLE_NAME