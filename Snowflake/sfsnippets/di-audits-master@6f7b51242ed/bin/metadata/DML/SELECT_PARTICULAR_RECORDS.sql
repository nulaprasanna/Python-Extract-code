-- Select a particular set of records from a table for comparision to the random selector.
-- Example input includes a WHERE like "(key_column_a = 123 and key_column_b = 234) OR (key_column_a = 124 and key_column_b = 235)
SELECT
     *
FROM
    $$DB_SCHEMA_NAME.$$TABLE_NAME
WHERE
    $$WHERE_CONDITION
