-- Select a random set of records with a limit on the number selected.
-- TODO: This is implemented as a straight select, and does not truly "sample". Needs a random selector.
-- TODO: For Example, perhaps in Oracle this could be done as where the last rowid character == some value. NR!
SELECT
     *
FROM
    $$DB_SCHEMA_NAME.$$TABLE_NAME
$$LIMIT_CONDITION
