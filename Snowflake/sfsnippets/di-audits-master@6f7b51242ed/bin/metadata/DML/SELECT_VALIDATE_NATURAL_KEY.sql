-- Using a provided natural key, this query identifies the number of occurrences of duplicates.
SELECT COUNT(1) AS DUPLICATES FROM (
	SELECT
		  COUNT(1) DUPLICATES
		, $$NATURAL_KEY
	FROM
		$$DB_SCHEMA_NAME.$$TABLE_NAME
	GROUP BY
		$$NATURAL_KEY
	HAVING
		COUNT(1) > 1
) TOTAL_DUPS