-- Collect the unique key from the eds repo IDed by a table. See EDS_DATA_CATALOG documentation for the ERD.
select
   UKC.*
FROM
    EDS_DATA_CATALOG.EDW_TABLE TBL
INNER JOIN
	EDS_DATA_CATALOG.EDW_UNIQUE_KEY UK
	ON
		TBL.ENVIRONMENT_NAME = UK.ENVIRONMENT_NAME
	AND
		TBL.DB_INSTANCE_NAME = UK.DB_INSTANCE_NAME
	AND
		TBL.DB_SCHEMA_NAME = UK.DB_SCHEMA_NAME
	AND
		TBL.TABLE_NAME = UK.TABLE_NAME
	AND
		UK.ACTIVE_FLAG = 'A'
INNER JOIN
	EDS_DATA_CATALOG.EDW_UNIQUE_KEY_COLUMN UKC
	ON
		UK.UNIQUE_KEY_NAME = UKC.UNIQUE_KEY_NAME
	AND
		UK.ENVIRONMENT_NAME = UKC.ENVIRONMENT_NAME
	AND
		UK.DB_INSTANCE_NAME = UKC.DB_INSTANCE_NAME
	AND
		UK.DB_SCHEMA_NAME = UKC.DB_SCHEMA_NAME
	AND
		UK.TABLE_NAME = UKC.TABLE_NAME
WHERE
        TBL.ENVIRONMENT_NAME = '$$ENVIRONMENT_NAME'
    and
        TBL.DB_INSTANCE_NAME = '$$DB_INSTANCE_NAME'
	and
		TBL.DB_SCHEMA_NAME = '$$DB_SCHEMA_NAME'
	and
		TBL.TABLE_NAME = '$$TABLE_NAME'