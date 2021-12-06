select A.SUPPORT_EMAIL_ALIAS
from edwds.edna_snowflake_app_role_mst R, edwds.edna_env_domain_snflk_app A, edwds.edna_environment_mst E
where upper(R.role) = '{}'
AND upper(R.account) = '{}'
AND R.APPLICATION_ACRONYM = A.APPLICATION_ACRONYM
and E.HOST_NAME = R.ACCOUNT