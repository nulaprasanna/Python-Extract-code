"""
For the purpose of this example values have been hardcoded. Ideally, all values should be read from a config file.

- hvac module README: https://hvac.readthedocs.io/en/stable/
- The path, namespace, and token to the secret are required.
- The secret (a password/file/etc...) will be available via a key value pair at this location
- Keep the token secure! Treat it like a password.
- Keeper Guide: https://apps.na.collabserv.com/wikis/home?lang=en-us#!/wiki/W353fd6037057_40be_b531_cc887f2d65a9/page/Keeper%20(Vault)%20User%20guide
- Note, alphaeast.keeper.cisco.com is used for dev/stg. east.keeper.cisco.com is used for prd.
"""

import hvac

# Config properties
keeper_uri = 'https://alphaeast.keeper.cisco.com'
keeper_namespace = 'cloudDB'
keeper_token = 's.XiDg...'
secret_path = 'secret/snowflake/dev/platform_ops_svc/key'

client = hvac.Client(
    url=keeper_uri,
    namespace=keeper_namespace,
    token=keeper_token
)

try:
    # Collect all the data from keeper for the provided input
    full_keeper_returned_data = client.read(secret_path)

    # Secrets are stored within the key entitled 'data'
    keeper_secrets = full_keeper_returned_data['data']

    # extract from the secrets the components you need
    snowsql_passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
    snowsql_private_key = keeper_secrets['private_key']

    # NEVER STORE/PRINT/LOG these secrets. They are to be passed through only
    # WRONG!!!! print('passphrase: ' + str(snowsql_passphrase))
    # CORRECT! create_snowflake_connection.py(snowsql_passphrase, snowsql_private_key)
except hvac.exceptions.Forbidden as e:
    print('ERROR: Unable to collect secrets from keeper. Permission denied')
except Exception as e:
    print('ERROR: Unable to collect secrets from keeper.')
    print('ERROR: ' + str(e))
