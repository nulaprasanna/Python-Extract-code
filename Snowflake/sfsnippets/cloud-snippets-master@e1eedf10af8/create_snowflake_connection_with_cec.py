import snowflake.connector

"""
*** If Using Java then simply add the authenticator='externalbrowser' <K,V> pair to the properties file ***

Connecting to Snowflake should be done with Keeper integration for re-occurring activities. For one off or manual
testing you may use SSO integration via an external browser.
"""

ctx = snowflake.connector.connect(
      user='CECID@cisco.com'
    , account='ciscodev.us-east-1'
    , authenticator='externalbrowser'
    # , warehouse='YOUR _WAREHOUSE'
    # , role='YOUR _ROLE'
    # , database='YOUR_DATABASE'
    # , schema='YOUR_SCHEMA'
)

cur = ctx.cursor()
try:
    cur.execute("show warehouses")
    print('Nice you are now connected to snowflake :) ')
finally:
    cur.close()

