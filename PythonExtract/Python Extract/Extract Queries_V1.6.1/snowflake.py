import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

from practice import df

r1=pd.read_csv("C:\Prasanna\Snowflake\Customer_data.csv")
url = URL(
    user='prasanna',
    password='Nanji123@a',
    account='hr91746.central-us.azure',
    warehouse='COMPUTE_WH',
    database='TRAININGDB',
    schema='SALES',
    role='ACCOUNTADMIN',
    authenticator='externalbrowser',
)

engine = create_engine(url)
connection = engine.connect()

#r1=df.to_csv('C:\Prasanna\Snowflake\Customer_data.csvcsv')
r1.to_sql('CUSTOMER_SCRIPT',con=engine,if_exists='append', index=False, chunksize=16000)
query = 'select current_date'
data = pd.read_sql(query, connection)
print (data)