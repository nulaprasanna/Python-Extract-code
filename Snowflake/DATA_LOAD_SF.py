
import snowflake.connector as sf
#import pyodbc
import pandas as pd
import pandasql as pds
import snowflake.connector
import snowflake.connector as snow
import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

r1=pd.read_csv("C:\Prasanna\Snowflake\Customer_data.csv")
#pd.read_csv place lo pd.read_excel petandi python script lo
#r1=pd.read_excel("TRACK_PID_GRP_FEB21_ANNMILLE.xlsx")
engine=create_engine(URL(
    user='prasanna',
    password='Nanji123@a',
    account='https://hr91746.central-us.azure',
    authenticator='externalbrowser',
    warehouse='COMPUTE_WH',
    database='TRAININGDB',
    schema='SALES',
    role='ACCOUNTADMIN'
    ))
#create cursor
connection=engine.connect()
#execute SQL statement
r1.to_sql('Customer_data',con=engine,if_exists='append', index=False, chunksize=16000)
    
