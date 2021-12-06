
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

r1=pd.read_csv("DEMAPPED_PRD_SKU_692021.csv")
#pd.read_csv place lo pd.read_excel petandi python script lo
#r1=pd.read_excel("TRACK_PID_GRP_FEB21_ANNMILLE.xlsx")
engine=create_engine(URL(
    user='pkosuri@cisco.com',
    password='62&Qualitysmart',
    account='cisco.us-east-1',
    authenticator='externalbrowser',
    warehouse='CXPS_CXP_ETL_WH',
    database='CXPS_DB',
    schema='CXPS_CXP_WI',
    role='CXPS_CXP_ETL_ROLE'
    ))
#create cursor
connection=engine.connect()
#execute SQL statement
r1.to_sql('DEMAPPED_PRD_SKU_692021',con=engine,if_exists='append', index=False, chunksize=16000)
    
