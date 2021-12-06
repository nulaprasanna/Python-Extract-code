import sys
from os import path
sys.path.insert(0, path.dirname( path.dirname( path.abspath(__file__) ) ) )
import concurrent.futures
import metadata.metadata_handler as meta
from itertools import repeat
import pandas as pd
import metadata.metadata_handler as meta
from alert.alert import Alert
import utils.util as util

    
if __name__ == '__main__':
# utils.test_jinja('email-success-template.html')
# utils.test_jinja('table-test.html')
    print('testing bin/test.py')
    def update_sf_views_log(sf_account,df,table):
        engine = meta.create_sf_engine(sf_account)
        with engine.connect() as sf_conn:
            try:
                sql = f"select DB,SCHEMA,COUNT,VIEW from {table}"
                old_logs = pd.read_sql_query(sql, con=sf_conn)
                old_logs.columns = map(str.upper, old_logs.columns)
                print('old df',old_logs)
                new = old_logs[['DB','SCHEMA']].merge(df, how='outer').fillna(old_logs)
                print('new',new)
                new.to_sql(name=table,con=sf_conn,if_exists="replace",method='multi',index=False)
            except Exception as e:
                if str(e).find('does not exist or not authorized'):
                    df.to_sql(name=table,con=sf_conn,if_exists="replace",method='multi',index=False)
                else:
                    raise Exception(e)

    df1 = pd.DataFrame({'DB': ['Falcon', 'Falcon','Falcon','Parrot', 'ZIn1','Zin2'],
                    'SCHEMA':['SS1', 'SS2','SS3', 'SS5', 'SS5','SS6'],
                    'COUNT': [1,2,3,4,5,6],
                    'VIEW':['a','b','c','d','e','f']})
    df2 = pd.DataFrame({'DB': ['Falcon', 'Falcon','Falcon','Parrot', 'Parrot','Calcium'],
                    'SCHEMA': ['SS1', 'SS2','SS3', 'SS_DVA', 'SS5','SS6'],
                    'COUNT':  [11,12,13,18,16,17],
                    'VIEW':['g','h','i','j','k','l']})
    
    # update_sf_views_log("CISCO.US-EAST-1",df1,'sf_orphan_views_log')
    def get_support_email(role,account):
        sql = util.read_file('sql/get_support_mailer.sql').format(role,account)
        conn = meta.create_oracle_connection('EDNA')
        df = meta.execute_oracle_df_qry(conn, sql)
        if len(df):
            return df.iloc[0]['SUPPORT_EMAIL_ALIAS']
        else:
            return ''
    jinja = Alert('Invalid view detection results')
    jinja.render_template()
    util
    support = get_support_email('MKTG_POCE_RPT_ROLE','CISCO.US-EAST-1')
    if support:
        print(f"******jinja****** {support}")
        support = 'invalid-views-alerts@cisco.com'
        jinja.send_email([support])
    else:
        print(f"ERROR: There is no support group assigned to the role {role} for {df.iloc[0]['ACCOUNT']}.")
