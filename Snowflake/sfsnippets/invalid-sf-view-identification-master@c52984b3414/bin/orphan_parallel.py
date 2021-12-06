import sys
from os import path
sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
import concurrent.futures
import time
import pdb
import pandas as pd
from itertools import repeat
import argparse
import logging
import re
import os
from datetime import datetime
from dateutil.tz import tzlocal
import metadata.metadata_handler as meta
from utils import util
from alert.alert import Alert

#TODO
# Invalid View as subject instead of Orphaned view (since invalid view is straight forward and easy to understand)
# We will make this service an API end point so that teams can check the views periodically given the optional parameters like (views owned by ROLE, in SCHEMA, in DATABASE, named VIEW) (we will push this request as our priority)
# We currently are logging start_time and end_time of the run for our info. But I also think it’s more useful to log first_detected_date and last_detected_date instead for end users.
# We will have all the details around the bad views in the csv attached to the email. Currently only provide database name, schema name, view name and error msgs.
    
def get_all_dbs(account,db_name,sch_name,v_name,rol_name):
    try:
        # tables view contain materialized views
        where_clause = [" TABLE_CATALOG NOT LIKE 'CLONE%' "]
        where_clause.append(" TABLE_SCHEMA NOT LIKE 'SS_REFRESH' ")
        where_clause.append(" TABLE_SCHEMA NOT LIKE 'BR_REFRESH' ")
        where_clause.append(" TABLE_TYPE like '%VIEW' ")
        where_clause.append(" DELETED is NULL ")
        if db_name != '%':
            where_clause.append(f" TABLE_CATALOG like '{db_name}' ")
        if sch_name != '%':
            where_clause.append(f" TABLE_SCHEMA like '{sch_name}' ")  
        if v_name != '%':
            where_clause.append(f" TABLE_NAME like '{v_name}' ")   
        if rol_name != '%':
            where_clause.append(f" TABLE_OWNER like '{rol_name}' ")  
        where = ' AND '.join(where_clause)
        sf_qry_db = f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_OWNER FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES WHERE {where} " 
                    
        sf_conn = meta.create_sf_connection(account)
        sf_df_db = meta.execute_sf_qry(sf_conn, sf_qry_db)
        meta.close_connection(sf_conn)
        if len(sf_df_db):
            with concurrent.futures.ProcessPoolExecutor() as executor:
                results = list(executor.map(check_views,repeat(account),sf_df_db['TABLE_CATALOG'],sf_df_db['TABLE_SCHEMA'],sf_df_db['TABLE_NAME'],sf_df_db['TABLE_OWNER']))
                total_views = pd.concat(results)
                return total_views
        logging.info(f'ORPHAN: There are no such views {db_name}.{sch_name}.{v_name} for role {rol_name}')
        return []
    except Exception as e:
        logging.info(f'ORPHAN: Error: get_all_dbs {db_name}: {str(e)}')
        raise Exception(e)
    except concurrent.futures.process.BrokenProcessPool as ex:
        logging.info(f'ORPHAN: Concurrent Error:get_all_dbs {str(ex)}')
        raise Exception(ex)


def check_views(account,db_name,sch_name,v_name,role):
    try:
        sf_conn = meta.create_sf_connection(account)
        logging.debug(f'ORPHAN: check_views:  "{db_name}"."{sch_name}"."{v_name}"')
        start_time = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        sf_qry_check_views = f'explain select count(*) from  "{db_name}"."{sch_name}"."{v_name}"'
        sf_good_views =  meta.execute_sf_qry(sf_conn, sf_qry_check_views)
        meta.close_connection(sf_conn)
        end_time = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        df = pd.DataFrame([{'ACCOUNT_NAME' : account, 'DATABASE_NAME': db_name, 'SCHEMA_NAME' : sch_name,
        'VIEW_NAME' : v_name, 'ERROR_MSG' : '', 'IS_BAD': 'N', 'START_TIME': start_time, 'END_TIME': end_time,
        'ROLE':role if role else 'NO_OWNER_ROLE'}])
        logging.debug(f"ORPHAN: good views: {db_name}.{sch_name}.{v_name}")
        return df
    except Exception as e:
        end_time = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        err_msg = re.sub('\s+',' ',str(e))
        err_msg = err_msg.replace("'", "")
        df = pd.DataFrame([{'ACCOUNT_NAME' : account, 'DATABASE_NAME': db_name, 'SCHEMA_NAME' : sch_name,
        'VIEW_NAME' : v_name, 'ERROR_MSG' : err_msg, 'IS_BAD': 'Y', 'START_TIME': start_time, 'END_TIME': end_time,
        'ROLE':role if role else 'NO_OWNER_ROLE'}])
        logging.debug(f"ORPHAN: bad views: {db_name}.{sch_name}.{v_name}")
        return df
    except concurrent.futures.process.BrokenProcessPool as ex:
        logging.info(f'ORPHAN: Error: Concurrent error {str(ex)}')
        raise Exception(ex)
        
def get_support_email(role,account):
    sql = util.read_file('sql/get_support_mailer.sql').format(role,account)
    conn = meta.create_oracle_connection('EDNA')
    df = meta.execute_oracle_df_qry(conn, sql)
    if len(df):
        return df.iloc[0]['SUPPORT_EMAIL_ALIAS']
    else:
        return ''

def send_jinja(grouped,role):
    df = grouped.get_group(role)
    account = df.iloc[0]['ACCOUNT_NAME']
    table = df[['DATABASE_NAME','SCHEMA_NAME','VIEW_NAME','ERROR_MSG']]
    #sending jinja email result
    jinja = Alert(f'Invalid views detection results {account}')
    jinja.add_header('Snowflake- Invalid Views Alert','assets/images/snowflake.png')
    paragraph = '''
    It has been detected that some objects which you own are in an orphaned state. 
    This means these objects may no longer be used in queries until they are fixed. 
    Thus any downstream users of these objects will be impacted.'''
    jinja.add_paragraph(paragraph,'Hello Team,')

    paragraph = '''
    The following views are in an orphaned state:'''
    jinja.add_paragraph(paragraph,'Invalid Views')
    jinja.add_table(table)
    jinja.add_attachment(df)

    paragraph = '''
    Examine each object by attempting to determine the cause of the object dependency break. Look for changes such as missing tables or views, altered columns, and loss of access. You may try to reconstitute the object by issuing the associated DDL.'''
    jinja.add_paragraph(paragraph,'How Should I Respond to these Alerts?')

    paragraph = '''
    Alerts are stored within Snowflake for later viewing. A user may view the above alerts in each Snowflake lifecycle by running this query:
    SELECT * from "DEMO_DB"."VIEW_VALIDATOR"."SF_VIEW_VALIDATION"'''
    jinja.add_paragraph(paragraph,'Viewing My Alerts')

    paragraph = '''
    If you have any questions, please reach out to cloud-platform-support@cisco.com. We schedule the job to be run once everyday.'''
    jinja.add_paragraph(paragraph,'Additional Info')

    paragraph = '''
    Here we can understand the process by which a view can become orphaned. If a table behind a view is dropped, the view no longer has all dependencies met.'''
    jinja.add_paragraph(paragraph,'Understanding Orphaned Objects')
    jinja.add_images(['assets/images/example_oo.png'])


    paragraph = '''
    Regards,<br> 
    D&A Foundational Services Team
    '''
    jinja.add_paragraph(paragraph)
    jinja.render_template()
    support = get_support_email(role,account)
    if support:
        jinja.send_email([support])
    else:
        jinja.send_email(['invalid-views-alerts@cisco.com'])
        logging.info(f"ERROR: There is no support group assigned to the role {role} for {account}.")


def update_sf_views_log(sf_account, new_logs, table):  
    session,engine = meta.create_sf_engine(sf_account) 
    with engine.connect() as sf_conn: 
        try:
            from sqlalchemy.sql import text as sa_text
            temp_table = table+'_temp'
            engine.execute(sa_text(f'DROP TABLE IF EXISTS {temp_table}').execution_options(autocommit=True))
            new_logs.to_sql(name=temp_table,con=sf_conn,method='multi',chunksize=10000,index=False)
            sql = util.read_file('sql/sf_merge_view_logs.sql').format(table,temp_table)
            engine.execute(sa_text(sql).execution_options(autocommit=True))
        except Exception as e:
            if str(e).find(f"{table}' does not exist or not authorized"):
                new_logs.to_sql(name=table,con=sf_conn,if_exists="replace",method='multi',chunksize=10000,index=False)
            else:
                raise Exception(e)


def main(sf_account,db_name,sch_name,v_name,rol_name,sf_log_table):
    try:
        start = time.perf_counter()

        total_views = get_all_dbs(sf_account,db_name,sch_name,v_name,rol_name)
        logging.info(f"ORPHAN: LOGS: Toal {len(total_views)} views were checked.")

        #update oracle logs
        conn = meta.create_oracle_connection()
        sql = util.read_file('sql/oracle_views_log.sql')
        if len(total_views):
            sub_total_views = total_views.drop(columns=['ROLE'])
            meta.executemany_oracle_qry(conn, sql,sub_total_views.to_dict('records'))
            #update sf logs
            update_sf_views_log(sf_account, total_views, sf_log_table)
            #send email alerts to support mailer
            bad_views = total_views[total_views['IS_BAD'] == 'Y'].copy()
            if len(bad_views):
                bad_views = bad_views[['ACCOUNT_NAME','DATABASE_NAME','SCHEMA_NAME','VIEW_NAME','ERROR_MSG','ROLE']]
                grouped = bad_views.groupby('ROLE')
                roles = list(grouped.groups)
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    list(executor.map(send_jinja,repeat(grouped),roles))
        finish = time.perf_counter()
        duration = round(finish-start, 2)
        logging.info(f'ORPHAN: Finished in {duration} second(s)')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        util.send_system_alert(str(e), exc_type, fname,
                                  exc_tb.tb_lineno, sf_account, ['invalid-views-alerts@cisco.com'])
        logging.info("ORPHAN: Sent system error alert")
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("sf_account", help="Enter the snowflake account.",
                        type=str.upper,choices=['CISCOTEST.US-EAST-1','CISCODEV.US-EAST-1','CISCOSTAGE.US-EAST-1','CISCO.US-EAST-1'])
    parser.add_argument("-d","--dbname", help="Optional database name",type=str.upper, default='%')
    parser.add_argument("-s","--schname", help="Optional schema name",type=str.upper, default='%')
    parser.add_argument("-v","--vname", help="Optional view name",type=str.upper, default='%')
    parser.add_argument("-r","--rolename", help="Optional role name",type=str.upper, default='%')
    args = parser.parse_args()
    sf_account = args.sf_account
    db_name = args.dbname
    sch_name = args.schname
    v_name = args.vname
    rol_name = args.rolename

    # create loggers and apply settings from config.ini [GENERAL]
    snowflake_config_settings = util.collect_property_file_contents('../properties/config.ini', sf_account)
    general_config_settings = util.collect_property_file_contents('../properties/config.ini', 'GENERAL')
    logging.basicConfig(level=logging.INFO
                        , filename=snowflake_config_settings['log_file_path'].format('orphan_parallel.py')
                        , format='%(levelname)s:%(asctime)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    
    main(sf_account,db_name,sch_name,v_name,rol_name,snowflake_config_settings['bad_view_target_table'].lower())
