from alert import Alert
import pandas as pd

def test_attach(tolist):
    jinja = Alert('Testing: attachment to base.html')
    df = pd.DataFrame({'name': ['Somufdskghlkdsfhkgfh', 'Kiku', 'Amol', 'Lini'],
     'physics': [68, 74, 77, 78],
     'chemistry': [84, 56, 73, 69],
     'algebra': [78, 88, 82, 87]})
    jinja.add_attachment(df)
    #render dataframe as html
    jinja.render_template()
    jinja.send_email(tolist)

def test_base(tolist):
    jinja = Alert('Testing:.. base')

    #add header
    #test w logo
    title = 'Snowflake- Orphaned View Alert'
    cid = jinja.add_header(title,'../assets/images/snowflake.png')
    # cid = jinja.add_header(title)
    
    #add md content
    sf_account = 'CISCO.US-EAST-1'
    db_name = 'EDW_SERVICE_ETL_DB'
    sch_name = '%'
    v_name = 'SELECT_BACKLOG_2_VW_MTTC'
    rol_name = '%'
    template_variables = {
        'account' : sf_account,
        'database' : '' if db_name == '%' else f' -d {db_name}',
        'schema' : '' if sch_name == '%' else f' -s {sch_name}',
        'view' : '' if v_name == '%' else f' -v {v_name}',
        'role' : '' if rol_name == '%' else f' -r {rol_name}',
        'badViews': 100,
        'totalViews':200,
        'duration': '1022 seconds',
        'table' : 'SF_ORPHAN_VIEWS_LOG'
    }
    # md_content = jinja.add_markdown('test.md',template_variables)

    #test wo attach files
    df = pd.DataFrame({'name': ['Somufdskghlkdsfhkgfh', 'Kiku', 'Amol', 'Lini'],
     'physics': [68, 74, 77, 78],
     'chemistry': [84, 56, 73, 69],
     'algebra': [78, 88, 82, 87]})
    jinja.add_attachment(df)

    #add table
    table_content = jinja.add_table(df)

    #add paragraph
    subheading = 'Hello Team,'
    paragraph = '''
     It has been detected that some objects which you own are in an orphaned state. 
     This means these objects may no longer be used in queries until they are fixed. 
     Thus any downstream users of these objects will be impacted.
    '''  
    j_paragraph_h = jinja.add_paragraph(paragraph,subheading)
    j_paragraph = jinja.add_paragraph(paragraph)

    jinja.render_template()
    jinja.send_email(tolist)
    

def test_extend(html_file,tolist):
    
    title = 'Snowflake- Orphaned View Alert'
      
    jinja = Alert('Testing: extentsion')
    jinja.extend_template(html_file)
    
    #add header
    header = jinja.add_header(title)

    #add md
    sf_account = 'CISCO.US-EAST-1'
    db_name = 'EDW_SERVICE_ETL_DB'
    sch_name = '%'
    v_name = 'SELECT_BACKLOG_2_VW_MTTC'
    rol_name = '%'
    template_variables = {
        'account' : sf_account,
        'database' : '' if db_name == '%' else f' -d {db_name}',
        'schema' : '' if sch_name == '%' else f' -s {sch_name}',
        'view' : '' if v_name == '%' else f' -v {v_name}',
        'role' : '' if rol_name == '%' else f' -r {rol_name}',
        'badViews': 100,
        'totalViews':200,
        'duration': '1022 seconds',
        'table' : 'SF_ORPHAN_VIEWS_LOG'
    }
    md_content = jinja.add_markdown('./markdown/test.md',template_variables)
    
    #add attachment
    df = pd.DataFrame({'name': ['Somufdskghlkdsfhkgfh', 'Kiku', 'Amol', 'Lini'],
     'physics': [68, 74, 77, 78],
     'chemistry': [84, 56, 73, 69],
     'algebra': [78, 88, 82, 87]})
    jinja.add_attachment(df)

    #add table
    j_table = jinja.add_table(df)


    #add paragraph
    subheading = 'Hello Team,'
    paragraph = '''
     It has been detected that some objects which you own are in an orphaned state. 
     This means these objects may no longer be used in queries until they are fixed. 
     Thus any downstream users of these objects will be impacted.
    '''  
    j_paragraph_h = jinja.add_paragraph(paragraph,subheading)
    j_paragraph = jinja.add_paragraph(paragraph)

    template_variables = {'header':header,'subheading':subheading, 'paragraph': j_paragraph, 'paragraph_h': j_paragraph_h,'md_content':md_content,'table':j_table}
    jinja.render_template(template_variables)
    jinja.send_email(tolist)

def test_images(images,recipient):
    jinja = Alert('Testing: images to base.html')
    jinja.add_images(images)
    jinja.render_template()
    jinja.send_email(recipient)
    
def test_orphan():
    jinja = Alert('Orphan view detection results')
    cid = jinja.add_header('Snowflake- Orphaned View Alert','../assets/images/snowflake.png')
    paragraph = '''
    It has been detected that some objects which you own are in an orphaned state. 
    This means these objects may no longer be used in queries until they are fixed. 
    Thus any downstream users of these objects will be impacted.'''
    jinja.add_paragraph(paragraph,'Hello Team,')

    dummy = '''002037 (42601): SQL compilation error: Failure during expansion of view V_SMART_LIC_FULFILMENT: SQL compilation error: Object EDW_SERVICE_ETL_DB.SS.LICENSE_FEATURES_D does not exist or not authorized.'''
    df = pd.DataFrame({'name': ['Somufdskghlkdsfhkgfh', 'Kiku', 'Amol', 'Lini', 'Leo','Alex'],
    'physics': [dummy, 74, 77, 78,dummy,dummy],
    'chemistry': [84, dummy, 73, 69,dummy,dummy],
    'algebra': [78, 88, dummy, 87,dummy,dummy]})
    jinja.add_attachment(df)

    paragraph = '''
    The following views are in an orphaned state:'''
    jinja.add_paragraph(paragraph,'Invalid Views')
    jinja.add_table(df)

    paragraph = '''
    Examine each object by attempting to determine the cause of the object dependency break. Look for changes such as missing tables or views, altered columns, and loss of access. You may try to reconstitute the object by issuing the associated DDL.'''
    jinja.add_paragraph(paragraph,'How Should I Respond to these Alerts?')

    paragraph = '''
    Alerts are stored within Snowflake for later viewing. A user may view the above alerts in each Snowflake lifecycle by running this query:
    SELECT * from "DEMO_DB"."VIEW_VALIDATOR"."SF_VIEW_VALIDATION"'''
    jinja.add_paragraph(paragraph,'Viewing My Alerts')

    paragraph = '''
    Here we can understand the process by which a view can become orphaned. If a table behind a view is dropped, the view no longer has all dependencies met.'''
    jinja.add_paragraph(paragraph,'Understanding Orphaned Objects')

    jinja.add_images(['../assets/images/example_oo.png'])

    paragraph = '''
    Regards,<br>
    D&A Foundational Services Team
    '''
    jinja.add_paragraph(paragraph)
    jinja.render_template()
    jinja.send_email(['invalid-views-alerts@cisco.com'])

def run_test():
    recipient = ['invalid-views-alerts@cisco.com']
    ####test 1
    # test_extend('./html/test_ext.html',recipient)
    # ####test 2
    # test_attach(recipient)
    # ####test 3
    # test_base(recipient)
    # ####test 4
    # images = ['../assets/images/example_oo.png']
    # for z, filename in enumerate(images):
    #     fp = open(filename, 'rb')
    # test_images(images,recipient)
    ####test 5
    test_orphan()
