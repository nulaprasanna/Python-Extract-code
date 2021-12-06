#!/apps/python/install/bin/python
#########################################################################################################################
# email_audit_report.py                                                                                                 #
# Script to email aduit report                                                                                          #
# Modification History                                                                                                  #
# Version       Modified By     Date                    Change History                                                  #
# 1.0           Manick          Feb-2021                Initial Version                                                 #
# 1.1           Manick          Mar-2021                Enabled emailing multiple images on same email                  #
# 1.2           Manick          Mar-2021                Include JIRA table and permissible delta as attachment          #
# 1.3           Manick          Mar-2021                Hide 0% on pie chart and include table for recurring autofix    #
# 1.4           Manick          Mar-2021                Collect and insert trend of audit values                        #
# 1.5           Manick          Mar-2021                Increase font size                                              #
# 1.6           Manick          Mar-2021                Include trend line chart                                        #
#########################################################################################################################
from metadata_handler import *
import argparse
from matplotlib import pyplot as plt
import numpy as np
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import smtplib,html,os,uuid
from email.mime.base import MIMEBase
from email import encoders
from email.header import Header
from datetime import date


def plot_graph(db_type,tot_cnt,excep_cnt):
	y = np.array([tot_cnt,excep_cnt])
	#mylabels = ['AUDIT MATCH', 'AUDIT MISMATCH']
	onelabel = ['AUDIT MATCH','']
	twolabels = ['AUDIT MATCH', 'AUDIT MISMATCH']
	mycolors = ["green", "Red"]
	myexplode = [0.3, 0]
	plt.rcParams.update({'font.size': 16})
	if y[1] > 0:
		plt.pie(y, labels = twolabels, explode = myexplode,colors=mycolors,autopct=lambda p: '{:.4f}%'.format(round(p,4)) if p > 0 else '',startangle=45 )
	else:
		plt.pie(y, labels = onelabel, explode = myexplode,colors=mycolors,autopct=lambda p: '{:.4f}%'.format(round(p,4)) if p > 0 else '',startangle=45 )
	plt.title(f"{db_type} AUDIT (%)")
	plt.savefig(f"{logs_dir}/{reqid}/{db_type}.jpg")
	plt.clf()
	plt.cla()
	plt.close()

def plot_trendline():
	# Pull trend data
	sql="select to_char(log_date,'mm/dd') dt,round((1-((orcl_mismatch_cnt+hana_mismatch_cnt+td_mismatch_cnt)/(orcl_tot_cnt+hana_tot_cnt+td_tot_cnt)))*100,2) cnt from diy_audit_trend where log_date > sysdate -31 order by log_date"
	res=execute_df_qry(conn,sql,reqid,conn)
	dates =res["dt"].to_numpy()
	aud_pct=res["cnt"].to_numpy()
	
	fig, ax1 = plt.subplots()
	plt.title(f"Audit match % across all platforms")

	color = 'tab:red'
	ax1.set_xlabel('Audit Date')
	ax1.set_ylabel('Audit match %')
	ax1.plot(dates, aud_pct, color=color,marker='o')
	ax1.tick_params(axis='y')
	plt.xticks(rotation=90)
	fig.tight_layout()  # otherwise the right y-label is slightly clipped
	plt.savefig(f"{logs_dir}/{reqid}/trend.jpg")
	plt.clf()
	plt.cla()
	plt.close()

def attach_file(filename):
    part = MIMEBase('application', 'octect-stream')
    part.set_payload(open(filename, 'rb').read())
    Encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename=%s' % os.path.basename(filename))
    return part

def attach_image(img_dict):
	with open(img_dict['path'], 'rb') as file:
		msg_image = MIMEImage(file.read(), name = os.path.basename(img_dict['path']))
		msg_image.add_header('Content-ID', '<{}>'.format(img_dict['cid']))
	return msg_image

def attach_file(filename):
	part = MIMEBase('application', 'octect-stream')
	part.set_payload(open(filename, 'rb').read())
	encoders.encode_base64(part)
	part.add_header('Content-Disposition', 'attachment; filename=%s' % os.path.basename(filename))
	return part

def generate_email(to_list):
	global tot_array,exception_array,perms_delta_array
	today=date.today()
	sql="select * from edw_audit_mon where out_of_sync_flag='N'"
	out=execute_df_qry(conn,sql,reqid,conn)
	out.to_csv(f'{logs_dir}/{reqid}/perms_delta.csv',index=False)
	orcl_perf_match=tot_array['ORACLE']-exception_array['ORACLE']-perms_delta_array['ORACLE']
	td_perf_match=tot_array['TERADATA']-exception_array['TERADATA']-perms_delta_array['TERADATA']
	hana_perf_match=tot_array['HANA']-exception_array['HANA']-perms_delta_array['HANA']
	dt=today.strftime("%d-%b-%Y")
	img1 = dict(title = 'Oracle', path = f"{logs_dir}/{reqid}/ORACLE.jpg", cid = str(uuid.uuid4()))
	img2 = dict(title = 'Teradata', path = f"{logs_dir}/{reqid}/TERADATA.jpg", cid = str(uuid.uuid4()))
	img3 = dict(title = 'Hana', path = f"{logs_dir}/{reqid}/HANA.jpg", cid = str(uuid.uuid4()))
	img4 = dict(title = 'Audit Trend', path = f"{logs_dir}/{reqid}/trend.jpg", cid = str(uuid.uuid4()))
	msg =MIMEMultipart('related')
	msg['Subject'] = f'Ingestion audit report for {dt}'
	msg['From'] = 'audit-report@cisco.com'
	msg['To'] = to_list
	msg_alternative = MIMEMultipart('alternative')
	msg_text = MIMEText(u'Image not working - maybe next time', 'plain', 'utf-8')
	msg_alternative.attach(msg_text)
	msg.attach(msg_alternative)
	msg.attach(attach_file(f'{logs_dir}/{reqid}/perms_delta.csv'))
	msg_html = u'<h1>Ingestion Audit Report</h1>'
	msg_html += u'<table> <tr> <td>'
	msg_html += u'<div dir="ltr">''<img src="cid:{cid}" width="85%" height="85%" alt="{alt}"><br></div>'.format(alt=html.escape(img1['title'], quote=True), **img1)
	msg_html += u'</td><td>'
	msg_html += u'<div dir="ltr">''<img src="cid:{cid}" width="85%" height="85%" alt="{alt}"><br></div>'.format(alt=html.escape(img2['title'], quote=True), **img2)
	msg_html += u'</td><td>'
	msg_html += u'<div dir="ltr">''<img src="cid:{cid}" width="85%" height="85%" alt="{alt}"><br></div>'.format(alt=html.escape(img3['title'], quote=True), **img3)
	msg_html += u'</td></tr></table>'
	msg_html += u'<html><head><style>table, th, td {  border: 1px solid black;  border-collapse: collapse;}th, td {  padding: 15px;text-align: center;font-size: 14px;}</style></head><body>'
	msg_html += u'<table border=yes width=50% align=center> <tr> <th> Audit Category </th> <th> Oracle </th> <th> Teradata </th> <th> Hana </th>  </tr>'
	msg_html += f'<tr> <td> Total Table Count </td> <td> {tot_array["ORACLE"]} </td> <td> {tot_array["TERADATA"]} </td> <td> {tot_array["HANA"]} </td></tr>'
	msg_html += f'<tr> <td> Perfect Match Count </td> <td> {orcl_perf_match} </td> <td> {td_perf_match} </td> <td> {hana_perf_match} </td></tr>'
	msg_html += f'<tr> <td> Permissible Delta Count </td> <td> {perms_delta_array["ORACLE"]} </td> <td> {perms_delta_array["TERADATA"]} </td> <td> {perms_delta_array["HANA"]} </td></tr>'
	msg_html += f'<tr> <td> Mis-match Count </td> <td> {exception_array["ORACLE"]} </td> <td> {exception_array["TERADATA"]} </td> <td> {exception_array["HANA"]} </td></tr>'
	msg_html += u'</table>'
	msg_html += u'<br><br> <h2> Audit Trend </h2>'
	msg_html += u'<div dir="ltr">''<img src="cid:{cid}" width="25%" height="25%" alt="{alt}"><br></div>'.format(alt=html.escape(img4['title'], quote=True), **img4)
	#sql=f"select target_db td,target_schema ts,target_table tt,occurrence_cnt oc,first_reported_date frd,last_reported_date lrd,jira_number jn from edw_audit_mon_jira where resolved_flag='N'"
	sql=f"select a.target_db td,a.target_schema ts,a.target_table tt,occurrence_cnt oc,first_reported_date frd,last_reported_date lrd,jira_number jn from edw_audit_mon_jira a,edw_audit_mon b"
	sql+=f" where b.OUT_OF_SYNC_FLAG='Y' and a.target_db=b.target_db and a.target_schema=b.target_schema and a.target_table=b.target_table"
	results=execute_df_qry(conn,sql,reqid,conn)
	if len(results) > 0:
		msg_html += u'<br><br><h2> Outstanding audit issue list </h2> <table border=yes width=50%>'
		msg_html += u'<tr> <th> Table Name </th> <th nowrap> Recurrence count </th> <th> JIRA </th> <th nowrap> First reported date </th> <th nowrap> Last reported date </th> </tr>'
		for index,row in results.iterrows():
			msg_html += f'<tr> <td> {row["td"]}.{row["ts"]}.{row["tt"]} </td> <td> {row["oc"]} </td> <td> {row["jn"]} </td> <td nowrap> {row["frd"]} </td> <td nowrap> {row["lrd"]} </td> </tr>'
		msg_html += u'</table>'
	sql=f"select count(*) cnt ,a.target_db || '.' || a.target_schema || '.' || a.target_table tn"
	sql+=f" from edw_audit_mon_history a,diy_aud_auto_fix_filter b,edw_job_streams c"
	sql+=f" where a.log_date in ( select distinct log_date from edw_audit_mon) and c.target_table_name=a.target_Table and c.target_schema=a.target_schema and c.target_db_name=a.target_db"
	sql+=f" and c.job_stream_id_link=b.job_stream_id and b.status='Success' group by a.target_db || '.' || a.target_schema || '.' || a.target_table order by 1 desc"
	results=execute_df_qry(conn,sql,reqid,conn)
	if len(results) > 0:
		msg_html +=u'<br><br><h2> Auto fix audit details </h2> <table border=yes width=50%>'
		msg_html +=u'<tr> <th> Auto fix history count </th> <th nowrap> Snowflake table name </th></tr>'
		for index,row in results.iterrows():
			msg_html += f'<tr> <td> {row["cnt"]} </td> <td nowrap> {row["tn"]}  </td> </tr>'
		msg_html += u'</table>'
	msg_html += u'</body>'
	msg_html = MIMEText(msg_html, 'html', 'utf-8')
	msg_alternative.attach(msg_html)
	msg.attach(attach_image(img1))
	msg.attach(attach_image(img2))
	msg.attach(attach_image(img3))
	msg.attach(attach_image(img4))
	return msg

def send_email(to_list):
	email_msg = generate_email(to_list)
	smtp = smtplib.SMTP()
	smtp.connect('outbound.cisco.com')
	smtp.sendmail('audit-report@cisco.com', to_list, email_msg.as_string())

def gather_stats():
	global tot_array,exception_array,perms_delta_array
	sql=f"select /*+ parallel (8) */ count(*) cnt,source_db_type sdt from EDW_INGESTION_DATA_AUDIT a,diy_master b where a. source_db=b.source_db_name and a.source_schema=b.source_schema" 
	sql+=f" and a.source_table=b.source_table and a.start_time=b.start_time group by source_db_type"
	total_obj=execute_df_qry(conn,sql,reqid,conn)
	for ind,row in total_obj.iterrows():
		tot_array[row["sdt"]]=row["cnt"]
	sql=f"select count(*) cnt,source_db_type sdt from EDW_AUDIT_MON a,diy_master b,diy_soft_delete c where a.source_db=b.source_db_name and a.source_schema=b.source_schema" 
	sql+=f" and a.source_table=b.source_table and a.out_of_sync_flag='Y' and b.reqid=c.reqid and a.start_time=b.start_time group by source_db_type"
	excep_obj=execute_df_qry(conn,sql,reqid,conn)
	for ind,row in excep_obj.iterrows():
		exception_array[row["sdt"]]=row["cnt"]
	sql=f"select count(*) cnt,source_db_type sdt from EDW_AUDIT_MON a,diy_master b,diy_soft_delete c where a.source_db=b.source_db_name and a.source_schema=b.source_schema" 
	sql+=f" and a.source_table=b.source_table and a.out_of_sync_flag='N'  and b.reqid=c.reqid and a.start_time=b.start_time group by source_db_type"
	delta_obj=execute_df_qry(conn,sql,reqid,conn)
	for ind,row in delta_obj.iterrows():
		perms_delta_array[row["sdt"]]=row["cnt"]
	orcl_tbl_cnt=tot_array['ORACLE']
	if 'ORACLE' in exception_array:
		orcl_excep_cnt=exception_array['ORACLE']
	else:
		orcl_excep_cnt=0
		exception_array['ORACLE']=0
	td_tbl_cnt=tot_array['TERADATA']
	if 'TERADATA' in exception_array:
		td_excep_cnt=exception_array['TERADATA']
	else:
		td_excep_cnt=0
		exception_array['TERADATA']=0
	hana_tbl_cnt=tot_array['HANA']
	if 'HANA' in exception_array:
		hana_excep_cnt=exception_array['HANA']
	else:
		hana_excep_cnt=0
		exception_array['HANA']=0
	if 'ORACLE' not in perms_delta_array:
		perms_delta_array['ORACLE']=0
		orcl_perms_cnt=0
	else:
		orcl_perms_cnt=perms_delta_array['ORACLE']
	if 'TERADATA' not in perms_delta_array:
		perms_delta_array['TERADATA']=0
		td_perms_cnt=0
	else:
		td_perms_cnt=perms_delta_array['TERADATA']
	if 'HANA' not in perms_delta_array:
		perms_delta_array['HANA']=0
		hana_perms_cnt=0
	else:
		hana_perms_cnt=perms_delta_array['HANA']
	sql=f"Insert into diy_audit_trend(log_date,orcl_tot_cnt,orcl_mismatch_cnt,orcl_perms_cnt,hana_tot_cnt,hana_mismatch_cnt,hana_perms_cnt,td_tot_cnt,td_mismatch_cnt,td_perms_cnt)"
	sql+=f" values (trunc(sysdate),{orcl_tbl_cnt},{orcl_excep_cnt},{orcl_perms_cnt},{hana_tbl_cnt},{hana_excep_cnt},{hana_perms_cnt},{td_tbl_cnt},{td_excep_cnt},{td_perms_cnt})"
	execute_qry(conn,sql,reqid,conn)
	execute_qry(conn,'commit',reqid,conn)
	plot_graph('ORACLE',orcl_tbl_cnt,orcl_excep_cnt)
	plot_graph('TERADATA',td_tbl_cnt,td_excep_cnt)
	plot_graph('HANA',hana_tbl_cnt,hana_excep_cnt)
	plot_trendline()


parser = argparse.ArgumentParser(description='Script to extract and email audit report',
    epilog='Example: python email_audit_report.py -e env_type -r reqid')
parser.add_argument('-e','--env',required=True,
	help='Envrionment type (TS*/DV*/PRD) of Job that is already ingested')
parser.add_argument('-r','--reqid',required=True,
	help='Reqid for which report is being generated')
parser.add_argument('-a','--email',required=True,
	help='Email id to which report needs to be sent')
try:
	args=parser.parse_args()
	env=args.env
	reqid=args.reqid
	emailadd=args.email
	tot_array={}
	exception_array={}
	perms_delta_array={}
	logs_dir="/apps/edwsfdata/python/logs/auditmon"
	if not os.path.exists(f'{logs_dir}/{reqid}'):
		os.mkdir(f'{logs_dir}/{reqid}')
	conn = open_oracle_connection(env,None,True)
	if type(conn) is int:
		raise Exception ("Unable to connect to repository DB")
	gather_stats()
	send_email(emailadd)
except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Failure in line {linenu}. Please find below exception \n {e}")

