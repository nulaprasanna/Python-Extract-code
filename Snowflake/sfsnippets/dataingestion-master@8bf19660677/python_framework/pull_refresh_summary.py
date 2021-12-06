#!/apps/python/install/bin/python
import cx_Oracle
import os, re, string, subprocess, sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib
def _formatheader(hlist):
	out="<tr>"
	for head in hlist:
		out += "<th>" + str(head) + "</th>"
	out += "</tr>"
	return(out)
def _formatrows(rlist):
	out=""
	for row in rlist:
		lineCnt=0
		out += "<tr>"
		for line in row:
			if lineCnt == 6:
				if "Success" in line:
					out += "<td bgcolor=#00ff00>" + str(line) + "</td>"
				elif "SRC2STG_Success_But_STG2BR_Fail" in line:
					out += "<td bgcolor=#ffff00>" + str(line) + "</td>"
				else:
					out += "<td bgcolor=#ff0000>" + str(line) + "</td>"
			else:
				out += "<td>" + str(line) + "</td>"
			lineCnt+=1
		out+="</tr>"
	return(out)
def _initiateHtml(jobGname):
	out="<html>"
	out += "<style>"
	out += "table {"
	out += "  font-size:12;"
	out += "  font-family: arial, sans-serif;"
	out += "  border-collapse: collapse;"
	out += "  width: 100%;"
	out += "}"
	out += ""
	out += "td, th {"
	out += "  border: 1px solid #dddddd;"
	out += "  text-align: left;"
	out += "  padding: 8px;"
	out += "}"
	out += ""
	out += "tr:nth-child(even) {"
	out += "  background-color: #e6fff7;"
	out += "}"
	out += "tr:nth-child(odd) {"
	out += "  background-color: #b3ffe7;"
	out += "}"
	out += "th:nth-child(odd) {"
	out += "  background-color: #ffff00;"
	out += "}"
	out += "th:nth-child(even) {"
	out += "  background-color: #ffff00;"
	out += "}"
	out += "</style>"
	out += "</head>"
	out += "<body>"
	out += ""
	out += "<h2><center>Job Group refresh summary - " + jobGname + "</h2>"
	out += ""
	out += "<table>"
	return out
def pull_job_group_refresh_summary(jobGroup,envName,Logs_Dir,reqid):
	try:
		os.environ['LD_LIBRARY_PATH']="/usr/cisco/packages/oracle/oracle-12.1.0.2/lib"
		os.environ['ORACLE_HOME']="/usr/cisco/packages/oracle/oracle-12.1.0.2"
		ejcparams={}
		ejcfile="/apps/edwsfdata/talend/TES"  + '/' + envName + '/ejc'
		exists=os.path.isfile(ejcfile)
		if not exists :
			raise Exception("EJC file - " + ejcfile + ' could not be located.. Please check and retry')
		fvar=open(ejcfile,'r')
		for line in fvar:
			if not line.strip().startswith("#") and len(line.strip())>0:
				name,value=line.split(';')
				ejcparams[name]=value.strip('\n')
		connstr = cx_Oracle.makedsn(ejcparams['EJC_HOST'], ejcparams['EJC_PORT'],service_name=ejcparams['EJC_SERVICENAME'])
		conn = cx_Oracle.connect(user=ejcparams['EJC_LOGIN'], password=ejcparams['EJC_PASSWORD'], dsn=connstr)
		cursor1=conn.cursor()
		cursor1.execute("select to_char(start_time,'YYYY-MM-DD HH24:MI:SS') from diy_job_group_refresh where reqid=" + str(reqid))
		results=cursor1.fetchall()
		for starttimeObj in results:
			startTime=starttimeObj[0]
		outf=open(Logs_Dir + 'out.html','w')
		#print("total job count is " + str(len(jobStreamarr)))
		htmlOut=_initiateHtml(jobGroup)
		loopcnt=0

		sql2="select ingestion_type as type,target_schema,target_table,decode(load_type,'DATE','INCREMENTAL','ID','INCREMENTAL','ALL_DATA','FULL','FULL') as load_type,"
		sql2+="src_rows,tgt_rows,decode(current_phase,'Success',' Success',current_phase) as status,start_time,end_time,round((end_time-start_time)*24*60,2) as duration_in_mins,"
		sql2+="err_msg,attribute1 as user_name,attribute4 as host_name,decode(retryFlag,'F','NO','T','YES','NO') as job_retried from diy_master "
		sql2+="where job_group='" + jobGroup + "' and last_update_time > to_date('" + str(startTime) + "','YYYY-MM-DD HH24:MI:SS') order by current_phase,target_table,target_schema"

		cursor1.execute(sql2)
		#get header and rows
		headerS = [i[0] for i in cursor1.description]
		headOut=_formatheader(headerS)
		htmlOut += headOut
		rows = [list(i) for i in cursor1.fetchall()]
		htmlOut += _formatrows(rows)
		htmlOut += "</table> </body> </html>"
		cursor1.close()
		conn.close()
		outf.write(htmlOut)
		outf.close()
	except Exception as e:
		print ("Error encountered while pulling job refresh summary .. Exception message given below")
		print(str(e))
def sendHtmEmail(emailAdd,logfile,Subject,jobGroup):
		with open (logfile,"r") as outf:
			html=outf.read().replace('\n', '')
		outf.close()
		logFile=open(logfile)
		msg=MIMEMultipart("alternative")
		part2 = MIMEText(html, 'html')
		msgfrom='sf-smart-ingestion@cisco.com'
		tolist=emailAdd
		msg['Subject'] = Subject
		msg['From']    = msgfrom
		msg['To']      = tolist
		file=logFile.name
		#msg.attach(MIMEText("ouput.htm"))
		attachment = MIMEBase('application', 'octet-stream')
		attachment.set_payload(open(file, 'rb').read())
		encoders.encode_base64(attachment)
		outfname=jobGroup + ".htm"
		attachment.add_header('Content-Disposition', 'attachment; filename="%s"' % outfname)
		msg.attach(attachment)
		msg.attach(part2)
		s=smtplib.SMTP("outbound.cisco.com")
		s.sendmail(msgfrom,tolist.split(","),msg.as_string())
		s.quit()