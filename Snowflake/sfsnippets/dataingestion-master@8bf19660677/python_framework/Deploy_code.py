#!/apps/python/install/bin/python
#########################################################################################################################
# Deploy_code.py                                                                                                        #
# Script to Collect metadata                                                                                            #
# Modification History                                                                                                  #
# Version         Modified By   Date            Change History                                                          #
# 1.0             Manick        Oct-2019        Initial Version                                                         #
#########################################################################################################################
import sys, pdb,subprocess
import cx_Oracle
import os
import argparse
from metadata_handler import *
from prettytable import PrettyTable
from datetime import datetime

parser = argparse.ArgumentParser(description='Script to deploy code',
								 epilog='Example: python Deploy_code.py -n host_name -s script_name -r report_only')
parser.add_argument('-n', '--hname', required=True,
					help='Host name where you want to deploy the code')
parser.add_argument('-s', '--script',
					help='Deploy only a specific script (ex: DIY_src2stg_mk.py)')
parser.add_argument('-r', '--reponly',
					help='Generate only report after comparing. But dont deploy.Valid values are yes/no')
try:
	args = parser.parse_args()
	hname = args.hname
	ssname = args.script
	if ssname is None:
		ssname=''
	rep_only=args.reponly
	if rep_only is None:
		rep_only=True
	else:
		if rep_only.lower() not in ('yes','no'):
			print("Invalid value for -r verb. It can be either Yes or No")
			sys.exit(1)
		else:
			if rep_only.lower() == 'yes':
				rep_only=True
			else:
				rep_only=False
	currHostName=os.uname()[1]
	rcmd='date'
	remotecmd=subprocess.Popen("ssh -q {host} '{cmd}'".format(host=hname, cmd=rcmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	print("Validating ssh")
	(out,err)=remotecmd.communicate()
	retcode=remotecmd.returncode
	if retcode != 0:
		print(f"SSH setup doesnt exist between {currHostName} and {hname}")
		sys.exit(1)
	print("Opening repos connection")
	conn=open_oracle_connection('PRD',None,True)
	cursor=conn.cursor()
	print("Running repos query to pull checksum")
	if len(ssname) > 0:
		sql=f"select path,script,chksum1,chksum2 from diy_code_repository where host_name='{currHostName}' and script='{ssname}'"
	else:
		sql=f"select path,script,chksum1,chksum2 from diy_code_repository where host_name='{currHostName}'"
	cursor.execute(sql)
	results=cursor.fetchall()
	t = PrettyTable(['ScriptName', 'local_chksum1', 'remote_chksum1', 'local_chksum2', 'remote_chksum2'])
	for obj in results:
		spath=obj[0]
		sname=obj[1]
		cksum1=obj[2]
		cksum2=obj[3]
		print(f"Working on {spath}/{sname}")
		scr_2_chk=f"{spath}/{sname}"
		localcmd=subprocess.Popen(f"cksum {spath}/{sname}",shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		(out,err)=localcmd.communicate()
		if retcode != 0:
			err_msg=err.decode('ascii').rstrip('\n')
			print(f"Chksum on local host for  {spath}/{sname} failed due to error - {err_msg}")
			sys.exit(1)
		else:
			out_msg=out.decode('ascii').rstrip('\n')
			lcksum1=int(out_msg.split(' ')[0])
			lcksum2=int(out_msg.split(' ')[1])
			if (cksum1 != lcksum1) or (cksum2 != lcksum2):
				print(f"Check sum mis-match exist between what is stored in repos db and what exists on local host for {spath}/{sname}")
				sql=f"update diy_code_repository set chksum1={lcksum1},chksum2={lcksum2} where host_name='{currHostName}' and path='{spath}' and script='{sname}'"
				cursor.execute(sql)
				cursor.execute('commit')
				cksum1=lcksum1
				cksum2=lcksum2
		rcmd=f"cksum {scr_2_chk}"
		remotecmd=subprocess.Popen("ssh -q {host} '{cmd}'".format(host=hname, cmd=rcmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		(out,err)=remotecmd.communicate()
		retcode=remotecmd.returncode
		if retcode != 0:
			err_msg=err.decode('ascii').rstrip('\n')
			print(f"SSH failed due to error - {err_msg}")
			if 'No such file or directory' in err_msg:
				print(f"***** Now copying {spath}/{sname}")
				remotecmd=subprocess.Popen(f"scp -q {spath}/{sname} {hname}:{scr_2_chk}",shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				(out,err)=remotecmd.communicate()
				retcode=remotecmd.returncode
				if retcode != 0:
					err_msg=err.decode('ascii').rstrip('\n')
					print(f"SCPing script {scr_2_chk} failed due to error - {err_msg}")
					sys.exit(1)
			else:
				sys.exit(1)
		else:
			out_msg=out.decode('ascii').rstrip('\n')
			rcksum1=int(out_msg.split(' ')[0])
			rcksum2=int(out_msg.split(' ')[1])
		if (cksum1 != rcksum1) or (cksum2 != rcksum2):
			t.add_row([f"{spath}/{sname}", cksum1,rcksum1,cksum2,rcksum2])
			if not rep_only:
				print(f"***** Now copying {spath}/{sname}")
				rcmd=f"cp {scr_2_chk} {spath}/bak/{sname}.bak.{datetime.now().strftime('%Y%m%d%H%M%S')}"
				remotecmd=subprocess.Popen(f"ssh -q {hname} '{rcmd}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				(out,err)=remotecmd.communicate()
				retcode=remotecmd.returncode
				if retcode != 0:
					err_msg=err.decode('ascii').rstrip('\n')
					print(f"Backing up script {scr_2_chk} failed due to error - {err_msg}")
					sys.exit(1)
				else:
					remotecmd=subprocess.Popen(f"scp -q {spath}/{sname} {hname}:{scr_2_chk}",shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
					(out,err)=remotecmd.communicate()
					retcode=remotecmd.returncode
					if retcode != 0:
						err_msg=err.decode('ascii').rstrip('\n')
						print(f"SCPing script {scr_2_chk} failed due to error - {err_msg}")
						sys.exit(1)
	print(t)

except Exception as e:
	linenu=sys.exc_info()[-1].tb_lineno
	print(f"Exception - {e} occurred at line {linenu} while Deploying code")