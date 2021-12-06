import subprocess
import os
from subprocess import *
import sys
import platform
import getpass
import time

#global variables declarations
currentDir=''
pmrepPath=''
domainFile=''

Exec_start_Time=time.time();

#Function to connect to the repository expects name of repository , domain and user details
def connect_to_repo(Repository,Domain,User):
        print('Enter details for connection')
        b = input("User ID:")
        c = getpass.getpass()
        Connection_start_Time=time.time();
        RepoCommand='pmrep connect -r EDW_TS3_TEST_101 -d Domain_EDW_TEST_101 -s EDW_USER_DOMAIN -n '+b+' -x '+c+''
        RepoCommand=RepoCommand.rstrip()
        p=subprocess.Popen(RepoCommand,stderr=subprocess.PIPE,stdin=subprocess.PIPE,stdout=subprocess.PIPE,shell=True)
        out,err=p.communicate()
        if p.returncode or err:
                print ("\n\n ERROR : Connection Failed !!!") #
                sys.stdout.flush()
                sys.stdin.flush()
        else:
                Connection_End_Time=time.time()
                Total_Connection_Time=Connection_End_Time-Connection_start_Time
                print ("Connection Successful !!!")
                print("\n\nTotal Time Taken to Connect: %f secs"%Total_Connection_Time);
                sys.stdout.flush()
                sys.stdin.flush()
        return p.returncode
#END OF FUNCTION connect_to_repo



#executes any os command, here provided pre defined commands
def execute_pmrep_command(command,output_file_name):
        out=open(output_file_name,'w')
        return_code=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,shell=True)
        output,error=return_code.communicate()
        a=output.strip()
        repr(a)
        out.write('a = '+repr(a)+'\n')
        out.close()
        return
#END OF FUNCTION execute_pmrep_command


#function to make output dirs , create if not exist
def create_output_directories():
        if platForm == 'Windows':
                try:
                        os.makedirs(currentDir.strip()+"\LogFiles")
                        print ("Log files generated and stored in 'LogFiles' directory") #
                except OSError:
                        print ("Log files generated and stored in 'LogFiles' directory") #
                        pass
        elif platForm == 'Linux':
                try:
                        os.makedirs(currentDir.strip()+"/LogFiles")
                        print ("Log files generated and stored in 'LogFiles' directory") #
                except OSError:
                        print ("Log files generated and stored in 'LogFiles' directory") #
                        pass
#END OF FUNCTION create_output_dirs
                


platForm=platform.system()
currentDir=os.getcwd()
pmrepPath=r'C:\Informatica\10.1.0\clients\PowerCenterClient\client\bin'
print(pmrepPath)
a=currentDir.strip()+"\Repo_Domain_User.param"
domainFile = a

#start of the main program body
if platform.system()=='Windows':
          os.system('cls')
elif platform.system()=='Linux':
          os.system('clear')
print ("Checking for all necessary environment variables ....\n")
#check_platform()
print ("\n\n***********************************************************************\n\n")
lines=open(domainFile,'r').readlines()
os.chdir(pmrepPath)
if platForm == 'Windows':
        logFileLoc=currentDir.strip()+"\\LogFiles\\"
elif platForm == 'Linux':
        logFileLoc=currentDir.strip()+"/LogFiles/"

for line in lines:
        fields=line.split(',')
        Repository=fields[0]
        Domain=fields[1]
        User=fields[2]
        return_code=connect_to_repo(Repository,Domain,User)
        print ("\n***********************************INPUT*******************************\n\n")
        if return_code:
                continue
        controlFile="controlFile.txt"
        if platForm == 'Windows':
                controlFile=currentDir.strip()+"\\"+controlFile.strip()
        elif platForm == 'Linux':
                controlFile=currentDir.strip()+"/"+controlFile.strip()

        cLines=open(controlFile,'r').readlines()
        #print(cLines)
        open(controlFile,'w').writelines(cLines)
        fileName="import_list.txt"
        print ("\n\n***********************************OUTPUT******************************\n\n")
        create_output_directories()
        
        if platForm == 'Windows':
                fileName=currentDir.strip()+"\\"+fileName.strip()
        elif platForm == 'Linux':
                fileName=currentDir.strip()+"/"+fileName.strip()


        iLines=open(fileName,'r').readlines()
        iLines=iLines[0:]
        for line in iLines:
            fields=line.split(',')
            folderName=fields[0]
            objectName=fields[1]
            impFile=objectName.strip()+".xml"
            ETLName=objectName

            if platForm == 'Windows':
                objectName=currentDir.strip()+"\Imported_XMLs\\"+objectName.strip()
            elif platForm == 'Linux':
                objectName=currentDir.strip()+"/"+objectName.strip()
            XLines=open(objectName.strip()+".xml",'r').readlines()
            open(impFile,'w').writelines(XLines)
            import_command="pmrep objectimport -i "+impFile.strip()+" -c "+controlFile
            #print(import_command)
            logFileLocstr=logFileLoc.strip()+ETLName.strip()+".txt"
            activity = impFile.strip()+" to " +folderName.strip()
            Import_start_Time=time.time();
            print('\nImporting ' +activity)
            execute_pmrep_command(import_command,logFileLocstr)
            Import_End_Time=time.time();
            Import_Time=Import_End_Time-Import_start_Time
            print("Import time: %f secs"%Import_Time);
            print("\n\n")

Exec_End_Time=time.time();
Total_Exec_time=Exec_End_Time-Exec_start_Time                
print("\nScript Execuion Finished in: %f secs"%Total_Exec_time);
print ("\n\n\t\t*******Exiting*********\n\n")
time.sleep(10)

print ("\n\nImport operation completed, \n\nFor more details please refer to LogFiles directory.\n\n***********************************************************************")
#END PROGRAM
