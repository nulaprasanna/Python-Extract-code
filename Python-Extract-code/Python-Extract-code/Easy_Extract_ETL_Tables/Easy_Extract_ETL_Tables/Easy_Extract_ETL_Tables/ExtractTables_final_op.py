#Author:Dinesh Bommisetty
#Last Modified By: Dinesh Bommisetty
#Last Modified Date: 9th Feb, 2019
#This is an enhancement to earlier developed extract queries scripts

import glob
import csv
import os
#from glob import glob
try:
    os.remove('Extracted_Tables_List.csv')
except OSError:
    pass
currentDirABSPath=os.path.split(os.path.abspath(__file__))[0]
targetFolder="TargetFilesWithTables"
targetFolderABSPath=os.path.join(currentDirABSPath,targetFolder);
stringtoGetCSVs=os.path.join(targetFolderABSPath,"*.csv");
filesList=glob.glob(stringtoGetCSVs);
with open('Extracted_Tables_List.csv', 'a') as singleFile:
    code=open('Extracted_Tables_List.csv',"w")
    code.write("ETL Name,Session Name,Session Seq,Mapping Name,Mapping Seq,Source Name,Source Seq,Target Name,Target Seq,Query Type,Tables Referred\n");
    code.close()
    for csv in filesList:
        if csv == 'Extracted_Tables_List.csv':
            pass
        else:
            for line in open(csv, 'r'):
                singleFile.write(line)
