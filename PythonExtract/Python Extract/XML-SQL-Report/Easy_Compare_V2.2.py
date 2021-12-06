#################################################################################################
###This script is Dedicated to all who love to code and have laid a hand on it.##################
### Special Thanks to GURUNATH,HARI KISHAN,DINESH,VIDYA,MANJUNATH,HAREESH,SHARMILA,KAVYA     ####
###                     Last Modified By HariKishan(vkoppura)                                ####
###                         Last Modified Date 02-Nov-2017                                   ####
###Updated link for the script: https://github.com/harry1180/python_webscraping##################
#################################################################################################

import os,glob,time
from bs4 import BeautifulSoup
currentDirABSPath=os.path.split(os.path.abspath(__file__))[0]
startTime=time.time();
#Definition execution starts from here when called from
def extractQueries1(sourceFolder="SourceXMLs_env2",targetFolder="TargetTXTs2_env2",targetFileExt=".txt"):
    print ("\t\t\t\tFunction execution starts here \n");
    sourceFolderABSPath=os.path.join(currentDirABSPath,sourceFolder);
    targetFolderABSPath=os.path.join(currentDirABSPath,targetFolder);
    try:
        os.mkdir(targetFolderABSPath) #Target folder will be created if not present already
    except:
        pass
    stringtoGetXMLs=os.path.join(sourceFolderABSPath,"*.xml");
    filesList=glob.glob(stringtoGetXMLs);
    #print ("These are the input files: \n");
    #print(filesList);
    filecount=0;
    for inputFile in filesList:
        filecount+=1
        #print("\n");
        #print("No.%d File processing started:"%filecount,os.path.split(inputFile)[1]);
        soup=BeautifulSoup(open(inputFile),"lxml");
        outputFile=inputFile.split(os.sep)[-1];
        outputFilePath=os.path.join(targetFolderABSPath,outputFile[:-4])+targetFileExt;
        code=open(outputFilePath,"w");
        code.write("ETL Name:\t");
        code.write(outputFile);
        # Sessions #
        sessions=soup.find_all("workflowlink");
        sessionsDict={};
        for eachSess in sessions:
            sessionsDict[eachSess['fromtask']]=eachSess['totask'];
        sessLoadOrderList=[]
        sessLoadOrderList.append(sessionsDict['Start']); #Assuming every ETL in EDW starts with Start task only
        for eachSess in sessionsDict:
            if eachSess !='Start':
                sessLoadOrderList.append(sessionsDict[sessLoadOrderList[len(sessLoadOrderList)-1]]);
        #print (sessLoadOrderList);
        
        #Mappings#
        sessmapcount=0;
        for eachSess in sessLoadOrderList:
            sessmapcount+=1;
            session=soup.find_all("session",attrs={"name":eachSess});
            #session=BeautifulSoup(str(session),'lxml');
            mappingName=session[0]['mappingname'];
            code.write("\n\n\nSession %d: \t"%sessmapcount);
            code.write(eachSess);
            code.write("\nMapping %d: \t"%sessmapcount);
            code.write(mappingName);
            mapping=soup.find_all("mapping",attrs={"name":mappingName});
            #mapping=BeautifulSoup(str(mapping),'lxml');
            code.write(mappingName);
            sub_soup=BeautifulSoup(str(mapping[0])+str(session[0]),"lxml");
            
            tgtLoadOrders=sub_soup.find_all("targetloadorder");
            tgtLoadOrdersList=[None]*len(tgtLoadOrders);
            for eachTag in tgtLoadOrders:
                tgtLoadOrdersList[int(eachTag['order'])-1]=eachTag['targetinstance'];

            tgtLoadOrdersDict={}.fromkeys(range(0,len(tgtLoadOrders)),[]);
            for eachTag in tgtLoadOrders:
                tempList=[]
                tempList.append(eachTag['targetinstance'])
                tgtLoadOrdersDict[int(eachTag['order'])-1]=tgtLoadOrdersDict[int(eachTag['order'])-1]+tempList;
            #print("tgtLoadOrdersDict : ",tgtLoadOrdersDict);
            
            SQnExpMappingDict={}
            SQnExpConnections=sub_soup.find_all("connector",attrs={"frominstancetype":["Source Qualifier"],"toinstancetype":["Expression"]})
            for eachTag in SQnExpConnections:
                if eachTag['toinstance'] not in SQnExpMappingDict.keys():
                    SQnExpMappingDict[eachTag['toinstance']]=eachTag['frominstance'];
            #print("SQnExpMappingDict : ",SQnExpMappingDict)
            
            ExpnTgtMappingDict={}
            ExpnTgtConnections=sub_soup.find_all("connector",attrs={"frominstancetype":["Expression"],"toinstancetype":["Target Definition"]})
            for eachTag in ExpnTgtConnections:
                if eachTag['toinstance'] not in ExpnTgtMappingDict.keys():
                    #ExpnTgtMappingDict[eachTag['toinstance']]=eachTag['frominstance']
                    ExpnTgtMappingDict[eachTag['toinstance']]=SQnExpMappingDict[eachTag['frominstance']] #to map Source Qualifiers with their respective target definitions 
            #print("ExpnTgtMappingDict : ",ExpnTgtMappingDict)    
            
            Tgt2SrcMappingDict={}
            Tgt2SrcMappingDict.update(ExpnTgtMappingDict)
            SQnTgtConnections=sub_soup.find_all("connector",attrs={"frominstancetype":["Source Qualifier"],"toinstancetype":["Target Definition"]})
            for eachTag in SQnTgtConnections:
                if eachTag['toinstance'] not in Tgt2SrcMappingDict.keys():
                    Tgt2SrcMappingDict[eachTag['toinstance']]=eachTag['frominstance']
            #print("Tgt2SrcMappingDict : ",Tgt2SrcMappingDict)
            
            
            sourceContents=sub_soup.find_all("transformation",attrs={"type":["Source Qualifier"]})
            sList=[]
            sourcesDict={}
    
            for instance in sourceContents:
                tempSrcName=instance['name']
                #print(tempSrcName,'kishan')
                sList.append(tempSrcName)
                sourcesDict[tempSrcName]={}.fromkeys(["Sql Query","Pre SQL","Post SQL"],"")
                instanceContents=instance.find_all("tableattribute",attrs={"name" : ["Sql Query","Pre SQL","Post SQL"]})
                for eachTag in instanceContents:
                    if (eachTag['name']== "Sql Query" ):
                        sourcesDict[tempSrcName]["Sql Query"]=(eachTag['value']);
                        #print('harry',sourcesDict[tempSrcName]["Sql Query"])
                    elif (eachTag['name']== "Sql Query"):
                        sourcesDict[tempSrcName]["Sql Query"]=eachTag['value'];
                        #print ("this is sql query",eachTag['value'])
                    elif (eachTag['name']== "Pre SQL"):
                        sourcesDict[tempSrcName]["Pre SQL"]=eachTag['value'];
                    elif (eachTag['name']== "Post SQL"):
                        sourcesDict[tempSrcName]["Post SQL"]=eachTag['value'];
                instanceContentsPartitions=instance.find_all("attribute",attrs={"name" : ["Sql Query","Pre SQL","Post SQL"]})
                """for eachTag in instanceContentsPartitions:
                    if eachTag['PARTITIONNAME'] in instanceContentsPartitions:
                        #print ('query related to partitions',eachTag['value'])
                
                    
                #for eachTag in instanceContentsPartitions:
                    #if (eachTag['name']=="Sql Query" and eachTag['partitionname'].startswith('Partition')):
                        #print("hi")"""
            sourceContents2=sub_soup.find_all("sesstransformationinst",attrs={"transformationtype":["Source Qualifier"]})        
            for instance in sourceContents2:
                tempSrcName=instance['sinstancename']
                if tempSrcName not in sList:
                    sList.append(tempSrcName)
                    sourcesDict[tempSrcName]={}.fromkeys(["Sql Query","Pre SQL","Post SQL"],"")
                instanceContents=instance.find_all("attribute",attrs={"name" : ["Sql Query","Pre SQL","Post SQL"]})
                for eachTag in instanceContents:
                    if (eachTag['name']== "Sql Query"):
                        if len(eachTag['value'])>0:
                            sourcesDict[tempSrcName]["Sql Query"]=eachTag['value'];
                    elif (eachTag['name']== "Pre SQL"):
                        if len(eachTag['value'])>0:
                            sourcesDict[tempSrcName]["Pre SQL"]=eachTag['value'];
                    elif (eachTag['name']== "Post SQL"):
                        if len(eachTag['value'])>0:
                            sourcesDict[tempSrcName]["Post SQL"]=eachTag['value'];
        
            targetContents=sub_soup.find_all("instance",attrs={"transformation_type":["Target Definition"]})
            tList=[]
            targetsDict={}
            for instance in targetContents:
                tempTgtName=instance['transformation_name']
                tList.append(tempTgtName)
                targetsDict[tempTgtName]={}.fromkeys(["Pre SQL","Post SQL"],"")
                instanceContents=instance.find_all("tableattribute",attrs={"name" : ["Pre SQL","Post SQL"]})
                for eachTag in instanceContents:
                    if (eachTag['name']== "Pre SQL"):
                        targetsDict[tempTgtName]["Pre SQL"]=eachTag['value'];
                    elif (eachTag['name']== "Post SQL"):
                        targetsDict[tempTgtName]["Post SQL"]=eachTag['value'];
                        
            targetContents2=sub_soup.find_all("sesstransformationinst",attrs={"transformationtype":["Target Definition"]})

            for instance in targetContents2:
                tempTgtName=instance['sinstancename']
                if tempTgtName not in tList:
                    tList.append(tempTgtName)
                    targetsDict[tempTgtName]={}.fromkeys(["Pre SQL","Post SQL"],"")
                instanceContents=instance.find_all("attribute",attrs={"name" : ["Pre SQL","Post SQL"]})
                for eachTag in instanceContents:
                    if (eachTag['name']== "Pre SQL"):
                        if len(eachTag['value'])>0:
                            targetsDict[tempTgtName]["Pre SQL"]=eachTag['value'];
                    elif (eachTag['name']== "Post SQL"):
                        if len(eachTag['value'])>0:
                            targetsDict[tempTgtName]["Post SQL"]=eachTag['value'];     
                            
                                          
            #print ("Sources List : ",sList);
            #print("Targets List : ",tList);
            #print("tgtLoadOrdersList : ",tgtLoadOrdersList);
            #print("tgtLoadOrdersDict : ",tgtLoadOrdersDict);
    
            #print("SQnExpMappingDict : ",SQnExpMappingDict);
            #print("ExpnTgtMappingDict : ",ExpnTgtMappingDict);
            #print("Tgt2SrcMappingDict : ",Tgt2SrcMappingDict);
            
            temp=0
            for num in tgtLoadOrdersDict.keys():
                temp+=1;
                temp2=0
                for tgtKey in tgtLoadOrdersDict[num]:
                    temp2+=1;
                    srcKey=Tgt2SrcMappingDict[tgtKey]
                    if temp2==1:
                        print("Source%d Name : "%temp,srcKey);
                        code.write("\n\n\nSource%d Name : "%temp+srcKey);
                        #print("Pre SQL : \n",sourcesDict[srcKey]["Pre SQL"]);
                        code.write("\n\n\nPre SQL : \n"+sourcesDict[srcKey]["Pre SQL"].replace("\r",""));
                        #print("SQL Query : \n",sourcesDict[srcKey]["Sql Query"));
                        code.write("\n\n\nSQL Query : \n"+sourcesDict[srcKey]["Sql Query"].replace("\r",""));
                        #print("Post SQL : \n",(sourcesDict[srcKey]["Post SQL"].rjust(50)))
                        code.write("\n\n\nPost SQL : \n"+sourcesDict[srcKey]["Post SQL"].replace("\r",""));
    
                        #print("Target%d Name : "%temp,tgtKey);
                        code.write("\n\n\nTarget%d Name : "%temp+tgtKey);
                        #print("Pre SQL : \n",targetsDict[tgtKey]["Pre SQL"]);
                        code.write("\n\n\nPre SQL : \n"+targetsDict[tgtKey]["Pre SQL"].replace("\r",""));
                        #print("Post SQL : \n",(targetsDict[tgtKey]["Post SQL"].rjust(50)))
                        code.write("\n\n\nPost SQL : \n"+targetsDict[tgtKey]["Post SQL"].replace("\r",""));
                    if temp2>1:
                        #print("Target%d-%d Name : "%(temp,temp2),tgtKey);
                        #print("Target%d Name : "%temp,tgtKey);
                        code.write("\n\n\nTarget%d-%d Name : "%(temp,temp2)+tgtKey);
                        #print("Pre SQL : \n",targetsDict[tgtKey]["Pre SQL"]);
                        code.write("\n\n\nPre SQL : \n"+targetsDict[tgtKey]["Pre SQL"].replace("\r",""));
                        #print("Post SQL : \n",(targetsDict[tgtKey]["Post SQL"].rjust(50)))
                        code.write("\n\n\nPost SQL : \n"+targetsDict[tgtKey]["Post SQL"].replace("\r",""));
    code.close();
    #print ("###############################\n");
    #print ("Log : \n\tNo.of Processed Files : %d "%len(filesList));
    #print ("Function closed");
    #print ("\n\t\t\t\tFunction execution ends here\n");

#Execution starts here
print("""\t\t\t\tWelcome
        This version is tested on Python 3.6
""");
extractQueries1()
#endTime=time.time()
#execTime=endTime-startTime
#print("\tExecution Time : %f secs"%execTime);


#import os,glob,time
#from bs4 import BeautifulSoup
#currentDirABSPath=os.path.split(os.path.abspath(__file__))[0]
#startTime=time.time();
#Definition execution starts from here when called from
def extractQueries2(sourceFolder="SourceXMLs_env1",targetFolder="TargetTXTs2_env1",targetFileExt=".txt"):
    #print ("\t\t\t\tFunction execution starts here \n");
    sourceFolderABSPath=os.path.join(currentDirABSPath,sourceFolder);
    targetFolderABSPath=os.path.join(currentDirABSPath,targetFolder);
    try:
        os.mkdir(targetFolderABSPath) #Target folder will be created if not present already
    except:
        pass
    stringtoGetXMLs=os.path.join(sourceFolderABSPath,"*.xml");
    filesList=glob.glob(stringtoGetXMLs);
    #print ("These are the input files: \n");
    #print(filesList);
    filecount=0;
    for inputFile in filesList:
        filecount+=1
        #print("\n");
        #print("No.%d File processing started:"%filecount,os.path.split(inputFile)[1]);
        soup=BeautifulSoup(open(inputFile),"lxml");
        outputFile=inputFile.split(os.sep)[-1];
        outputFilePath=os.path.join(targetFolderABSPath,outputFile[:-4])+targetFileExt;
        code=open(outputFilePath,"w");
        code.write("ETL Name:\t");
        code.write(outputFile);
        # Sessions #
        sessions=soup.find_all("workflowlink");
        sessionsDict={};
        for eachSess in sessions:
            sessionsDict[eachSess['fromtask']]=eachSess['totask'];
        sessLoadOrderList=[]
        sessLoadOrderList.append(sessionsDict['Start']); #Assuming every ETL in EDW starts with Start task only
        for eachSess in sessionsDict:
            if eachSess !='Start':
                sessLoadOrderList.append(sessionsDict[sessLoadOrderList[len(sessLoadOrderList)-1]]);
        #print (sessLoadOrderList);
        
        #Mappings#
        sessmapcount=0;
        for eachSess in sessLoadOrderList:
            sessmapcount+=1;
            session=soup.find_all("session",attrs={"name":eachSess});
            #session=BeautifulSoup(str(session),'lxml');
            mappingName=session[0]['mappingname'];
            code.write("\n\n\nSession %d: \t"%sessmapcount);
            code.write(eachSess);
            code.write("\nMapping %d: \t"%sessmapcount);
            code.write(mappingName);
            mapping=soup.find_all("mapping",attrs={"name":mappingName});
            #mapping=BeautifulSoup(str(mapping),'lxml');
            code.write(mappingName);
            sub_soup=BeautifulSoup(str(mapping[0])+str(session[0]),"lxml");
            
            tgtLoadOrders=sub_soup.find_all("targetloadorder");
            tgtLoadOrdersList=[None]*len(tgtLoadOrders);
            for eachTag in tgtLoadOrders:
                tgtLoadOrdersList[int(eachTag['order'])-1]=eachTag['targetinstance'];

            tgtLoadOrdersDict={}.fromkeys(range(0,len(tgtLoadOrders)),[]);
            for eachTag in tgtLoadOrders:
                tempList=[]
                tempList.append(eachTag['targetinstance'])
                tgtLoadOrdersDict[int(eachTag['order'])-1]=tgtLoadOrdersDict[int(eachTag['order'])-1]+tempList;
            #print("tgtLoadOrdersDict : ",tgtLoadOrdersDict);
            
            SQnExpMappingDict={}
            SQnExpConnections=sub_soup.find_all("connector",attrs={"frominstancetype":["Source Qualifier"],"toinstancetype":["Expression"]})
            for eachTag in SQnExpConnections:
                if eachTag['toinstance'] not in SQnExpMappingDict.keys():
                    SQnExpMappingDict[eachTag['toinstance']]=eachTag['frominstance'];
            #print("SQnExpMappingDict : ",SQnExpMappingDict)
            
            ExpnTgtMappingDict={}
            ExpnTgtConnections=sub_soup.find_all("connector",attrs={"frominstancetype":["Expression"],"toinstancetype":["Target Definition"]})
            for eachTag in ExpnTgtConnections:
                if eachTag['toinstance'] not in ExpnTgtMappingDict.keys():
                    #ExpnTgtMappingDict[eachTag['toinstance']]=eachTag['frominstance']
                    ExpnTgtMappingDict[eachTag['toinstance']]=SQnExpMappingDict[eachTag['frominstance']] #to map Source Qualifiers with their respective target definitions 
            #print("ExpnTgtMappingDict : ",ExpnTgtMappingDict)    
            
            Tgt2SrcMappingDict={}
            Tgt2SrcMappingDict.update(ExpnTgtMappingDict)
            SQnTgtConnections=sub_soup.find_all("connector",attrs={"frominstancetype":["Source Qualifier"],"toinstancetype":["Target Definition"]})
            for eachTag in SQnTgtConnections:
                if eachTag['toinstance'] not in Tgt2SrcMappingDict.keys():
                    Tgt2SrcMappingDict[eachTag['toinstance']]=eachTag['frominstance']
            #print("Tgt2SrcMappingDict : ",Tgt2SrcMappingDict)
            
            
            sourceContents=sub_soup.find_all("transformation",attrs={"type":["Source Qualifier"]})
            sList=[]
            sourcesDict={}
    
            for instance in sourceContents:
                tempSrcName=instance['name']
                #print(tempSrcName,'kishan')
                sList.append(tempSrcName)
                sourcesDict[tempSrcName]={}.fromkeys(["Sql Query","Pre SQL","Post SQL"],"")
                instanceContents=instance.find_all("tableattribute",attrs={"name" : ["Sql Query","Pre SQL","Post SQL"]})
                for eachTag in instanceContents:
                    if (eachTag['name']== "Sql Query" ):
                        sourcesDict[tempSrcName]["Sql Query"]=(eachTag['value']);
                        #print('harry',sourcesDict[tempSrcName]["Sql Query"])
                    elif (eachTag['name']== "Sql Query"):
                        sourcesDict[tempSrcName]["Sql Query"]=eachTag['value'];
                        #print ("this is sql query",eachTag['value'])
                    elif (eachTag['name']== "Pre SQL"):
                        sourcesDict[tempSrcName]["Pre SQL"]=eachTag['value'];
                    elif (eachTag['name']== "Post SQL"):
                        sourcesDict[tempSrcName]["Post SQL"]=eachTag['value'];
                instanceContentsPartitions=instance.find_all("attribute",attrs={"name" : ["Sql Query","Pre SQL","Post SQL"]})
                """for eachTag in instanceContentsPartitions:
                    if eachTag['PARTITIONNAME'] in instanceContentsPartitions:
                        #print ('query related to partitions',eachTag['value'])
                
                    
                #for eachTag in instanceContentsPartitions:
                    #if (eachTag['name']=="Sql Query" and eachTag['partitionname'].startswith('Partition')):
                        #print("hi")"""
                        
            
            sourceContents2=sub_soup.find_all("sesstransformationinst",attrs={"transformationtype":["Source Qualifier"]})        
            for instance in sourceContents2:
                tempSrcName=instance['sinstancename']
                if tempSrcName not in sList:
                    sList.append(tempSrcName)
                    sourcesDict[tempSrcName]={}.fromkeys(["Sql Query","Pre SQL","Post SQL"],"")
                instanceContents=instance.find_all("attribute",attrs={"name" : ["Sql Query","Pre SQL","Post SQL"]})
                for eachTag in instanceContents:
                    if (eachTag['name']== "Sql Query"):
                        if len(eachTag['value'])>0:
                            sourcesDict[tempSrcName]["Sql Query"]=eachTag['value'];
                    elif (eachTag['name']== "Pre SQL"):
                        if len(eachTag['value'])>0:
                            sourcesDict[tempSrcName]["Pre SQL"]=eachTag['value'];
                    elif (eachTag['name']== "Post SQL"):
                        if len(eachTag['value'])>0:
                            sourcesDict[tempSrcName]["Post SQL"]=eachTag['value'];
        
            targetContents=sub_soup.find_all("instance",attrs={"transformation_type":["Target Definition"]})
            tList=[]
            targetsDict={}
            for instance in targetContents:
                tempTgtName=instance['transformation_name']
                tList.append(tempTgtName)
                targetsDict[tempTgtName]={}.fromkeys(["Pre SQL","Post SQL"],"")
                instanceContents=instance.find_all("tableattribute",attrs={"name" : ["Pre SQL","Post SQL"]})
                for eachTag in instanceContents:
                    if (eachTag['name']== "Pre SQL"):
                        targetsDict[tempTgtName]["Pre SQL"]=eachTag['value'];
                    elif (eachTag['name']== "Post SQL"):
                        targetsDict[tempTgtName]["Post SQL"]=eachTag['value'];
                        
            targetContents2=sub_soup.find_all("sesstransformationinst",attrs={"transformationtype":["Target Definition"]})

            for instance in targetContents2:
                tempTgtName=instance['sinstancename']
                if tempTgtName not in tList:
                    tList.append(tempTgtName)
                    targetsDict[tempTgtName]={}.fromkeys(["Pre SQL","Post SQL"],"")
                instanceContents=instance.find_all("attribute",attrs={"name" : ["Pre SQL","Post SQL"]})
                for eachTag in instanceContents:
                    if (eachTag['name']== "Pre SQL"):
                        if len(eachTag['value'])>0:
                            targetsDict[tempTgtName]["Pre SQL"]=eachTag['value'];
                    elif (eachTag['name']== "Post SQL"):
                        if len(eachTag['value'])>0:
                            targetsDict[tempTgtName]["Post SQL"]=eachTag['value'];     
                            
                                          
            #print ("Sources List : ",sList);
            #print("Targets List : ",tList);
            #print("tgtLoadOrdersList : ",tgtLoadOrdersList);
            #print("tgtLoadOrdersDict : ",tgtLoadOrdersDict);
    
            #print("SQnExpMappingDict : ",SQnExpMappingDict);
            #print("ExpnTgtMappingDict : ",ExpnTgtMappingDict);
            #print("Tgt2SrcMappingDict : ",Tgt2SrcMappingDict);
            
            temp=0
            for num in tgtLoadOrdersDict.keys():
                temp+=1;
                temp2=0
                for tgtKey in tgtLoadOrdersDict[num]:
                    temp2+=1;
                    srcKey=Tgt2SrcMappingDict[tgtKey]
                    if temp2==1:
                        #print("Source%d Name : "%temp,srcKey);
                        code.write("\n\n\nSource%d Name : "%temp+srcKey);
                        #print("Pre SQL : \n",sourcesDict[srcKey]["Pre SQL"]);
                        code.write("\n\n\nPre SQL : \n"+sourcesDict[srcKey]["Pre SQL"].replace("\r",""));
                        #print("SQL Query : \n",sourcesDict[srcKey]["Sql Query"));
                        code.write("\n\n\nSQL Query : \n"+sourcesDict[srcKey]["Sql Query"].replace("\r",""));
                        #print("Post SQL : \n",(sourcesDict[srcKey]["Post SQL"].rjust(50)))
                        code.write("\n\n\nPost SQL : \n"+sourcesDict[srcKey]["Post SQL"].replace("\r",""));
    
                        #print("Target%d Name : "%temp,tgtKey);
                        code.write("\n\n\nTarget%d Name : "%temp+tgtKey);
                        #print("Pre SQL : \n",targetsDict[tgtKey]["Pre SQL"]);
                        code.write("\n\n\nPre SQL : \n"+targetsDict[tgtKey]["Pre SQL"].replace("\r",""));
                        #print("Post SQL : \n",(targetsDict[tgtKey]["Post SQL"].rjust(50)))
                        code.write("\n\n\nPost SQL : \n"+targetsDict[tgtKey]["Post SQL"].replace("\r",""));
                    if temp2>1:
                        #print("Target%d-%d Name : "%(temp,temp2),tgtKey);
                        #print("Target%d Name : "%temp,tgtKey);
                        code.write("\n\n\nTarget%d-%d Name : "%(temp,temp2)+tgtKey);
                        #print("Pre SQL : \n",targetsDict[tgtKey]["Pre SQL"]);
                        code.write("\n\n\nPre SQL : \n"+targetsDict[tgtKey]["Pre SQL"].replace("\r",""));
                        #print("Post SQL : \n",(targetsDict[tgtKey]["Post SQL"].rjust(50)))
                        code.write("\n\n\nPost SQL : \n"+targetsDict[tgtKey]["Post SQL"].replace("\r",""));
    code.close();
    #print ("###############################\n");
    #print ("Log : \n\tNo.of Processed Files : %d "%len(filesList));
    #print ("Function closed");
    #print ("\n\t\t\t\tFunction execution ends here\n");

#Execution starts here
print("""\t\t\t\tWelcome
        This version is tested on Python 3.6
""");
extractQueries2()


"""with open('wf_N_ROL_TRX_LINE_GL_DISTRI.txt', 'r') as file1:
    with open('wf_N_REVENUE_TRANSFER.txt', 'r') as file2:
        same = set(file1).intersection(file2)

same.discard('\n')

with open('some_output_file.txt', 'w') as file_out:
    for line in same:
        file_out.write(line)

import difflib
f=open('wf_N_ROL_TRX_LINE_GL_DISTRI.txt','r')  #open a file
f1=open('wf_N_REVENUE_TRANSFER.txt','r') #open another file to compare
str1=f.read()
str2=f1.read()
str1=str1.split()  #split the words in file by default through the spce
str2=str2.split()
d=difflib.Differ()     # compare and just print
diff=list(d.compare(str2,str1))
with open('some_output_file.txt', 'w') as file_out:
    for line in '\n'.join(diff):
        file_out.write(line)
#print ('\n'.join(diff))"""

import difflib
import sys
import os
import glob
currentDirABSPath=os.path.split(os.path.abspath(__file__))[0]
def extractQueries(sourceFolder="TargetTXTs2_env1",sourceFolder2="TargetTXTs2_env2",targetFolder="HtmlReports",targetFileExt=".html"):
    #print ("\t\t\t\tFunction execution starts here \n");
    sourceFolderABSPath=os.path.join(currentDirABSPath,sourceFolder);
    sourceFolderABSPath2=os.path.join(currentDirABSPath,sourceFolder2);
    targetFolderABSPath=os.path.join(currentDirABSPath,targetFolder);
    #print('this is correct')
    try:
        os.mkdir(targetFolderABSPath) #Target folder will be created if not present already
    except:
        pass
    stringtoGetXMLs=os.path.join(sourceFolderABSPath,"*.txt");
    stringtoGetXMLs2=os.path.join(sourceFolderABSPath2,"*.txt");
    filesList=glob.glob(stringtoGetXMLs);
    filesList2=glob.glob(stringtoGetXMLs2);
    #print ("These are the input files: \n");
    #print(filesList);
    filecount=0;
    for inputFile1,inputFile2 in zip(filesList,filesList2):
        #fromfile = "wf_N_ROL_TRX_LINE_GL_DISTRI.txt"
        #tofile = "wf_N_REVENUE_TRANSFER.txt"
        outputFile=inputFile1.split(os.sep)[-1];
        outputFilePath=os.path.join(targetFolderABSPath,outputFile[:-4])+'report'+targetFileExt;
        #print(outputFilePath)
        #code=open(outputFilePath,"w");
        with open(outputFilePath, 'w') as file_out:
            fromfile = inputFile1
            tofile = inputFile2
            fromlines = open(inputFile1, 'U').readlines()
            
            tolines = open(inputFile2, 'U').readlines()
            diff = difflib.HtmlDiff().make_file(fromlines,tolines,fromfile,tofile)
            for line in diff:
                file_out.write(line)

extractQueries()
endTime=time.time()
execTime=endTime-startTime
print("\tExecution Time : %f secs"%execTime);
#sys.stdout.writelines(diff)
#with open('some_output_file.html', 'w') as file_out:
    #for line in diff:
        #file_out.write(line)

