import os,glob,time
from bs4 import BeautifulSoup
currentDirABSPath=os.path.split(os.path.abspath(__file__))[0]
startTime=time.time();
#Definition execution starts from here when called from
def extractQueries(sourceFolder="SourceXMLs",targetFolder="TargetTXTs2",targetFileExt=".sql"):
    print ("\t\t\t\tFunction execution starts here \n");
    sourceFolderABSPath=os.path.join(currentDirABSPath,sourceFolder);
    targetFolderABSPath=os.path.join(currentDirABSPath,targetFolder);
    try:
        os.mkdir(targetFolderABSPath) #Target folder will be created if not present already
    except:  
        pass
    stringtoGetXMLs=os.path.join(sourceFolderABSPath,"*.xml");
    filesList=glob.glob(stringtoGetXMLs);
    print ("These are the input files: \n");
    print(filesList);
    filecount=0;
    for inputFile in filesList:
        filecount+=1
        print("\n");
        print("No.%d File processing started:"%filecount,os.path.split(inputFile)[1]);
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
            if eachSess['fromtask'] in sessionsDict.keys():
                sessionsDict[eachSess['fromtask']]=sessionsDict[eachSess['fromtask']]+[eachSess['totask']];
            else:
                sessionsDict[eachSess['fromtask']]=[eachSess['totask']];                
        sessLoadOrderList=[]
        def rectree(fromtask):
            for eachtotask in sessionsDict[fromtask]:
                if eachtotask not in sessLoadOrderList:
                    sessLoadOrderList.append(eachtotask);
                    if eachtotask in sessionsDict.keys():                  
                        rectree(eachtotask); #checking recursively for to tasks
                else:
                    sessLoadOrderList.remove(eachtotask);
                    sessLoadOrderList.append(eachtotask);
                    if eachtotask in sessionsDict.keys():                  
                        rectree(eachtotask);
                            
        rectree('Start'); #calling recursive session finder definition assuming that Start is the first ever task in EDW ETLs
        print (sessLoadOrderList);
        
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
                sList.append(tempSrcName)
                sourcesDict[tempSrcName]={}.fromkeys(["Sql Query","Pre SQL","Post SQL"],"")
                instanceContents=instance.find_all("tableattribute",attrs={"name" : ["Sql Query","Pre SQL","Post SQL"]})
                for eachTag in instanceContents:
                    if (eachTag['name']== "Sql Query"):
                        sourcesDict[tempSrcName]["Sql Query"]=eachTag['value'];
                    elif (eachTag['name']== "Pre SQL"):
                        sourcesDict[tempSrcName]["Pre SQL"]=eachTag['value'];
                    elif (eachTag['name']== "Post SQL"):
                        sourcesDict[tempSrcName]["Post SQL"]=eachTag['value'];
            
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
                            
                                          
            print ("Sources List : ",sList);
            print("Targets List : ",tList);
            print("tgtLoadOrdersList : ",tgtLoadOrdersList);
            print("tgtLoadOrdersDict : ",tgtLoadOrdersDict);
    
            print("SQnExpMappingDict : ",SQnExpMappingDict);
            print("ExpnTgtMappingDict : ",ExpnTgtMappingDict);
            print("Tgt2SrcMappingDict : ",Tgt2SrcMappingDict);
            
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
    
                        print("Target%d Name : "%temp,tgtKey);
                        code.write("\n\n\nTarget%d Name : "%temp+tgtKey);
                        #print("Pre SQL : \n",targetsDict[tgtKey]["Pre SQL"]);
                        code.write("\n\n\nPre SQL : \n"+targetsDict[tgtKey]["Pre SQL"].replace("\r",""));
                        #print("Post SQL : \n",(targetsDict[tgtKey]["Post SQL"].rjust(50)))
                        code.write("\n\n\nPost SQL : \n"+targetsDict[tgtKey]["Post SQL"].replace("\r",""));
                    if temp2>1:
                        print("Target%d-%d Name : "%(temp,temp2),tgtKey);
                        #print("Target%d Name : "%temp,tgtKey);
                        code.write("\n\n\nTarget%d-%d Name : "%(temp,temp2)+tgtKey);
                        #print("Pre SQL : \n",targetsDict[tgtKey]["Pre SQL"]);
                        code.write("\n\n\nPre SQL : \n"+targetsDict[tgtKey]["Pre SQL"].replace("\r",""));
                        #print("Post SQL : \n",(targetsDict[tgtKey]["Post SQL"].rjust(50)))
                        code.write("\n\n\nPost SQL : \n"+targetsDict[tgtKey]["Post SQL"].replace("\r",""));
    code.close();
    print ("###############################\n");
    print ("Log : \n\tNo.of Processed Files : %d "%len(filesList));
    print ("Function closed");
    print ("\n\t\t\t\tFunction execution ends here\n");

#Execution starts here
print("""\t\t\t\tWelcome
        This version is tested on Python 3.6
""");
extractQueries()
endTime=time.time()
execTime=endTime-startTime
print("\tExecution Time : %f secs"%execTime);
