import os,glob
from bs4 import BeautifulSoup
currentDirABSPath=os.path.split(os.path.abspath(__file__))[0]
def extractQueries(sourceFolder="SourceXMLs",targetFolder="TargetTXTs2",targetFileExt=".sql"):
    sourceFolderABSPath=os.path.join(currentDirABSPath,sourceFolder);
    print(os.path.join(currentDirABSPath,sourceFolder));
    targetFolderABSPath=os.path.join(currentDirABSPath,targetFolder);
    print("ABS1",sourceFolderABSPath);
    print("ABS2",targetFolderABSPath);
    try:
        os.mkdir(targetFolderABSPath)
    except:
        pass
    stringtoGetXMLs=os.path.join(sourceFolderABSPath,"*.xml")
    filesList=glob.glob(stringtoGetXMLs);
    inputFile=filesList[0];
    print(inputFile);
    print("Filename:",os.path.split(inputFile)[1]);
    soup=BeautifulSoup(open(inputFile),"lxml");
    outputFile=inputFile.split(os.sep)[-1];
    outputFilePath=os.path.join(targetFolderABSPath,outputFile[:-4])+targetFileExt;
    code=open(outputFilePath,"w");
    code.write("ETL Name : "+outputFile);
    
    mappings=soup.find_all("mapping");
    mapcount=0;
    eachMapping=mappings[1];
    mapcount=mapcount+1;
    code.write("\n\nMapping%d Name : "%mapcount);
    code.write(eachMapping['name']);
    tgtLoadOrders=eachMapping.find_all("targetloadorder")
    tgtLoadOrdersList=[None]*len(tgtLoadOrders)
    for eachTag in tgtLoadOrders:
        tgtLoadOrdersList[int(eachTag['order'])-1]=eachTag['targetinstance']
    #print("tgtLoadOrdersList : ",tgtLoadOrdersList)

    
    tgtLoadOrdersDict={}.fromkeys(range(0,len(tgtLoadOrders)),[])
    #print("tgtLoadOrdersDict : ",tgtLoadOrdersDict)
    for eachTag in tgtLoadOrders:
        tempList=[]
        tempList.append(eachTag['targetinstance'])
        tgtLoadOrdersDict[int(eachTag['order'])-1]=tgtLoadOrdersDict[int(eachTag['order'])-1]+tempList
    #print("tgtLoadOrdersDict : ",tgtLoadOrdersDict)
    
    
    Tgt2SrcMappingDict={}
    #Tgt2SrcMappingDict.update(ExpnTgtMappingDict)
    SQnTgtConnections=eachMapping.find_all("connector",attrs={"frominstancetype":["Source Qualifier"],"toinstancetype":["Target Definition"]})
    for eachTag in SQnTgtConnections:
        if eachTag['toinstance'] not in Tgt2SrcMappingDict.keys():
            Tgt2SrcMappingDict[eachTag['toinstance']]=eachTag['frominstance']
        #print("Tgt2SrcMappingDict : ",Tgt2SrcMappingDict)
        



    sourceContents=eachMapping.find_all("transformation",attrs={"type":["Source Qualifier"]})
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


    sourceContents2=eachMapping.find_all("sesstransformationinst",attrs={"transformationtype":["Source Qualifier"]})        
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

    targetContents=eachMapping.find_all("instance",attrs={"transformation_type":["Target Definition"]})
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
    

    targetContents2=eachMapping.find_all("sesstransformationinst",attrs={"transformationtype":["Target Definition"]})
    
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

    print "Loop completed------------------------------";
    code.close();
    print ("###############################\n");
    print ("Log : \n\tNo.of Processed Files : %d "%len(filesList));
        
    
    
print("""\t\t\t\tWelcome
        This version is tested on Python 3.6
""");
extractQueries()