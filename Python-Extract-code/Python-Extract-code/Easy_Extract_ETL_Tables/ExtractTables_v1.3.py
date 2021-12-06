#Author:Dinesh Bommisetty
#Last Modified By: Dinesh Bommisetty
#Last Modified Date: 9th Feb, 2019
#This is an enhancement to earlier developed extract queries scripts
import time
startTime=time.time();
import ExtractTables_each_file
import ExtractTables_final_op

endTime=time.time()
TotalexecTime=endTime-startTime
print("\tTotal Time elapsed: %f secs"%TotalexecTime);
