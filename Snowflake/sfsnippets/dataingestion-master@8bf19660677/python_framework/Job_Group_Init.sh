#!/bin/bash
#set -x
###==============================================================================================================
#@(#) Object                : Job_Group_Init.sh
#@(#) Description           : This script takes JOB_GROUP_NAME as parameter
#@(#)                         and initialize all job belongs to the group
#@(#)                         1. Check all directory (log, Archive, Data) available, if not available create
#@(#)                         2. Talend Job to check the status of Previous Job Group Run
#@(#)                         3. Talend Job to update the status in Job Group table
#@(#)                         4. Talend Job to update the status of Job Stream Run table
#@(#) Author                : Vidhya Sriranganayagi
#@(#) Creation date         : 03/19/2019
#@(#) Expiration date       : <mm/dd/yyyy>
#@(#) modification history  : <mm/dd/yyyy>, <name>
#@(#)
###==============================================================================================================
#Set Process Name
echo "#################################################################################################################################################################"
### print the args passed to the script
echo "Running Script : ${0}"
echo "The arguments passed to this script are : $*"

v_CURRENT_SHELL_SCRIPT=${0}
v_PROCESS_NAME="JOB_GROUP_INIT_TALEND"

if [ $# -lt 1 ]
then
	echo "Missing Parameter Job Group Name"
	exit 1
else
	export v_JOB_GROUP_NAME=${1}
fi

export v_PROCESS_START_DTM=`date +%Y%m%d%H%M%S`

### Initialize environment
#export ENV_FILE_PATH="/apps/informatica/scripts/talend/scripts/config"
export ENV_FILE_PATH="/apps/edwsfdata/talend/scripts/config"
#export ENV_FILE_PATH="/Users/prpanda/Documents/Cisco/Workspace/ETL/snowflake/talend/scripts/config"
. ${ENV_FILE_PATH}/JC_env.sh
#. ${ENV_FILE_PATH}/JC_RUN_ENV.sh
. ${SHELL_SCRIPT_PATH}/SOURCE_FUNCTIONS.sh

echo "Check existence of run folders for the job group ${v_JOB_GROUP_NAME}, if not create the folders"
CHECK_FOLDER "${v_JOB_GROUP_NAME}"
ReturnCode=${?}
TEST_ERROR ${ReturnCode} "Current Script:${v_CURRENT_SHELL_SCRIPT}: Check existence of run folders for the job group" ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME}

export Job_Initialization_Param="${USER_CONFIG_PATH}/Job_Group_Init_Param_${v_JOB_GROUP_NAME}.param"


echo "RUN_ENV;${RUN_ENVIRONMENT}" > ${Job_Initialization_Param}
echo "job_group_id;${v_JOB_GROUP_NAME}" >> ${Job_Initialization_Param}
echo "param_file_path;${USER_CONFIG_PATH}" >> ${Job_Initialization_Param}
echo "param_creation_time;${v_PROCESS_START_DTM}" >> ${Job_Initialization_Param}
echo "ENVIRONMENT_NAME;${RUN_ENVIRONMENT}" >> ${Job_Initialization_Param}
echo "JC_ENV_PATH;${JC_ENV_PATH}" >> ${Job_Initialization_Param}
echo "JC_LOGON_FILE;${JC_LOGON_FILE}" >> ${Job_Initialization_Param}



### Variables
###========
### Define local variables ( Capital letters for beginning of each syllable)
ProcessStartDTM=`date +%Y-%m-%dT%H.%M.%S`
Pid="[${$}]"
echo "The PID of this script for ${v_PROCESS_NAME} is: ${Pid}"

### Number of steps of this script.
NbTotalSteps=3
CntStep=1

### STEP functions
###===========

#STEP1 ()
### object: Check all folders are existing for the job group
### If does not exists then create
###
#{
#	SET_CHECKPOINT "${v_PROCESS_NAME}" ${CntStep} "${v_JOB_GROUP_NAME}"
#	echo "Check existence of run folders for the job group ${v_JOB_GROUP_NAME}, if not create the folders"
#	CHECK_FOLDER "${v_JOB_GROUP_NAME}"
#	ReturnCode=${?}
#	TEST_ERROR ${ReturnCode} "Current Script:${v_CURRENT_SHELL_SCRIPT}: Check existence of run folders for the job group" ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME}
#}

STEP1 ()
#--Call Talend job to check the status of the previous run 
###
{
	echo "Talend Job Execution to check the status of the previous run"
	SET_CHECKPOINT ${v_PROCESS_NAME} ${CntStep} ${v_JOB_GROUP_NAME} 
	export v_TALEND_LOG="${LOG_FILE_PATH}/${v_JOB_GROUP_NAME}_PREV_RUN_CHK_GROUP_${v_PROCESS_START_DTM}.log"
	sh /$v_TALEND_SCRIPT_PATH/GET_JOB_RUN_STATUS/GET_JOB_RUN_STATUS_run.sh --context_param paramfile=${Job_Initialization_Param} > ${v_TALEND_LOG} 2>&1
	ReturnCode=$?
	TEST_ERROR ${ReturnCode} "Current Script:${v_CURRENT_SHELL_SCRIPT}:  Talend Job Execution to check the status in EDW_JOB_GROUP table :${v_JOB_GROUP_NAME},${v_PROCESS_NAME} ${v_JOB_GROUP_NAME}  :${v_JOB_GROUP_NAME}" ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME}
}


STEP2 ()
#--Call Talend job to update the status and insert into group run table 
###
{
	echo "Talend Job to update the status and insert into group run table"
	SET_CHECKPOINT ${v_PROCESS_NAME} ${CntStep} ${v_JOB_GROUP_NAME} 
	. ${USER_CONFIG_PATH}/${RUN_ENVIRONMENT}_${v_JOB_GROUP_NAME}_GROUP_EXISITENCE.sh
	. ${USER_CONFIG_PATH}/${RUN_ENVIRONMENT}_${v_JOB_GROUP_NAME}_NON_COMPLETE_JOB_GROUP_RECORD_COUNT.sh	
	if [ "$JOB_GROUP_ID_COUNT" = "0" ]  && [ "$JOB_GROUP_RECORD_COUNT" != "0" ]
    then
    echo "There are ${JOB_GROUP_RECORD_COUNT} incomplete jobs under group ${v_JOB_GROUP_NAME}"
    exit 1
    fi
	export v_TALEND_LOG="${LOG_FILE_PATH}/${v_JOB_GROUP_NAME}_JOB_GROUP_INIT_${v_PROCESS_START_DTM}.log"
	sh /$v_TALEND_SCRIPT_PATH/Job_Group_Initialization/Job_Group_Initialization_run.sh --context_param paramfile=${Job_Initialization_Param} > ${v_TALEND_LOG} 2>&1
	ReturnCode=$?
	TEST_ERROR ${ReturnCode} "Current Script:${v_CURRENT_SHELL_SCRIPT}:  Talend Job Execution to Update the run status to R in EDW_JOB_GROUP table :${v_JOB_GROUP_NAME},${v_PROCESS_NAME} ${v_JOB_GROUP_NAME}  :${v_JOB_GROUP_NAME}" ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME}
}

STEP3 ()
#--Call Talend job to update the status in job_stream table 
###
{
	echo "Talend Job to update the the status in job_stream table"
	SET_CHECKPOINT ${v_PROCESS_NAME} ${CntStep} ${v_JOB_GROUP_NAME} 
	. ${USER_CONFIG_PATH}/${RUN_ENVIRONMENT}_${v_JOB_GROUP_NAME}_NON_COMPLETE_JOB_STREAM_RECORD_COUNT.sh	
	if [ "$JOB_STREAM_RECORD_COUNT" != "0" ]
    then
    echo "There are ${JOB_STREAM_RECORD_COUNT} incomplete jobs under group ${v_JOB_GROUP_NAME}"
    exit 1
    fi
	export v_TALEND_LOG="${LOG_FILE_PATH}/${v_JOB_GROUP_NAME}_JOB_STREAM_INIT_${v_PROCESS_START_DTM}.log"
	sh /$v_TALEND_SCRIPT_PATH/Job_Stream_Initialization/Job_Stream_Initialization_run.sh --context_param paramfile=${Job_Initialization_Param} > ${v_TALEND_LOG} 2>&1
	ReturnCode=$?
	TEST_ERROR ${ReturnCode} "Current Script:${v_CURRENT_SHELL_SCRIPT}:  Talend Job Execution to Update the run status to P in EDW_JOB_STREAMS table :${v_JOB_GROUP_NAME},${v_PROCESS_NAME} ${v_JOB_GROUP_NAME}  :${v_JOB_GROUP_NAME}" ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME}
}


### MAIN
###====
if [ ! -d ${RUN_PATH}/"${v_JOB_GROUP_NAME}"/checkpoint ]
then
	mkdir -p ${RUN_PATH}/"${v_JOB_GROUP_NAME}"/checkpoint
	CntStep=${MAX_STEP_NUMBER}
	SET_CHECKPOINT ${v_PROCESS_NAME} ${CntStep} ${v_JOB_GROUP_NAME} 
	ReturnCode=$?
	TEST_ERROR ${ReturnCode} "Current Script:${v_CURRENT_SHELL_SCRIPT}: Initialization of checkpoint for new job group ${v_JOB_GROUP_NAME}" ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME}
fi

#Check existence of checkpoint file if does not exist then create with step ${MAX_STEP_NUMBER}
if [ ! -f "${RUN_PATH}/${v_JOB_GROUP_NAME}/checkpoint/${v_CHECKPOINT_FILE}" ]
then
	CntStep=${MAX_STEP_NUMBER}
	SET_CHECKPOINT ${v_PROCESS_NAME} ${CntStep} ${v_JOB_GROUP_NAME}  
	ReturnCode=$?
	TEST_ERROR ${ReturnCode} "Current Script:${v_CURRENT_SHELL_SCRIPT}: Initialization of checkpoint for new job group ${v_JOB_GROUP_NAME} and Job: " ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME} 
fi

GET_CHECKPOINT ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME} 
CntStep=${STEP_NUMBER}

if [ ${STEP_NUMBER} -eq ${MAX_STEP_NUMBER} ]
then
	### If the Step number is max step number then initialize the step number to 1
	CntStep=1
	SET_CHECKPOINT ${v_PROCESS_NAME} ${CntStep} ${v_JOB_GROUP_NAME}
	ReturnCode=$?
	TEST_ERROR ${ReturnCode} "Current Script:${v_CURRENT_SHELL_SCRIPT}: Initialization of checkpoint for job group ${v_JOB_GROUP_NAME}" ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME} 
	GET_CHECKPOINT ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME} 
	ReturnCode=$?
	TEST_ERROR ${ReturnCode} "Current Script:${v_CURRENT_SHELL_SCRIPT}: Initial reading of checkpoint for job group ${v_JOB_GROUP_NAME}" ${v_PROCESS_NAME} ${v_JOB_GROUP_NAME} 
fi

while  [[ ${CntStep} -le ${NbTotalSteps} ]] && [[ ${ReturnCode} -eq 0 ]]
do
    echo "==============================================================================="
    echo "Start running Step-${CntStep}"
    echo "==============================================================================="
	echo "Step : STEP${CntStep}"
    STEP${CntStep}
    let "CntStep=CntStep+1"
done

echo "Set Job Group initialization Steps to ${MAX_STEP_NUMBER}"
CntStep=${MAX_STEP_NUMBER}
SET_CHECKPOINT ${v_PROCESS_NAME} ${CntStep} ${v_JOB_GROUP_NAME} 
ReturnCode=${?}
echo "ReturnCode=${ReturnCode}"
echo "Current Script:${v_CURRENT_SHELL_SCRIPT}: Job Group Initialization completed successfully ${v_JOB_GROUP_NAME} "
SEND_MESSAGE "The Job Group Initialization is complete for the  ${v_JOB_GROUP_NAME}" "Current Script:${v_CURRENT_SHELL_SCRIPT}: All the steps for job group initialization is complete for the job group ${v_JOB_GROUP_NAME} " ${v_JOB_GROUP_NAME} 
echo "#################################################################################################################################################################"
