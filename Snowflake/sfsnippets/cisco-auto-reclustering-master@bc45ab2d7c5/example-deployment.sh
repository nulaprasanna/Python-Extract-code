#!/bin/bash

# branch
BRANCH=master
NAME=cisco-auto-reclustering

# deploy directories
CSC_DEPLOYMENT_DIR=/users/algarbar/cisco_auto_cluster/${NAME}

# repo directories
CSC_REPO_DIR=/users/algarbar/cisco_auto_cluster/REPO/${NAME}

function check_rc() {
    if [ ${1} -ne 0 ]; then
        echo -en "\n${2}\n"
        exit 1
    fi
}

function copy_files() {
    echo -en "Copying bin files to ${1}/bin... "
    cp -R bin/* ${1}/bin
    check_rc $? "ERROR: could NOT copy sh files to ${1}!\n"
    echo -en "COMPLETE!\n"

    echo -en "Copying property files to ${1}/properties... "
    cp -R properties/* ${1}/properties
    check_rc $? "ERROR: could NOT copy property files to ${1}/properties!\n"
    echo -en "COMPLETE!\n"

    echo -en "Copying util files to ${1}/utils... "
    cp -R utils/* ${1}/utils
    check_rc $? "ERROR: could NOT copy lib files to ${1}/lib!\n"
    echo -en "COMPLETE!\n"

}

# build dependent folders
mkdir -p ${CSC_DEPLOYMENT_DIR}/bin
check_rc $? "ERROR: could NOT construct bin deployment folder ${CSC_DEPLOYMENT_DIR}!\n"
mkdir -p ${CSC_DEPLOYMENT_DIR}/logs
check_rc $? "ERROR: could NOT construct logs deployment folder ${CSC_DEPLOYMENT_DIR}!\n"
mkdir -p ${CSC_DEPLOYMENT_DIR}/properties
check_rc $? "ERROR: could NOT construct properties deployment folder ${CSC_DEPLOYMENT_DIR}!\n"
mkdir -p ${CSC_DEPLOYMENT_DIR}/utils
check_rc $? "ERROR: could NOT construct utils deployment folder ${CSC_DEPLOYMENT_DIR}!\n"

# checkout GIT code
cd ${CSC_REPO_DIR}
check_rc $? "ERROR: could NOT cd to ${CSC_REPO_DIR}!\n"
git checkout ${BRANCH}
check_rc $? "ERROR: could NOT check out branch ${BRANCH}!\n"
git pull origin ${BRANCH}
check_rc $? "ERROR: could NOT pull branch ${BRANCH}!\n"


# copy cisco-auto-reclustering to deploy directory
copy_snowflake_operations ${CSC_DEPLOYMENT_DIR}
chmod -R 700 ${CSC_DEPLOYMENT_DIR}
