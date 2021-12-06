#!/bin/bash

# branch
BRANCH=master
NAME=dataingestion

# deploy directories
CLOUDCGW_DI_DEPLOY_DIR=/home/edwuser

# repo directories
CLOUDCGW_DI_DIR_REPO_DIR=/home/edwuser/REPO/${NAME}

function check_rc() {
    if [ ${1} -ne 0 ]; then
                echo -en "\n${2}\n"
                exit 1
    fi
}

function copy_example_scripts() {
    echo -en "Copying Example files to ${1}... "
    cp -R talend/* ${1}
    check_rc $? "ERROR: could NOT copy to ${1}!\n"
    echo -en "COMPLETE!\n"
}

# build dependent folders
mkdir -p ${CLOUDCGW_DI_DEPLOY_DIR}/talend
check_rc $? "ERROR: could NOT construct deployment folder ${CLOUDCGW_SCRIPTS_DIR}/talend!\n"

# build hdpcgw-scripts
cd ${CLOUDCGW_DI_DIR_REPO_DIR}
check_rc $? "ERROR: could NOT cd to ${CLOUDCGW_DI_DIR_REPO_DIR}!\n"
git checkout ${BRANCH}
check_rc $? "ERROR: could NOT check out branch ${BRANCH}!\n"
git pull origin ${BRANCH}
check_rc $? "ERROR: could NOT pull branch ${BRANCH}!\n"

copy_example_scripts ${CLOUDCGW_DI_DEPLOY_DIR}/talend
# chmod -R 755 ${CLOUDCGW_DI_DEPLOY_DIR}/talend
