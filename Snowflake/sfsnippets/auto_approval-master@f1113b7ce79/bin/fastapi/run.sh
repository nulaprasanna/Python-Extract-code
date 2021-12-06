#!/bin/bash

export PYTHONPATH="/apps/tools/EDNA_API/auto_approval"  
export ORACLE_HOME=/usr/cisco/packages/oracle/current
export LD_LIBRARY_PATH=/usr/cisco/packages/oracle/current/lib:/usr/cisco/packages/oracle/current/lib:$LD_LIBRARY_PATH
source /apps/tools/EDNA_API/auto_approval_venv/bin/activate
 

cd ../model


result="`python dataaccess_model.py "$1" "$2"`"
echo -e "${result}"

