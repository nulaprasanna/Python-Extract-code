**eDnA Auto Approval**

- eDnA Auto Approval - ML approach to responding to subscription requests.

### Local Asset Delivery

Accessing the project contents to view or make changes may be done through a GIT HTTPS pull.

```sh
$ git clone https://bitbucket-eng-sjc1.cisco.com/bitbucket/scm/cgw/auto_approval.git
```

### Installation and Configuration Setup

Accessing the content locally requires creation of a virtual environment per the provided requirements.txt. Additionally, you will need Oracle installations available.

python : python3 in below case. 

Creating the virtual environment and installing the requirements file.
```bash
$ python -m venv auto_approval_venv
$ source auto_approval_venv/bin/activate
(auto_approval_venv) $ pip install -r auto_approval/requirements.txt
```

Example Oracle reference for CGW hosts:
```sh
$ export ORACLE_HOME=/usr/cisco/packages/oracle/oracle-12.1.0.2/
$ export LD_LIBRARY_PATH=/usr/cisco/packages/oracle/oracle-12.1.0.2/lib:/usr/cisco/packages/oracle/oracle-12.1.0.2/lib
$ export PATH="${ORACLE_HOME}/bin:${LD_LIBRARY_PATH}:$PATH"
```

Update the config.ini file with the oracle DB credentials.
```sh
$ mv auto_approval/properties/config-template.ini auto_approval/properties/config.ini 
```

Create required data directories to store archived pickle files
```sh
$ mkdir auto_approval/archived_models 
```

Export the python path of your virtual python location in order to pickup the config and utilities by the script. 
```sh
$ export PYTHONPATH='/yourhomedirectory/auto_approval'
```

## Running a test to validate

All unit and regression tests should be executed and pass for installation. Individual unit tests may also be executed to confirm a specific functionality.
```sh
$ source /yourhomedirectory/auto_approval/bin/activate && export PYTHONPATH='/yourhomedirectory/auto_approval' && cd /yourhomedirectory/auto_approval/bin/model && python dataaccess_model.py --rebuild Y && deactivate
$ source /yourhomedirectory/auto_approval/bin/activate && export PYTHONPATH='/yourhomedirectory/auto_approval' && cd /yourhomedirectory/auto_approval/bin/model && python dataaccess_model.py --request_data  "{\"FUNCTION_AREA\": \"CXP\",\"DOMAIN_NAME\": \"REF\",\"OBJECT_DB_NAME\": \"EDW_REF_ETL_DB\",\"SCHEMA\" : \"SS\",\"OBJ_NAME\": \"MY_NEW_TABLE\",\"TARGET_ROLE\": \"CX_ROITRIP_BUSINESS_ANALYST_ROLE\",\"IS_RESTRICTED\": 1 ,\"ACTUAL_SANDBOX\": 1,\"REQUESTOR_ORG\": 0,\"HAS_ACCESS_TO_DOMAIN\": 1}" && deactivate
```

## Launching the FastAPI/Uvicorn Service with HTTPS/SSL

Make sure you have the SSL files in the properties folder
```sh
$ source /yourhomedirectory/auto_approval_venv/bin/activate && cd /yourhomedirectory/auto_approval/bin/fastapi && nohup /yourhomedirectory/auto_approval_venv/bin/uvicorn main:app --reload --port <PORT> --host <HOSTNAME> --ssl-keyfile=../../properties/<hostname>.key --ssl-certfile=../../properties/<hostname>.cer &
```

## Launching the FastAPI/Uvicorn Service with HTTP

```sh
$ source /yourhomedirectory/auto_approval_venv/bin/activate && cd /yourhomedirectory/auto_approval/bin/fastapi && nohup /yourhomedirectory/auto_approval_venv/bin/uvicorn main:app --reload --port <PORT> --host <HOSTNAME> &
```

Check nohup.out for the URL to launch the service in the web browser, and for logs

