# Cisco Auto Reclustering (Cisco Smart Clustering)

The ```cisco-auto-reclustering``` script (branded Cisco Smart Clustering) will determine for a list of tables in a metadata repository what requires re-clustering. It does so on the basis of the associated values.

This is used in replacement of Snowflake's in-built autoclustering due to its additional features. The most appropriate setting will be the average depth and overlaps as a driving mechanism.

## Getting Started

Access to the material assets is available from [GIT](https://bitbucket-eng-sjc1.cisco.com/bitbucket/projects/CGW/repos/cisco-auto-reclustering/browse) and restricted to AD group **Cloud-CGW**.

### Local Asset Delivery

Accessing the project contents to view or make changes may be done through a GIT HTTPS pull.

```sh
$ git clone https://${who am i}@bitbucket-eng-sjc1.cisco.com/bitbucket/scm/cgw/cisco-auto-reclustering.git
```

### Installing

Accessing the content locally requires installation of a Python virtual environment with dependencies on:
* **hvac** - The Keeper software handling access to secure tokens and secrets.
* **snowflake** - The module used to connect and run commands on Snowflake.
* **cx_Oracle** - The module used to connect to Oracle
* **pandas** - Used to manipulate dataframes

(alternatively, use the global anaconda environment)

## Running Tests
###Execution
* **Profiling** - Any object, group, or all may be tested without execution via the executeType argument as PROFILE
* **Clustering** - The recommendation is to run a clustering test using  
### Internal Tests
* **Sanity** - The sanity test attempts to run with no inputs and validate dependency availability
* **Test[1,...n]** -(Awaiting Test Case Development)

## Scripts

### bin/execute_recluster.py
This is the **main** script. It will collect the profile of input tables and recluster the tables if the option is passed. Run the script with -h to see all options.

### bin/metadata/metadata_handler.py
This script handles all interaction with databases. Primarily, it collects metadata, collects $ clustering information, and writes results to metadata.

## Output
### output.log
The output.log file contains the ongoing results of execution of execute_recluster.py. Take note of the IDs found for execution and any errors found inside. The level of logging may be controlled from the config.ini.

```sh
DEBUG:08/25/2019 11:40:21 AM:Detected DB type: ConnectionOracleObject
INFO:08/25/2019 11:40:21 AM:Connecting to (ORACLE)...
INFO:08/25/2019 11:40:23 AM:CONNECTED!
INFO:08/25/2019 11:40:23 AM:Fetching the list of tables for which clustering may be required...
DEBUG:08/25/2019 11:40:25 AM:Table List: [1 2 3]
INFO:08/25/2019 11:40:25 AM:Fetch Complete!
INFO:08/25/2019 11:40:25 AM:Processing all tables fetched.
DEBUG:08/25/2019 11:40:25 AM:Processing table (EDW_OPS_ETL_DB.SS.CJ_POLICY_CLASS).
DEBUG:08/25/2019 11:40:25 AM:Detected DB type: ConnectionSnowflakeObject
INFO:08/25/2019 11:40:25 AM:Connecting to (SNOWFLAKE)...
DEBUG:08/25/2019 11:40:25 AM:Starting new HTTPS connection (1): east.keeper.cisco.com:443
DEBUG:08/25/2019 11:40:25 AM:https://east.keeper.cisco.com:443 "GET /v1/secret/snowflake/prd/platform_ops_svc/key HTTP/1.1" 200 2042
INFO:08/25/2019 11:40:27 AM:CONNECTED!
DEBUG:08/25/2019 11:40:27 AM:Fetching clustering status from Snowflake for (EDW_OPS_ETL_DB.SS.CJ_POLICY_CLASS).
ERROR:08/25/2019 11:40:29 AM:Unable to collect clustering information!!! ERROR: Execution failed on sql '-- Selects the clustering information for the provided input table
select SYSTEM$CLUSTERING_INFORMATION('EDW_OPS_ETL_DB.SS.CJ_POLICY_CLASS');': 000005 (XX000): Invalid clustering keys or table CJ_POLICY_CLASS is not clustered
DEBUG:08/25/2019 11:40:29 AM:Fetched clustering status complete!
WARNING:08/25/2019 11:40:29 AM:Table (EDW_OPS_ETL_DB.SS.CJ_POLICY_CLASS) is not clustered or does not exist. Thus, no action will be taken.
DEBUG:08/25/2019 11:40:29 AM:Processing table (EDW_OPS_ETL_DB.SS.CJ_POLICIES).
DEBUG:08/25/2019 11:40:29 AM:Detected DB type: ConnectionSnowflakeObject
INFO:08/25/2019 11:40:29 AM:Connecting to (SNOWFLAKE)...
DEBUG:08/25/2019 11:40:29 AM:Starting new HTTPS connection (1): east.keeper.cisco.com:443
DEBUG:08/25/2019 11:40:29 AM:https://east.keeper.cisco.com:443 "GET /v1/secret/snowflake/prd/platform_ops_svc/key HTTP/1.1" 200 2042
INFO:08/25/2019 11:40:30 AM:CONNECTED!
DEBUG:08/25/2019 11:40:30 AM:Fetching clustering status from Snowflake for (EDW_OPS_ETL_DB.SS.CJ_POLICIES).
DEBUG:08/25/2019 11:40:30 AM:Fetched clustering status complete!
DEBUG:08/25/2019 11:40:30 AM:Profiling table (EDW_OPS_ETL_DB.SS.CJ_POLICIES) complete.
INFO:08/25/2019 11:40:30 AM:Disconnected from database (cisco.us-east-1.snowflakecomputing.com)!
DEBUG:08/25/2019 11:40:31 AM:Processing table (EDW_OPS_ETL_DB.SS.CJ_POLICIES).
DEBUG:08/25/2019 11:40:31 AM:Detected DB type: ConnectionSnowflakeObject
INFO:08/25/2019 11:40:31 AM:Connecting to (SNOWFLAKE)...
DEBUG:08/25/2019 11:40:31 AM:Starting new HTTPS connection (1): east.keeper.cisco.com:443
DEBUG:08/25/2019 11:40:31 AM:https://east.keeper.cisco.com:443 "GET /v1/secret/snowflake/prd/platform_ops_svc/key HTTP/1.1" 200 2042
INFO:08/25/2019 11:40:32 AM:CONNECTED!
DEBUG:08/25/2019 11:40:32 AM:Fetching clustering status from Snowflake for (EDW_OPS_ETL_DB.SS.CJ_POLICIES).
DEBUG:08/25/2019 11:40:32 AM:Fetched clustering status complete!
DEBUG:08/25/2019 11:40:32 AM:Profiling table (EDW_OPS_ETL_DB.SS.CJ_POLICIES) complete.
INFO:08/25/2019 11:40:32 AM:Disconnected from database (cisco.us-east-1.snowflakecomputing.com)!
INFO:08/25/2019 11:40:35 AM:Disconnected from database (ODSPROD.cisco.com)!
INFO:08/25/2019 11:40:35 AM:Processing for all tables fetched COMPLETE!
```

### profile.csv

The profile.csv file will contain the results of the present run's execution. Previous runs will be overwritten. This content will describe the key attribute values and evaluation of the reclustering decision.

Only objects which exist will be present within the profile.

Contents:
```csv
ID,GROUP_NAME,ACCOUNT,DATABASE,SCHEMA,TABLE,DRIVING MECHANISM,SET TCPC,SET AVG DEPTH,OTHER_DRIVER, SET BOTH CALCULATION,ACTUAL TOTAL PARTITIONS,ACTUAL TCPC, ACTUAL AVERAGE DEPTH,ACTUAL BOTH CALCULATION,WOULD BE CLUSTERED
2,test,CISCO.US-EAST-1,EDW_OPS_ETL_DB,SS,CJ_POLICIES,BOTH,75,31,2.0,0.024193548387096774,1,0.0,1.0,0.0,True
3,test,CISCO.US-EAST-1,EDW_OPS_ETL_DB,SS,CJ_POLICIES,TCPC,101,31,2.0,0.03258064516129032,1,0.0,1.0,0.0,True
```

## Usage

### Execution 

Tables are registered within the driving table specified by the config.ini's DB/Schema into table CISCO_AUTO_RECLUSTER. Once tables are added, they are eligible for execution. Refer to the [driving model](https://bitbucket-eng-sjc1.cisco.com/bitbucket/projects/CGW/repos/cisco-auto-reclustering/browse/bin/models/ClusterRowObject.py) to understand all the options available.

### Usage Flow
To leverage Cisco Smart Clustering this is the flow:
1. A job will run every day to collect all tables which are not present in the driving table (CISCO_AUTO_CLUSTER). Tables found will be inserted into an incremental table CISCO_AUTO_CLUSTER_INC. 
2. If the table is present in the list, it will do nothing. If it is not present it will be added. All tables will have auto recluster revoked.
3. The team who made the table can approach the Performance COE to change the grouping of the tables.
4. The performance COE can also change the configuration from the default value of the driving mechanism to match a more appropriate amount. They may also change the group under which it is executed.
5. Execution of any registered value may be accomplished with the SF re-cluster API hosted from Cloud Toolkit - [SF API](https://gitscm.cisco.com/projects/IT-EDS-OTHERS-SFAPI/repos/sfapi-container/browse). 

## Limitations
The metadata is collected from SNOWFLAKE.ACCOUNT_USAGE views. If a table has a clustering key set and the reconciliation script is run, the metadata may not reflect the true values. There exists a replication delay to the Snowflake ACCOUNT_USAGE schema. Thus, automatic clustering may not be disabled until the subsequent run. 