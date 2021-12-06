DATA LOADER UTILITY

INSTALLATION
1.  Once this repository is cloned, please create a sub-directory 'data' and edit 'ENTRY_POINT' in the file config/config.ini to point to this sub-directory.
2.  Add .conf file, e.g. ciscodev.us-east-1.conf for Snowflake connectivity.
3.  Rename logs/logs.template into logs/logs.txt
4.  

OVERALL DESCRIPTION
The Data Loader utility supports uploading data from UNIX file system to Snowflake. It does not do any extraction

PROCESS DESCRIPTION
1.  A requestor fills out an Excel-based request and send it to SF Migration Central. This request would contain:
     a name of the requested table, 
     frequency of uploads (every 4 hours, daily, weekly), 
     merge mode – append, overwrite, upsert.

2.  Based on the submitted request, a member of SF Migration Central team has to:
    a.  Create a directory on the gateway server cloudgw-dev-01 where the ingested data will be placed.  The directory should be named exactly like the target table, for example:

        /data/dataloader/ciscodev.us-east-1/edw_di_testing_dv3/ss/action_definition

    , where the requestor can upload the data file ACTION_DEFINITION.txt for ingestion, as well as the DDL for the target table if it does not exist. The DDL contains two SQL statement - CREATE TABLE statement, as well as "GRANT SELECT ON <target_table_name> TO ROLE <requestor_role_name>;".
    The expected directory structure looks as follows:
        ENTRY_POINT stored in config/config.ini, e.g. “/data/dataloader”
        ENTRY_POINT/<SF_acccount>/<target_DB_name>/<target_schema_name>/<target_table_name>
    Once the directory is created, only the requestor and user “cgwedwadm” have access to it

    b.  Once the target directory is created, requestor or SF Migration Central team member uploads the file there.

    c.  Using templates, SF Migration Central team member creates parameter and run.sh files:

        i.  run.sh . This file contains 3 commands - (a) scp dataset from the gateway server into the target directory, (b) changes the current directory to $HOME/data_loader, and (c) runs the utility:
        
            python3 main.py -a <account_name> -t <fully_qualified_table_name>
            python3 main.py -a ciscodev.us-east-1 -t EDW_SERVICE_ETL_DB_DV3.SS.TEST01
            
        ii. <target_table_name>.param . Parameter file is created from a template, which can be found in the data_sample sub-directory.

    d.  Schedule Control-M for regular uploads, or manually launch for ad-hocs by running run.sh:
            Either use sh run.sh, or change privileges for this file to be executable and run it like "./run.sh"


CURRENT LIMITATIONS
1.  The supported file types are limited to CSV and Parquet
2.  Support for parallelization of very large files 1Tb+ will be introduced in the next version
