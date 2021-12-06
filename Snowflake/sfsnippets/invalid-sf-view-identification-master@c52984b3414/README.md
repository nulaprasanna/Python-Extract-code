# Invalid Snowflake View Identification

This utility will traverse all views within the provided snowflake account, checking if they are orphaned. Orphaned views will result in errors when select queries are fired upon them. Thus, it is in our interest to proactively find and correct them.

### Local Asset Delivery

Accessing the project contents to view or make changes may be done through a GIT HTTPS pull.

```sh
$ git clone https://${who am i}@bitbucket-eng-sjc1.cisco.com/bitbucket/scm/cgw/
```

### Installing

Clone the repo. Create a virtual environment whose components include the modules of requirement.txt. Ensure permissions are set within Snowflake for universal table read access and access to SNOWFLAKE.ACCOUNT_USAGE.VIEWS.

## Execution

Configure a file entitled ```config.ini``` within the properties folder as per the template. Change the values to match destination targets. Execute the file ```sequential_identification.py``` with argument **--account**.

For example:
```bash
python sequential_identification.py --account CISCODEV.US-EAST-1
```

## Release Schedule

### R 1.0 (Deployed)
This release is slow and of limited functionality. It performs checks sequentially. 

### R 1.1 (Planned)
This release will involve the creation of task hierarchies. Each view will be assigned to a single task. Execution of these tasks will proceed simultaneously.


## Sample Output

```bash
INFO:01/29/2020 10:53:13 AM:-----------------------------------
INFO:01/29/2020 10:53:13 AM:Beginning fetch of views for Snowflake account: CISCODEV.US-EAST-1
INFO:01/29/2020 10:53:13 AM:Connecting to (SNOWFLAKE)...
INFO:01/29/2020 10:53:14 AM:CONNECTED!
INFO:01/29/2020 10:53:14 AM:Collecting all views from the source...
INFO:01/29/2020 10:53:25 AM:COMPLETE: Collected View list.
INFO:01/29/2020 10:53:26 AM:STATUS of check on Account: ciscodev - (0/14223)
INFO:01/29/2020 10:54:29 AM:STATUS of check on Account: ciscodev - (250/14223)
...
INFO:01/29/2020 10:55:59 AM:COMPLETED checks of views on account (ciscodev). Good: 12458, Bad: 1765
INFO:01/29/2020 10:55:59 AM:Attempting to load bad views batch to (ciscodev)... 
INFO:01/29/2020 10:56:01 AM:COMPLETE! Loaded bad views batch to (ciscodev).
```
