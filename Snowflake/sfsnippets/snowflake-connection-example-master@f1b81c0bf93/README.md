# Snowflake Connection Example

Connecting to Snowflake is actually complicated to do correctly. Who knew?!

The connection should be performed as follows:
* Check if encrypted, cached private key and passphrase files are available.
    * If missing a file, go fetch it from Keeper.
* Try to connect to Snowflake.
    * If it fails, force a fetch of new Keeper passphrase and private key files.
    * Then, retry to connect.

## Getting Started

Access to the material assets is available from [GIT](https://bitbucket-eng-sjc1.cisco.com/bitbucket/projects/CGW/repos/snowflake-connection-example/browse) and is open to Public.

### Local Asset Delivery

Accessing the project contents to view or make changes may be done through a GIT HTTPS pull.

```sh
$ git clone https://${who am i}@bitbucket-eng-sjc1.cisco.com/bitbucket/scm/cgw/snowflake-connection-example.git
```

### Installing

Accessing the content locally requires installation of a Python virtual environment with dependencies on:
* **hvac** - The Keeper software handling access to secure tokens and secrets.
* **snowflake-connector-python** - The module used to connect and run commands on Snowflake.
* **pyAesCrypt** - The module used to encrypt/decrypt the cached files

## Running

Export your python path (PYTHONPATH) to include all directories and the context root. Create and update the config.ini file from the template to match your configuration. 

Run the ```./bin/connect_to_snowflake_example.py```

## Sample Output

* **Success** - When nothing goes wrong.
```bash
Connected!
Disconnected from database (cisco.us-east-1.snowflakecomputing.com)!
```

* **Missing Files** - When the encrypted files are missing it will report and then create the missing files
```bash
ERROR: Unable to open and collect file contents for (File: ../bin/pkey.aes)
ERROR: [Errno 2] No such file or directory: '../bin/pkey.aes'
ERROR: Unable to open and collect file contents for (File: ../bin/passphrase.aes)
ERROR: [Errno 2] No such file or directory: '../bin/passphrase.aes'
Had to contact keeper for details
Connected!
Disconnected from database (cisco.us-east-1.snowflakecomputing.com)!
``` 
 
* **Bad Passphrase or Key** - When the cached private key or passphrase has expired, a new version will be fetched.
```bash
Unable to connect to (SNOWFLAKE) database!!! ERROR: Bad decrypt. Incorrect password?
Had to contact keeper for details
retry attempt: #1
Connected!
Disconnected from database (cisco.us-east-1.snowflakecomputing.com)!
```