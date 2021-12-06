# DI Audits

## Getting Started
Access to the material assets is available from [GIT](https://bitbucket-eng-sjc1.cisco.com/bitbucket/projects/CGW/repos/di-audits/browse) and is unrestricted.

### Local Asset Delivery

Accessing the project contents to view or make changes may be done through a GIT HTTPS pull.

```sh
$ git clone https://bitbucket-eng-sjc1.cisco.com/bitbucket/scm/cgw/di-audits.git
$ cd di-audits
$ mkdir bin/logs
$ chmod -R 700 *
$ properties/config.ini # Place config details here
```

## TL;DR Functionality
This package exposes common auditing functionalities as python methods. It also ties to the EDS metadata repo to enrich user requests

Users can build custom auditing scripts using these functions.

## Test

All unit and regression tests should be executed and pass for consideration of installation. Individual unit tests may also be executed to confirm a specific functionality. These can be developed per a user requirement

```sh
$ # Export all required path variables (such as Oracle)
$ export ORACLE_HOME=/usr/cisco/packages/oracle/current
$ export LD_LIBRARY_PATH=/usr/cisco/packages/oracle/current/lib
$ export PATH="${ORACLE_HOME}/bin:${LD_LIBRARY_PATH}:$PATH"
$ python example-all-databases.py
```

### Unit Test Purposes

* **example.py** - This script functions both as a test as well as an instruction manual of sorts to confirm each possible audit functionality.

### Assumptions
*  Availability of Python 3 and modules
    * cx_oracle
    * pandas
    * hvac
    * snowflake
    * jaydebeapi (or teradata)
* Oracle Installation
* Limited write space
* In Cisco VPN 
