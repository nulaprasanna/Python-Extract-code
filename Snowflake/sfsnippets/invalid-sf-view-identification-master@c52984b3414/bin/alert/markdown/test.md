Hello Snowflake users,
## Snowflake account
* {account}

## Python Run command
* python orphan_parallel.py {account}{database}{schema}{view}{role}

## Results of orphan view detection

* We have identified that there are {badViews} invalid views. Total number of views scanned in this run is {totalViews}. 

## Duration

* The duration of this check is {duration}

## Logs

* The logs are stored in Oracle.
* select * from {table} order by start_time desc 
