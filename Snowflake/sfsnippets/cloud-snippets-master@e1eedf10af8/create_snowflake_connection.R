#Example of Connecting to Snowflake

library("RJDBC", "DBI")

#source the config file with the list of config variables
source("config.R")

open_database_connection <- function(driverClassVariable,classPathVariable,JDBCStringVariable,usernameVariable,passwordVariable,databaseVariable,warehouseVariable,schemaVariable){
  #Establish a database connection. Assumes Snowflake.
  #:params: reads directly from config file
  #:return: The database connection object
  
  jdbcDriver <- JDBC(driverClass=driverClassVariable, classPath=classPathVariable)
  jdbcConnection <- dbConnect(jdbcDriver,JDBCStringVariable,usernameVariable,passwordVariable,opts=list(database=databaseVariable, warehouse=warehouseVariable, schema=schemaVariable))
  return(jdbcConnection)
}

commit <- function(conn){
  #Perform commit with query
  result <- dbGetQuery(conn, statement = read_file('query.sql'))
}

close_connection <- function(conn){
  #Close the Snowflake Connection. Attach this method to any finally or exit block.
  close(conn)
}
