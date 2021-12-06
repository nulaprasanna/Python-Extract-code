
/*Database Scripts*/


/*Create Table Using Script*/
CREATE TABLE "TRAININGDB"."SALES"."CUSTOMER_SCRIPT" (
  "C_CUSTKEY" NUMBER (38, 0) NOT NULL, 
  "C_NAME" VARCHAR (25) NOT NULL, 
  "C_ADDRESS" VARCHAR (40) NOT NULL, 
  "C_NATIONKEY" NUMBER (38, 0) NOT NULL, 
  "C_PHONE" VARCHAR (15) NOT NULL, 
  "C_ACCTBAL" NUMBER (12, 0) NOT NULL, 
  "C_MKTSEGMENT" VARCHAR (10), 
  "C_COMMENT" VARCHAR (117)
) 


/*Load your Customer_data.csv in the staged area of your cloud provider*/

/*Select from Stage */
select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8
  from @SNOWFLAKE_Stage/Customer_data.csv t;

/*Copy into CUSTOMER table create Manually.*/ 
copy into "TRAININGDB"."SALES"."CUSTOMER"
select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8
  from @SNOWFLAKE_Stage/Customer_data.csv (file_format => "CSV_FILE") t;

/*Check to see if Data was copied*/
select * from "TRAININGDB"."SALES"."CUSTOMER"
  
/*Create Table AS with Stage Select Statement*/
create table CUSTOMER_CTA as
select t.$1 as C_custkey, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8
  from @SNOWFLAKE_Stage/Customer_data.csv (file_format => "CSV_FILE") t;


-- Create table with names and data types using CTA
CREATE TABLE "TRAININGDB"."SALES"."CUSTOMER_SCRIPT_CTA" (
  "C_CUSTKEY" NUMBER (38, 0) NOT NULL, 
  "C_NAME" VARCHAR (25) NOT NULL, 
  "C_ADDRESS" VARCHAR (40) NOT NULL, 
  "C_NATIONKEY" NUMBER (38, 0) NOT NULL, 
  "C_PHONE" VARCHAR (15) NOT NULL, 
  "C_ACCTBAL" NUMBER (12, 0) NOT NULL, 
  "C_MKTSEGMENT" VARCHAR (10), 
  "C_COMMENT" VARCHAR (117)
) as
 select t.$1 as C_custkey, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8
  from @SNOWFLAKE_Stage/Customer_data.csv (file_format => "CSV_FILE") t;
  
  
/* Alternative Create table with names and data types using CTA*/
CREATE TABLE "TRAININGDB"."SALES"."CUSTOMER_SCRIPT_CTA_DEF" as
 select t.$1::NUMBER(38, 0) as C_custkey, t.$2::VARCHAR(25) as C_Name, t.$3::VARCHAR(40) as C_ADDRESS, t.$4::NUMBER(38, 0) as C_NATIONKEY, t.$5::VARCHAR(15) as C_PHONE, t.$6::NUMBER(12, 0) as C_ACCTBAL, t.$7::VARCHAR(10) as C_MKTSEGMENT, t.$8::VARCHAR(117) as C_COMMENT
  from @SNOWFLAKE_Stage/Customer_data.csv (file_format => "CSV_FILE") t;