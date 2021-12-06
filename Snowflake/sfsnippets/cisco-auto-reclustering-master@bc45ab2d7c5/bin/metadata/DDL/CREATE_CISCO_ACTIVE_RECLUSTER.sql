-- CISCO_AUTO_RECLUSTER houses the main driving data. All tables requiring reclustering remain here.

ALTER TABLE CISCO_AUTO_RECLUSTER
 DROP PRIMARY KEY CASCADE;

DROP TABLE CISCO_AUTO_RECLUSTER CASCADE CONSTRAINTS;

CREATE TABLE CISCO_AUTO_RECLUSTER
(
  ID                  NUMBER                    NOT NULL,
  ACCOUNT             VARCHAR2(200 BYTE)        NOT NULL,
  DB_NAME             VARCHAR2(200 BYTE)        NOT NULL,
  SCHEMA_NAME         VARCHAR2(200 BYTE)        NOT NULL,
  TABLE_NAME          VARCHAR2(200 BYTE)        NOT NULL,
  RUN_SCHEDULE        VARCHAR2(200 BYTE)        NOT NULL,
  DRIVING_MECH        VARCHAR2(200 BYTE)        NOT NULL,
  TCPC                NUMBER(15)                DEFAULT 75                    NOT NULL,
  AVG_DEPTH           NUMBER(15)                DEFAULT 30                    NOT NULL,
  OTHER_DRIVER        NUMBER(15),
  MAX_RUNS            NUMBER(15)                DEFAULT 1                     NOT NULL,
  SIZE_OVERRIDE       VARCHAR2(50 BYTE),
  WAREHOUSE_OVERRIDE  VARCHAR2(200 BYTE),
  ACTIVE_FLAG         VARCHAR2(50 BYTE)         DEFAULT 'NO'                  NOT NULL,
  GROUP_NAME          VARCHAR2(100 BYTE),
  PASSPHRASE          VARCHAR2(200 BYTE)        NOT NULL
);


--  There is no statement for index SYS_C001832967.
--  The object is created when the parent object is created.

ALTER TABLE CISCO_AUTO_RECLUSTER ADD (
  PRIMARY KEY
  (ID)
  ENABLE VALIDATE);
