-- Any reclustering operation will generate a record placed here. This may be referenced to see the configuration
-- and the captured clustering information for that run.

CREATE TABLE CISCO_AUTO_RECLUSTER_HISTORY
(
  RUN_ID                  NUMBER(15)            NOT NULL,
  ID                      NUMBER(15)            NOT NULL,
  RUN_DATE                DATE                  NOT NULL,
  DRIVING_MECH            VARCHAR2(200),
  TCPC                    NUMBER(15),
  AVG_DEPTH               NUMBER(15),
  OTHER_DRIVER            NUMBER(15),
  SIZE_OVERRIDE           VARCHAR2(50),
  WAREHOUSE_OVERRIDE      VARCHAR2(200),
  CLUSTERING_INFORMATION  VARCHAR2(4000)
)
RESULT_CACHE (MODE DEFAULT)
STORAGE    (
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING
NOCOMPRESS
NOCACHE
NOPARALLEL
NOMONITORING;


ALTER TABLE CISCO_AUTO_RECLUSTER_HISTORY ADD (
  CONSTRAINT CISCO_AUTO_RECLUSTER_HISTORYPK
  PRIMARY KEY
  (RUN_ID)
  ENABLE VALIDATE);
