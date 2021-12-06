-- Inserts the rgistration of each occurance into the history table
INSERT INTO CISCO_AUTO_RECLUSTER_HISTORY
(
  ID
, RUN_DATE
, DRIVING_MECH
, TCPC
, AVG_DEPTH
, OTHER_DRIVER
, SIZE_OVERRIDE
, WAREHOUSE_OVERRIDE
, CLUSTERING_INFORMATION)
VALUES
(:1, sysdate, :2, :3, :4, :5, :6, :7, :8)