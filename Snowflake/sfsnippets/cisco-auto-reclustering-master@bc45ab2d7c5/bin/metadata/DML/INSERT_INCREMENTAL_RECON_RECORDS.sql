-- Insert the tables which were found to contain clustering keys into the driving, incremental table.
INSERT INTO CISCO_AUTO_RECLUSTER_INC
(
    ACCOUNT
  , DB_NAME
  , SCHEMA_NAME
  , TABLE_NAME)
VALUES
(:1, :2, :3, :4)