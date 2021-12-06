-- Ensure that all tables found which are set to auto recluster will not recluster.
alter table $$TABLE_CATALOG.$$TABLE_SCHEMA.$$TABLE_NAME suspend recluster;