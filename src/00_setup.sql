-- Phase 1 setup: run once in a Databricks SQL editor or SQL notebook.
-- Databricks Free Edition usually gives you the "workspace" catalog.
-- If your catalog has a different name, change it here and in the two Python files.

USE CATALOG workspace;

CREATE SCHEMA IF NOT EXISTS raw;     -- landing files live here (as a Volume)
CREATE SCHEMA IF NOT EXISTS bronze;  -- raw data as Delta tables
CREATE SCHEMA IF NOT EXISTS silver;  -- cleaned data (Phase 2)
CREATE SCHEMA IF NOT EXISTS gold;    -- analytics tables (Phase 2)

CREATE VOLUME IF NOT EXISTS raw.landing;

-- Check: you should see the four schemas, and the volume under raw.
SHOW SCHEMAS;
SHOW VOLUMES IN raw;
