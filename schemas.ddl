/*
========================================================================================
SYSTEM2 DATABASE SCHEMA INITIALIZATION
========================================================================================
Description: This script creates PostgreSQL extensions and per-pipeline schemas
             for the `system2` database. Must be executed after database.ddl.

Prerequisites:
  - The `system2` database must already exist (created by database.ddl).
  - PostgreSQL must be running.
  - Must have superuser privileges (typically the `postgres` user).

Optional Components:
  - PostgreSQL extensions (see EXTENSIONS section).
  - If an extension is not installed, comment out BOTH its CREATE EXTENSION
    and the matching COMMENT ON EXTENSION statement; otherwise the COMMENT
    ON statement will fail and halt execution.
  - Extensions requiring separate installation:
    * TimescaleDB: Time-series database capabilities.

Connection:
  - Connect to the system2 database to create extensions and schemas:
    psql -U postgres -d system2 -f schemas.ddl
  - Alternative with TCP/IP and password authentication:
    psql -h localhost -U postgres -d system2 -f schemas.ddl
========================================================================================
*/

----------------------------------------------------------------------------------------
-- EXTENSIONS
----------------------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
COMMENT ON EXTENSION timescaledb IS
    'Time-series database built on PostgreSQL.';

----------------------------------------------------------------------------------------
-- PIPELINE SCHEMAS
----------------------------------------------------------------------------------------
-- Add one CREATE SCHEMA block per pipeline.

CREATE SCHEMA IF NOT EXISTS wid;
COMMENT ON SCHEMA wid IS
    'World Inequality Database (WID.world) income and wealth distribution data.';
