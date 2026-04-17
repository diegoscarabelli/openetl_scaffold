/*
========================================================================================
LENS DATABASE CREATION
========================================================================================
Description: This script creates the `lens` and `lens_dev` databases for data
             pipeline operations. Must be executed before schemas.ddl and pipeline
             table scripts.

Prerequisites:
  - PostgreSQL (or TimescaleDB-enabled PostgreSQL) must be installed and running.
  - Must have superuser privileges (typically the `postgres` user).

Connection:
  - Connect to the default `postgres` database to create the `lens` database:
    psql -U postgres -d postgres -f database.ddl
  - Alternative with TCP/IP and password authentication:
    psql -h localhost -U postgres -d postgres -f database.ddl

Notes:
  - Character encoding and collation are set for UTF-8 support.
  - Replace `lens` below with your preferred database name.
========================================================================================
*/

-- Set client encoding for consistent character handling.
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

----------------------------------------------------------------------------------------
-- DATABASE CREATION
----------------------------------------------------------------------------------------

CREATE DATABASE lens
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TEMPLATE template0;

COMMENT ON DATABASE lens IS
    'Data pipeline analytics database.';

----------------------------------------------------------------------------------------
-- DEVELOPMENT DATABASE
----------------------------------------------------------------------------------------

CREATE DATABASE lens_dev
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TEMPLATE template0;

COMMENT ON DATABASE lens_dev IS
    'Non-production database for development and testing.';
