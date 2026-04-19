/*
========================================================================================
LENS DATABASE IDENTITY AND ACCESS MANAGEMENT (IAM)
========================================================================================
Description: This script creates roles, login users, and permissions for the `lens`
             database (or `lens_dev`). Must be executed after schemas.ddl.

Prerequisites:
  - The `lens` database must already exist (created by database.ddl).
  - Schemas should be created (created by schemas.ddl).
  - PostgreSQL 14 or newer (uses `pg_read_all_data` and `pg_write_all_data`
    predefined roles introduced in PostgreSQL 14).
  - Must have superuser privileges (typically the `postgres` user).
  - IMPORTANT: Replace the two `<REDACTED>` password placeholders below with
    real passwords before running this script.

Design Notes:
  - Blanket permissions via PostgreSQL predefined roles `pg_read_all_data` and
    `pg_write_all_data` cover all non-system schemas, including future ones.
    No per-schema `GRANT` is needed when adding a new pipeline schema.
  - `pg_write_all_data` does NOT include `TRUNCATE`. Pipelines that truncate
    tables must be granted `TRUNCATE` explicitly on those tables.
  - A single generic `data_pipelines` user handles all pipeline writes.
    For stronger per-pipeline isolation, replace it with per-pipeline users
    and scope permissions per schema.
  - Re-running this script is safe: `CREATE ROLE`/`CREATE USER` statements
    will error if the role already exists, but `psql` continues and the
    remaining `GRANT`/`COMMENT` statements still apply.

Connection:
  - Connect to the `lens` database to create users and grant permissions:
    psql -U postgres -d lens -f iam.sql
  - Alternative with TCP/IP and password authentication:
    psql -h localhost -U postgres -d lens -f iam.sql
========================================================================================
*/

-- Set client encoding for consistent character handling.
SET client_encoding = 'UTF8';

----------------------------------------------------------------------------------------
-- READ-ONLY ROLE AND USER
----------------------------------------------------------------------------------------

-- Permission bundle for read-only access across all schemas.
CREATE ROLE readers;
GRANT pg_read_all_data TO readers;
COMMENT ON ROLE readers IS
    'Read-only permission bundle for data consumers.';

-- Login user for read-only data consumers (BI tools, notebooks, ad-hoc SQL).
CREATE USER read_only WITH PASSWORD '<REDACTED>';
GRANT readers TO read_only;
COMMENT ON ROLE read_only IS
    'Login user for read-only data consumers (BI tools, notebooks, ad-hoc SQL).';

----------------------------------------------------------------------------------------
-- PIPELINE SERVICE USER
----------------------------------------------------------------------------------------

-- Login user for pipeline read and write operations across all schemas.
CREATE USER data_pipelines WITH PASSWORD '<REDACTED>';
GRANT pg_read_all_data, pg_write_all_data TO data_pipelines;
COMMENT ON ROLE data_pipelines IS
    'Service user for data pipeline operations (read + write across all schemas).';
