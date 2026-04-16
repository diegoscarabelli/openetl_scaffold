-- Run once, as a superuser, before all other DDL.
-- Replace system2_pipelines_db with your preferred database name.

CREATE DATABASE system2_pipelines_db
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8';
