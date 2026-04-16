-- DDL for the example pipeline.
-- Keep in sync with sqla_models.py: same column names, types, constraints.
-- Run: psql -U postgres -d system2 -f pipelines/example/tables.ddl

CREATE TABLE IF NOT EXISTS example.example_record (
    id SERIAL PRIMARY KEY
    , name TEXT NOT NULL UNIQUE
    , value TEXT
    , create_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
    , update_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE example.example_record IS
'Template table — replace with your domain entity.';
