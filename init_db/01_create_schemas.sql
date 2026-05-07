CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS public.pipeline_watermarks (
    source_name     TEXT PRIMARY KEY,
    last_updated_ts TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
