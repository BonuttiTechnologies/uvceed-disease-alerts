-- Phase 3 schema additions (idempotent)
CREATE TABLE IF NOT EXISTS signal_snapshots (
  id bigserial PRIMARY KEY,
  zip_code text NOT NULL,
  signal_type text NOT NULL,
  generated_at timestamptz NOT NULL,
  payload jsonb NOT NULL,
  pathogen text,
  geo_level text,
  geo_id text,
  state text,
  county_fips text,
  risk_level text,
  trend text,
  confidence text,
  composite_score double precision
);

CREATE INDEX IF NOT EXISTS idx_signal_snapshots_zip_type_time
  ON signal_snapshots(zip_code, signal_type, generated_at DESC);

CREATE TABLE IF NOT EXISTS zip_requests (
  zip_code text PRIMARY KEY,
  first_requested_at timestamptz NOT NULL DEFAULT now(),
  last_requested_at timestamptz NOT NULL DEFAULT now(),
  last_refreshed_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_zip_requests_last_requested
  ON zip_requests(last_requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_zip_requests_last_refreshed
  ON zip_requests(last_refreshed_at DESC);
