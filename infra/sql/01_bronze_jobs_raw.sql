-- Bronze external table over raw JSON Lines job records written by the scraper.
-- Purpose:
--   - preserve the source-aligned landing dataset in its original structured form
--   - provide a stable query surface for validation, replay, and downstream curation
--
-- Partitioning:
--   - data is partitioned by run_date
--   - partition projection is used so Athena can resolve partitions from table properties
--     without relying on Glue crawlers or MSCK REPAIR operations
--
-- Operational notes:
--   - this table is intended to be append-only at the S3 prefix level
--   - schema changes should be version-controlled and applied explicitly
--   - this layer should remain close to the scraper contract and avoid business-specific reshaping

CREATE
EXTERNAL TABLE IF NOT EXISTS __DATABASE_NAME__.bronze_jobs_raw (
  job_hash string,
  source_url string,
  raw_source_url string,
  canonical_source_url string,
  portal_key string,
  run_id string,
  scraped_at string,
  input_seed_url string,
  job_id string,
  title string,
  company string,
  locations array<string>,
  posted_date string,
  remote string,
  employment_type string,
  career_area string,
  ref_number string,
  description_text string,
  apply_url string,
  raw_fields map<string,string>,
  record_status string,
  validation_errors array<string>,
  validation_warnings array<string>
)
PARTITIONED BY (run_date string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://__BUCKET_NAME__/__DATASET_ROOT__/bronze/jobs/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.run_date.type'='date',
  'projection.run_date.range'='2026-01-01,NOW',
  'projection.run_date.format'='yyyy-MM-dd',
  'projection.run_date.interval'='1',
  'projection.run_date.interval.unit'='DAYS',
  'storage.location.template'='s3://__BUCKET_NAME__/__DATASET_ROOT__/bronze/jobs/run_date=${run_date}/'
);
