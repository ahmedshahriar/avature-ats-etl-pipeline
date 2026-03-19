-- Ops external table over portal-level summary records produced by the scraper extension.
-- Purpose:
--   - expose operational telemetry by portal for trend analysis and health monitoring
--   - support downstream analysis of request volume, export volume, duplicate rate,
--     quarantine counts, and field completeness by portal
--
-- Partitioning:
--   - data is partitioned by run_date
--   - partition projection is used to avoid crawler-based partition discovery
--
-- Operational notes:
--   - this dataset is intended for operational analytics, not as the canonical business dataset
--   - the schema reflects the portal summary artifact emitted at the end of each scraper run
--   - changes to portal summary fields should be treated as controlled schema changes

CREATE
EXTERNAL TABLE IF NOT EXISTS __DATABASE_NAME__.ops_portal_summary_raw (
  portal_key string,
  requests_total bigint,
  responses_total bigint,
  error_responses_total bigint,
  listing_requests_total bigint,
  pagination_requests_total bigint,
  job_detail_requests_total bigint,
  jobs_exported_total bigint,
  jobs_quarantined_total bigint,
  duplicates_dropped_total bigint,
  duplicate_rate double,
  job_detail_success_rate double,
  title_completeness_pct double,
  description_completeness_pct double,
  apply_url_completeness_pct double,
  posted_date_completeness_pct double,
  company_completeness_pct double
)
PARTITIONED BY (run_date string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://__BUCKET_NAME__/__DATASET_ROOT__/ops/portal_summaries/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.run_date.type'='date',
  'projection.run_date.range'='2026-01-01,NOW',
  'projection.run_date.format'='yyyy-MM-dd',
  'projection.run_date.interval'='1',
  'projection.run_date.interval.unit'='DAYS',
  'storage.location.template'='s3://__BUCKET_NAME__/__DATASET_ROOT__/ops/portal_summaries/run_date=${run_date}/'
);
