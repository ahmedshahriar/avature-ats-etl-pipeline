-- One-time bootstrap query for the curated silver jobs table.
-- Purpose:
--   - convert valid bronze JSON records into a curated Parquet dataset
--   - establish a more query-efficient analytical layer for downstream consumers
--
-- Data quality rules:
--   - only records marked as valid in bronze are included
--   - the silver layer is expected to contain normalized, analytics-ready job records
--
-- Execution guidance:
--   - run this query once to create and initialize the silver table
--   - the external_location must be empty before the first execution
--   - subsequent daily loads should use the incremental INSERT query instead of rerunning CTAS
--
-- Modeling note:
--   - run_date is used as the partition column and must remain the final selected column


CREATE TABLE __DATABASE_NAME__.silver_jobs_curated
    WITH
(
    format =
    'PARQUET',
    write_compression =
    'SNAPPY',
-- if a workgroup enforces a centralized query results location, a CTAS query that specifies external_location fails.
-- Athena also requires the CTAS external_location prefix to be empty before the first run.
-- enforce_work_group_configuration is set to false
    external_location =
    's3://__BUCKET_NAME__/__DATASET_ROOT__/silver/jobs_curated/',
    partitioned_by =
    ARRAY
[
    'run_date']
) AS
SELECT job_hash,
       source_url,
       raw_source_url,
       canonical_source_url,
       portal_key,
       run_id,
       scraped_at,
       input_seed_url,
       job_id,
       title,
       company,
       locations,
       posted_date,
       remote,
       employment_type,
       career_area,
       ref_number,
       description_text,
       apply_url,
       validation_warnings,
       run_date
FROM __DATABASE_NAME__.bronze_jobs_raw
WHERE record_status = 'valid';
