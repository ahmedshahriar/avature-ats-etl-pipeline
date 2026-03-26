-- One-time bootstrap query for the history-oriented silver snapshot table.
-- Purpose:
--   - preserve one valid job snapshot per run_date for historical analysis
--   - add a stable content_hash so downstream views can detect changed jobs over time
--
-- Execution guidance:
--   - run this query once after bronze and curated silver are already bootstrapped
--   - the external_location must be empty before the first execution
--   - subsequent daily loads should use the incremental INSERT query instead of rerunning CTAS
--
-- Modeling note:
--   - run_date is the partition column and must remain the final selected column

CREATE TABLE __DATABASE_NAME__.silver_jobs_history_snapshot
    WITH
(
    format =
    'PARQUET',
    write_compression =
    'SNAPPY',
    external_location =
    's3://__BUCKET_NAME__/__DATASET_ROOT__/silver/jobs_history_snapshot/',
    partitioned_by =
    ARRAY
[
    'run_date']
) AS
SELECT job_hash,
       to_hex(
           sha256(
               to_utf8(
                   concat(
                       coalesce(title, ''),
                       '|',
                       coalesce(company, ''),
                       '|',
                       coalesce(array_join(locations, '|'), ''),
                       '|',
                       coalesce(posted_date, ''),
                       '|',
                       coalesce(remote, ''),
                       '|',
                       coalesce(employment_type, ''),
                       '|',
                       coalesce(career_area, ''),
                       '|',
                       coalesce(ref_number, ''),
                       '|',
                       coalesce(description_text, ''),
                       '|',
                       coalesce(apply_url, '')
                   )
               )
           )
       )                                                                              AS content_hash,
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
