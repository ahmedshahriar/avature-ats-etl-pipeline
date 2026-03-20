-- Incremental daily load into the curated silver jobs table.
-- Purpose:
--   - append today's valid bronze rows into the silver Parquet dataset
--   - stay safe to rerun by skipping rows already present in silver for the same run_date/job_hash
--
-- Execution guidance:
--   - this query assumes a daily workflow and promotes the current UTC run_date partition
--   - reruns on the same day do not duplicate rows because of the anti-join
--
-- Data quality rules:
--   - only records marked as valid in bronze are inserted
--
-- Modeling note:
--   - run_date is the partition column and must remain the final selected column

INSERT INTO __DATABASE_NAME__.silver_jobs_curated
SELECT
    b.job_hash,
    b.source_url,
    b.raw_source_url,
    b.canonical_source_url,
    b.portal_key,
    b.run_id,
    b.scraped_at,
    b.input_seed_url,
    b.job_id,
    b.title,
    b.company,
    b.locations,
    b.posted_date,
    b.remote,
    b.employment_type,
    b.career_area,
    b.ref_number,
    b.description_text,
    b.apply_url,
    b.validation_warnings,
    b.run_date
FROM __DATABASE_NAME__.bronze_jobs_raw b
LEFT JOIN __DATABASE_NAME__.silver_jobs_curated s
    ON s.run_date = b.run_date
   AND s.job_hash = b.job_hash
WHERE b.record_status = 'valid'
  AND b.run_date = CAST(current_date AS varchar)
  AND s.job_hash IS NULL;
