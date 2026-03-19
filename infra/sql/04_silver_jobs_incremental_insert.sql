-- Incremental daily load into the curated silver jobs table.
-- Purpose:
--   - append a single run_date partition from bronze into the silver Parquet dataset
--   - support routine daily promotion after the initial CTAS bootstrap
--
-- Execution guidance:
--   - replace __TARGET_RUN_DATE__ with the specific partition date before execution
--   - run this only after the silver table has already been created
--   - reruns for the same date should be handled carefully to avoid duplicate inserts
--
-- Data quality rules:
--   - only records marked as valid in bronze are inserted
--
-- Modeling note:
--   - run_date is the partition column and must remain the final selected column

INSERT INTO __DATABASE_NAME__.silver_jobs_curated
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
WHERE record_status = 'valid'
  AND run_date = '__TARGET_RUN_DATE__';
