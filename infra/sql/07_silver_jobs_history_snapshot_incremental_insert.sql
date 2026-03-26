-- Incremental daily load into the history-oriented silver snapshot table.
-- Purpose:
--   - append valid bronze rows into the run_date snapshot table for historical analysis
--   - stay safe to rerun by skipping rows already present in the same run_date partition
--
-- Execution guidance:
--   - the saved Athena named query resolves __RUN_DATE_FILTER__ to CAST(current_date AS varchar)
--   - the Step Functions manual override path injects the requested run_date
--   - reruns for the same run_date do not duplicate rows because of the anti-join

INSERT INTO __DATABASE_NAME__.silver_jobs_history_snapshot
SELECT
    b.job_hash,
    to_hex(
        sha256(
            to_utf8(
                concat(
                    coalesce(b.title, ''),
                    '|',
                    coalesce(b.company, ''),
                    '|',
                    coalesce(array_join(b.locations, '|'), ''),
                    '|',
                    coalesce(b.posted_date, ''),
                    '|',
                    coalesce(b.remote, ''),
                    '|',
                    coalesce(b.employment_type, ''),
                    '|',
                    coalesce(b.career_area, ''),
                    '|',
                    coalesce(b.ref_number, ''),
                    '|',
                    coalesce(b.description_text, ''),
                    '|',
                    coalesce(b.apply_url, '')
                )
            )
        )
    )                                                                              AS content_hash,
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
LEFT JOIN __DATABASE_NAME__.silver_jobs_history_snapshot h
    ON h.run_date = b.run_date
   AND h.job_hash = b.job_hash
WHERE b.record_status = 'valid'
  AND b.run_date = __RUN_DATE_FILTER__
  AND h.job_hash IS NULL;
