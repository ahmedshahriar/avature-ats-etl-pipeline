-- Gold analytical view for per-run change events.
-- Purpose:
--   - expose newly seen jobs, updated jobs, and jobs that disappear in the next run
--   - provide a downstream-friendly change feed without mutating historical snapshots

CREATE
OR REPLACE VIEW __DATABASE_NAME__.gold_job_change_events AS
WITH distinct_run_dates AS (
    SELECT run_date,
           lead(run_date) OVER (ORDER BY run_date) AS next_run_date
    FROM (
        SELECT DISTINCT run_date
        FROM __DATABASE_NAME__.silver_jobs_history_snapshot
    )
),
snapshots AS (
    SELECT s.*,
           lag(s.content_hash) OVER (PARTITION BY s.job_hash ORDER BY s.run_date) AS previous_content_hash,
           lag(s.run_date) OVER (PARTITION BY s.job_hash ORDER BY s.run_date)     AS previous_run_date,
           lead(s.run_date) OVER (PARTITION BY s.job_hash ORDER BY s.run_date)    AS next_seen_run_date
    FROM __DATABASE_NAME__.silver_jobs_history_snapshot s
)
SELECT run_date AS event_run_date,
       portal_key,
       job_hash,
       job_id,
       title,
       company,
       source_url,
       content_hash,
       previous_content_hash,
       previous_run_date,
       'new_job' AS event_type
FROM snapshots
WHERE previous_run_date IS NULL

UNION ALL

SELECT run_date AS event_run_date,
       portal_key,
       job_hash,
       job_id,
       title,
       company,
       source_url,
       content_hash,
       previous_content_hash,
       previous_run_date,
       'updated_job' AS event_type
FROM snapshots
WHERE previous_run_date IS NOT NULL
  AND previous_content_hash IS DISTINCT FROM content_hash

UNION ALL

SELECT d.next_run_date AS event_run_date,
       s.portal_key,
       s.job_hash,
       s.job_id,
       s.title,
       s.company,
       s.source_url,
       s.content_hash,
       s.content_hash AS previous_content_hash,
       s.run_date     AS previous_run_date,
       'missing_from_latest_run' AS event_type
FROM snapshots s
JOIN distinct_run_dates d
  ON d.run_date = s.run_date
WHERE d.next_run_date IS NOT NULL
  AND (s.next_seen_run_date IS NULL OR s.next_seen_run_date <> d.next_run_date);
