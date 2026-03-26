-- Gold analytical view for job lifecycle tracking.
-- Purpose:
--   - expose first_seen / last_seen state per job_hash
--   - make it easy for BI tools to answer "is this job missing from the latest run?"

CREATE
OR REPLACE VIEW __DATABASE_NAME__.gold_job_lifecycle_summary AS
WITH latest_run AS (
    SELECT max(run_date) AS latest_run_date
    FROM __DATABASE_NAME__.silver_jobs_history_snapshot
),
lifecycle AS (
    SELECT job_hash,
           max_by(portal_key, run_date)        AS portal_key,
           max_by(job_id, run_date)            AS job_id,
           max_by(source_url, run_date)        AS source_url,
           max_by(title, run_date)             AS title,
           max_by(company, run_date)           AS company,
           max_by(locations, run_date)         AS locations,
           max_by(posted_date, run_date)       AS posted_date,
           max_by(remote, run_date)            AS remote,
           max_by(employment_type, run_date)   AS employment_type,
           max_by(career_area, run_date)       AS career_area,
           max_by(ref_number, run_date)        AS ref_number,
           max_by(apply_url, run_date)         AS apply_url,
           max_by(content_hash, run_date)      AS latest_content_hash,
           min(run_date)                       AS first_seen_run_date,
           max(run_date)                       AS last_seen_run_date,
           count(*)                            AS snapshot_count
    FROM __DATABASE_NAME__.silver_jobs_history_snapshot
    GROUP BY job_hash
)
SELECT lifecycle.*,
       latest_run.latest_run_date,
       lifecycle.last_seen_run_date < latest_run.latest_run_date AS missing_from_latest_run
FROM lifecycle
CROSS JOIN latest_run;
