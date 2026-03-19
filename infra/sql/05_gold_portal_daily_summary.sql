-- Gold analytical view derived from the curated silver jobs table.
-- Purpose:
--   - provide a business-friendly daily portal summary for reporting and analysis
--   - expose export counts, completeness metrics, and remote-work distribution by portal
--
-- Design intent:
--   - this view is derived from curated silver data rather than raw operational summaries
--   - it represents the reporting layer for portal/day level analysis
--
-- Operational notes:
--   - use this layer for dashboards, ad hoc analysis, and downstream reporting use cases
--   - if reporting definitions change, update this view as a version-controlled schema change

CREATE
OR REPLACE VIEW __DATABASE_NAME__.gold_portal_daily_summary AS
SELECT run_date,
       portal_key,
       COUNT(*)                                                                         AS jobs_exported_total,

       SUM(CASE WHEN title IS NOT NULL AND TRIM(title) <> '' THEN 1 ELSE 0 END)         AS title_present_total,
       SUM(CASE
               WHEN description_text IS NOT NULL AND TRIM(description_text) <> '' THEN 1
               ELSE 0 END)                                                              AS description_present_total,
       SUM(CASE WHEN apply_url IS NOT NULL AND TRIM(apply_url) <> '' THEN 1 ELSE 0 END) AS apply_url_present_total,
       SUM(CASE
               WHEN posted_date IS NOT NULL AND TRIM(posted_date) <> '' THEN 1
               ELSE 0 END)                                                              AS posted_date_present_total,
       SUM(CASE WHEN company IS NOT NULL AND TRIM(company) <> '' THEN 1 ELSE 0 END)     AS company_present_total,

       CAST(100.0 * SUM(CASE WHEN title IS NOT NULL AND TRIM(title) <> '' THEN 1 ELSE 0 END) / COUNT(*) AS
            double)                                                                     AS title_completeness_pct,
       CAST(100.0 * SUM(CASE WHEN description_text IS NOT NULL AND TRIM(description_text) <> '' THEN 1 ELSE 0 END) /
            COUNT(*) AS
            double)                                                                     AS description_completeness_pct,
       CAST(100.0 * SUM(CASE WHEN apply_url IS NOT NULL AND TRIM(apply_url) <> '' THEN 1 ELSE 0 END) / COUNT(*) AS
            double)                                                                     AS apply_url_completeness_pct,
       CAST(100.0 * SUM(CASE WHEN posted_date IS NOT NULL AND TRIM(posted_date) <> '' THEN 1 ELSE 0 END) / COUNT(*) AS
            double)                                                                     AS posted_date_completeness_pct,
       CAST(100.0 * SUM(CASE WHEN company IS NOT NULL AND TRIM(company) <> '' THEN 1 ELSE 0 END) / COUNT(*) AS
            double)                                                                     AS company_completeness_pct,

       SUM(CASE WHEN remote = 'remote' THEN 1 ELSE 0 END)                               AS remote_jobs_total,
       SUM(CASE WHEN remote = 'hybrid' THEN 1 ELSE 0 END)                               AS hybrid_jobs_total,
       SUM(CASE WHEN remote = 'onsite' THEN 1 ELSE 0 END)                               AS onsite_jobs_total,
       SUM(CASE WHEN remote = 'unknown' THEN 1 ELSE 0 END)                              AS unknown_remote_jobs_total

FROM __DATABASE_NAME__.silver_jobs_curated
GROUP BY run_date, portal_key;
