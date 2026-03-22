# AWS Operations Runbook

This runbook is intentionally operational-only.

It covers:

- Athena bootstrap
- daily workflow checks
- manual workflow runs
- alert handling
- rollback / validation

It does **not** repeat project overview, local setup, Docker usage, or detailed deployment steps from the README.

---

## 1. One-time Athena bootstrap

CDK creates the Glue database, Athena workgroup, and saved Athena queries, but it does **not** execute the SQL
automatically.

Run these saved Athena queries in this order:

1. `01_bronze_jobs_raw`
2. `02_ops_portal_summary_raw`
3. `03_silver_jobs_curated_ctas`
4. `05_gold_portal_daily_summary`

### Notes

- `03_silver_jobs_curated_ctas` is a **one-time initialization** query.
- The silver CTAS `external_location` must be empty before the first run.
- Daily operation should use `04_silver_jobs_incremental_insert` through the workflow.
- The gold layer is a **view**, so it does not need a separate daily refresh job.

---

## 2. Daily production flow

Production schedule owner:

- `schedule_target: workflow`

Expected flow:

1. EventBridge Scheduler starts the Step Functions workflow
2. Step Functions runs the ECS Fargate scraper task
3. Step Functions runs the Athena silver incremental insert
4. Gold remains queryable through Athena

### Daily checks

Confirm:

- the Step Functions execution succeeded
- the ECS scraper task completed successfully
- the Athena silver insert succeeded
- the workflow scheduler DLQ is empty
- no relevant CloudWatch alarms are in `ALARM`

---

## 3. Manual workflow runs

Use a **manual Step Functions execution**.

### Normal manual run

Use an empty input object:

```json
{}
````

### Manual backfill / rerun

Use both override values together:

```json
{
  "run_date_override": "2026-03-20",
  "run_id_override": "20260320T120000Z"
}
```

### Important rules

* Provide **both** override fields or neither.
* `run_date_override` controls the logical partition promoted to silver.
* `run_id_override` gives the execution a unique run identifier.

---

## 4. Alert handling

### A. Workflow scheduler DLQ alarm

Meaning:

* EventBridge Scheduler could not deliver the scheduled invocation to Step Functions.

What to do:

1. Open the workflow scheduler DLQ in SQS
2. Inspect the failed event payload
3. Check the scheduler target ARN and execution role
4. Confirm the state machine still exists and the schedule is enabled
5. Fix the delivery problem
6. Start the workflow manually if that daily run was missed

---

### B. Workflow failed alarm

Meaning:

* The workflow started, but one of its states failed.

What to do:

1. Open the failed Step Functions execution
2. Identify the failed state:

    * ECS scraper task
    * Athena query submission
    * Athena query polling
3. Review the execution error payload
4. Check ECS logs and Athena query history
5. Re-run manually or use Step Functions redrive if appropriate

---

### C. Workflow timed out alarm

Meaning:

* The overall workflow exceeded its timeout.

What to do:

1. Check whether ECS runtime was unusually long
2. Check whether Athena polling/query time was unusually long
3. Confirm this is not caused by a stuck or failing scrape
4. Increase timeout only if longer runtime is expected and justified

---

### D. Workflow throttled alarm

Meaning:

* Step Functions reported throttling.

What to do:

1. Check for repeated manual triggering
2. Check recent execution volume
3. Investigate unexpected spikes before retrying repeatedly

---

### E. Empty-run or low-quality alarms

Meaning:

* Very low exported job volume, or
* degraded job-detail success rate

What to do:

1. Review scraper logs
2. Check affected portals
3. Inspect a few raw pages manually
4. Confirm whether the behavior is expected or caused by extraction drift

---

## 5. ECS / scraper troubleshooting

Check:

* ECS task stopped reason
* container exit code
* CloudWatch log stream
* S3 run outputs
* image tag used by the task

Common causes:

* Avature page structure changes
* parser / selector drift
* runtime config issue
* container image mismatch
* network/transient failures

---

## 6. Athena troubleshooting

### Silver insert failed

Check:

* Athena query execution status
* bronze data exists for the expected `run_date`
* silver table exists
* workgroup result location is valid
* query did not exceed the bytes-scanned cutoff

### Bootstrap / CTAS issue

Check:

* bootstrap queries were run in order
* silver CTAS location was empty before first run
* Glue database and named queries exist

---

## 7. Cost-control operational notes

Implemented guardrails:

* Athena bytes-scanned cutoff
* AWS Budget alerting
* cost allocation tags applied through CDK

### Manual billing step

After deploy, activate the user-defined cost allocation tags in AWS Billing:

1. Open **Billing and Cost Management**
2. Go to **Cost allocation tags**
3. Select the tag keys used by the project
4. Click **Activate**

---

## 8. Rollback basics

Use this when a release causes operational issues.

1. Disable the production schedule temporarily
2. Revert the last known bad change
3. Redeploy the previous stable version
4. Validate:

    * schedule state
    * workflow execution
    * ECS task run
    * Athena silver insert
    * alarm state

---

## 9. Post-deploy validation

After deployment:

1. Run one manual workflow execution
2. Confirm ECS scraper success
3. Confirm Athena silver insert success
4. Query the expected silver partition
5. Query the gold view
6. Confirm alarms and scheduler DLQ are healthy

---
