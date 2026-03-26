# Avature ATS ETL Pipeline

<p align="center"><a href="https://github.com/ahmedshahriar/avature-ats-etl-pipeline/actions/workflows/ci.yml"><img align="absmiddle" alt="CI" src="https://github.com/ahmedshahriar/avature-ats-etl-pipeline/actions/workflows/ci.yml/badge.svg"></a> <a href="https://github.com/ahmedshahriar/avature-ats-etl-pipeline/actions/workflows/cd.yml"><img align="absmiddle" alt="CD" src="https://github.com/ahmedshahriar/avature-ats-etl-pipeline/actions/workflows/cd.yml/badge.svg"></a> <a href="https://docs.astral.sh/ruff/"><img align="absmiddle" alt="Code Style: Ruff" src="https://img.shields.io/badge/Code%20Style-Ruff-46A3FF?logo=ruff&logoColor=white"></a> <a href="LICENSE"><img align="absmiddle" alt="License: Apache-2.0" src="https://img.shields.io/badge/License-Apache--2.0-green.svg"></a></p>

<p align="center"><img align="absmiddle" alt="Python 3.13" src="https://img.shields.io/badge/Python-3.13-blue?logo=python&logoColor=white"> <a href="https://scrapy.org/"><img align="absmiddle" alt="Scrapy" src="https://img.shields.io/badge/Crawler-Scrapy-60A839?logo=scrapy&logoColor=white"></a> <a href="https://aws.amazon.com/cdk/"><img align="absmiddle" alt="AWS CDK" src="https://img.shields.io/badge/IaC-AWS%20CDK-FF9900?logo=amazonaws&logoColor=white"></a></p>

A Scrapy-based end-to-end ETL pipeline for crawling Avature career portals (job listings → job details) and exporting normalized job records.

Engineered for reliable cloud execution, this project features:

- **Infrastructure as Code (IaC)** provisioning managed entirely via AWS CDK
- **Workflow orchestration** with EventBridge Scheduler, Step Functions and ECS Fargate
- **Scalable data lake storage** writing bronze artifacts and run metadata to S3
- **Cross-run idempotency & deduplication** powered by DynamoDB
- **Data quality controls** with validation, warnings, and quarantine for invalid records
- **Curated analytics layer** using Athena and Glue with bronze / silver / gold modeling plus historical job snapshots
- **Reliability tooling** with offline scraper regression tests, seed health auditing, and a local smoke crawl command
- **Operational observability** with CloudWatch logs, Embedded Metric Format (EMF) metrics, alarms, and SNS alerts
- **Fault tolerance** using SQS Dead Letter Queue (DLQ) for failed invocations
- **Cost guardrails** with Athena scan limits and AWS Budget alerts
- **Automated CI/CD** using GitHub Actions with AWS OIDC and AWS ECR

## Estimated AWS cost

> Reference scenario: **us-east-1**, current `prod` configuration, **1 scheduled workflow run per day**, **1 ECS Fargate ARM64 task per run**, **1 vCPU / 2 GiB RAM**, **~2 hours per run**, analytics enabled, dashboard enabled, Container Insights disabled, and daily Athena silver + history snapshot promotion.

| Function                        | AWS service | Scenario / config anchor | Estimated monthly cost |
|---------------------------------| --- | --- |:----------------------:|
| Batch scraper compute           | ECS Fargate (Linux/ARM64) | `1024 CPU`, `2048 MiB`, `ARM64`, ~2 hours/day, 30 days |       **~$2.37**       |
| Public task networking          | Public IPv4 address | `assign_public_ip=True` for the Fargate task, ~2 hours/day |       **~$0.30**       |
| Daily workflow orchestration    | Step Functions Standard | ~10–20 state transitions per run, 30 runs/month |       **~$0.01**       |
| Daily trigger                   | EventBridge Scheduler | 30 scheduled invocations/month |       **~$0.00**       |
| Daily silver + history promotion | Athena | workgroup scan cutoff = `512 MB/query`; worst-case two daily queries at that cap |      **≤ ~$0.15**      |
| Metadata catalog                | AWS Glue Data Catalog | 1 small database + a few tables / named queries |       **~$0.00**       |
| Bronze and ops artifact storage | Amazon S3 Standard | assume ~10 MB of new objects per run, plus low request volume |       **~$0.01**       |
| Cross-run dedupe                | DynamoDB on-demand | assume ~5,000 new unique jobs/day, 1 write per job, plus small PITR/storage overhead |    **~$0.10–$0.15**    |
| Runtime and workflow logs       | CloudWatch Logs | `INFO` logging, 30-day retention, light batch log volume |    **~$0.15–$0.20**    |
| Low-cardinality metrics         | CloudWatch custom metrics | assume 7 custom metric series, 1 `[Project, Stage, Spider]` combination, emitted ~2 hours/day |       **~$0.18**       |
| Ops dashboard                   | CloudWatch Dashboard | 1 custom dashboard |       **$3.00**        |
| Operational alarms              | CloudWatch Alarms | 7 standard-resolution alarms (3 runtime + 3 workflow + 1 scheduler DLQ) |       **~$0.70**       |
| Alerting / DLQ                  | SNS + SQS | low alert traffic; no email subscription configured in current `prod` config |       **~$0.00**       |
| Budget guardrail                | AWS Budgets | budget notifications only |       **$0.00**        |
| Container image registry        | Amazon ECR | assume ~1–2 GB of retained private image storage; same-region ECS/Fargate pulls are free |    **~$0.10–$0.20**    |

**Expected monthly envelope:** roughly **$6–$7/month** under this scenario.

### What drives the bill most

<details>
<summary>See the main cost drivers</summary>

1. **CloudWatch dashboard and alarms**
2. **Fargate runtime**
3. **CloudWatch logs**
4. **Any extra custom metric series beyond the base low-cardinality set**

At this scale, **Step Functions, EventBridge Scheduler, Athena daily promotion, S3 requests, SNS, and SQS are comparatively tiny**.
</details>

### What keeps cost low

<details>
<summary>See the main cost-control choices</summary>

- **ARM64 Fargate**
- **once-daily scheduling**
- **Container Insights disabled**
- **Athena bytes-scanned cutoff**
- **low-cardinality EMF**
- **1-month CloudWatch log retention**
- **S3 lifecycle rules for ops/quarantine artifacts**

> Notes:
> - The custom metric estimate assumes exactly **one** `[Project, Stage, Spider]` dimension combination. Additional unique combinations increase cost linearly.
> - CloudWatch custom metrics are **prorated by hour**, so a short daily batch workload is much cheaper than 24×7 publishing.
> - The S3 estimate reflects **new monthly data only**. Historical storage will continue to grow over time because `avature/bronze/jobs/` does not currently have an expiration rule.
> - The CloudWatch Logs estimate assumes the configured **30-day retention policy** and moderate `INFO`-level log volume.
> - The DynamoDB estimate assumes **~5,000 new unique jobs/day**. Higher unique job volume scales linearly.
> - No Secrets Manager line item is included because it is not part of the shared stack files.
> - Same-region pulls from **ECR → ECS/Fargate** are free, but local developer pulls to the internet are billed separately.

</details>

---

## What it extracts

From each Avature job detail page, the spider extracts the core job record plus lineage and validation metadata.

### Core fields

- **Job Title (`title`)**
- **Job Description (`description_text`)** (clean text; derived from Avature sections)
- **Application URL (`apply_url`)**

### Metadata when available

- `locations`
- `posted_date`
- `company`
- `career_area`
- `employment_type`
- `remote`
- `ref_number`
- `job_id`


### Lineage and identity fields

- `source_url` — canonical detail URL used as the stable item URL
- `canonical_source_url` — normalized detail URL used for stable identity
- `raw_source_url` — raw URL returned by the site before canonicalization
- `portal_key` — stable portal identifier such as `ally.avature.net/careers`
- `input_seed_url` — the listing URL from [`seed_urls.csv`](seed_urls.csv) that led to the crawl
- `run_id` — unique execution identifier
- `scraped_at` — UTC timestamp when the item was produced
- `job_hash` — stable unique job key derived from portal identity (`portal_key`) and `job_id` when available

### Validation fields

- `validation_errors`
- `validation_warnings`
- `record_status` — `valid` or `quarantined`

### Raw extraction fallback

- `raw_fields` — raw label/value pairs captured from the page for audit/debugging

---

## Outputs

### Local

Each run creates a unique output directory:

```text
output/run_<RUN_ID>/
  jobs.jsonl
  metrics.json
  run_manifest.json
  portal_summary.jsonl
  quarantine.jsonl
  scrapy.log
```

### AWS

When deployed on AWS, outputs are written to S3:

```text
s3://<S3_BUCKET_NAME>/avature/bronze/jobs/run_date=<YYYY-MM-DD>/run_id=<RUN_ID>/jobs.jsonl
s3://<S3_BUCKET_NAME>/avature/bronze/quarantine/run_date=<YYYY-MM-DD>/run_id=<RUN_ID>/quarantine.jsonl
s3://<S3_BUCKET_NAME>/avature/ops/runs/run_date=<YYYY-MM-DD>/run_id=<RUN_ID>/metrics.json
s3://<S3_BUCKET_NAME>/avature/ops/runs/run_date=<YYYY-MM-DD>/run_id=<RUN_ID>/run_manifest.json
s3://<S3_BUCKET_NAME>/avature/ops/portal_summaries/run_date=<YYYY-MM-DD>/run_id=<RUN_ID>/portal_summary.jsonl
```

### Artifact roles

- `jobs.jsonl` — exported valid job records
- `metrics.json` — full Scrapy stats snapshot plus custom crawl/pipeline metrics
- `run_manifest.json` — run lineage summary (run metadata), counts, timestamps, input fingerprint, and artifact URIs
- `portal_summary.jsonl` — portal-level crawl breakdowns and completeness metrics
- `quarantine.jsonl` — invalid records that failed hard validation checks

---

## Metrics

### Primary coverage metric

- **Coverage** = number of exported unique jobs, measured by stable `job_hash`

### Crawl funnel metrics

- `crawl/jobs_discovered_total`
- `crawl/job_detail_requests_total`
- `item_scraped_count` / `pipeline/jobs_exported_total`
- `pipeline/jobs_quarantined_total`
- `pipeline/duplicates_dropped_total`
- `pipeline/dynamodb_duplicates_dropped`

### Quality and observability metrics

- `crawl/job_detail_success_rate` - successful job-detail parses divided by job-detail requests
- `crawl/job_detail_parse_exception_total`
- `crawl/job_detail_parse_failure_total` (funnel-loss approximation)
- per-field completeness percentages from the pipeline
- request/response counts by request kind (`listing`, `pagination`, `job_detail`)
- response buckets by request kind (`2xx`, `3xx`, `4xx`, `5xx`)

### Artifact notes

- `metrics.json` is the full run-level stats snapshot
- `portal_summary.jsonl` contains high-cardinality portal-level breakdowns for downstream analysis
- CloudWatch EMF intentionally stays **low-cardinality** to control cost

---

## Validation and quarantine flow

Each item passes through a validation stage before it is accepted as a final exported record.

### Validation behavior

- required fields such as `title`, `description_text`, and stable identity fields are checked
- fields such as `posted_date`, `remote`, and `locations` are normalized
- missing non-critical fields produce `validation_warnings`
- missing critical fields produce `validation_errors`

### Outcome rules

- records with only warnings remain `record_status = "valid"` and are exported to `jobs.jsonl`
- records with hard validation failures are marked `record_status = "quarantined"`, written to `quarantine.jsonl`, and dropped from the final exported dataset

This gives the pipeline a clean separation between:
- **valid exported records**
- **invalid quarantined records**


## Architecture

<img width="4544" height="2690" alt="aws-ETL-pipeline-diagram" src="https://github.com/user-attachments/assets/95f6dda3-fdcc-403f-a81b-f2a40777197d" />

Scraper-side ownership is intentionally split as follows:

- **Spider** — extraction, canonical URL handling, `portal_key`, and request classification
- **ValidationPipeline** — normalization, warnings/errors, and quarantine
- **JobPipeline** — exported-item metrics, EMF emission, and `run_manifest.json`
- **CrawlMetricsExtension** — crawl telemetry, `metrics.json`, and `portal_summary.jsonl`

---

## Project structure

```text
.
├── scrapy.cfg
├── seed_urls.csv          # REQUIRED: List of Avature portal URLs to scrape
├── data/
│   └── smoke_seed_urls.csv # Curated low-volume portals for the local smoke command
├── requirements.txt
├── pyproject.toml          # Project config, dependencies (uv), and QA (ruff, ty, pre-commit)
├── Dockerfile
├── core/                   # Scrapy spider and pipeline logic
│   ├── items.py
│   ├── settings.py
│   ├── pipelines.py
│   ├── seed_io.py
│   ├── tools/
│   │   ├── seed_audit.py   # Checks seed URLs for accessibility, correct structure, and crawlability before a run
│   │   └── smoke.py
│   ├── extensions.py       # Scrapy extension for emitting crawl-level metrics and portal summaries
│   └── spiders/
│       └── avature_spider.py
├── infra/                  # AWS CDK Infrastructure as Code
│   ├── environments/
│   │   ├── dev.yaml
│   │   └── prod.yaml
│   ├── stacks/             # CDK stacks for modular infrastructure components
│   ├── sql/                # Athena bootstrap queries and named queries
│   ├── tests/              # Unit tests for CDK constructs
│   ├── app.py              # Main CDK app entry point
│   └── bootstrap_app.py    # CDK app for GitHub OIDC bootstrap stack
├── tests/                  # Scraper, pipeline, and CLI unit tests
│   ├── unit/
│   └── fixtures/           # Saved HTML fixtures
└── output/
    └── run_<RUN_ID>/...    # job exports, metrics, and logs for each run

```

---

## Local development

### Prerequisites

* Python **3.13+**
* [`uv`](https://docs.astral.sh/uv/) recommended

### 1. Create and activate a virtual environment

```bash
uv venv
source .venv/bin/activate
```

### 2. Install dependencies

For local development, use `uv` and the project metadata in [`pyproject.toml`](pyproject.toml):

```bash
uv sync --group local --group infra --group dev
```

This installs the scraper runtime, infra test dependencies, and QA tooling used by the repository.

If you prefer a plain virtual environment for the scraper runtime only:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure target URLs

The spider reads listing page URLs from a root-level [`seed_urls.csv`](seed_urls.csv) (a sample file is included in the repository).

Add the target Avature career portal listing URLs to the required `url` column in [`seed_urls.csv`](seed_urls.csv). The spider derives its allowed domains from the URLs in this file.

Example:

```csv
url
https://example.avature.net/careers/SearchJobs
https://another-company.avature.net/en_US/careers/SearchJobs
```


### 4. Create environment configuration:

Copy the environment example file:

```bash
cp .env.example .env
```

Start with something like:

```env
PROJECT_NAME=avature-etl
ENV_NAME=dev
DEPLOY_ENV=local

LOG_LEVEL=INFO
LOGSTATS_INTERVAL=30
LOG_FILE=scrapy.log

METRICS_FILE=metrics.json
METRICS_DUMP_INTERVAL=30

SCRAPY_FEED_NAME=jobs.jsonl
SCRAPY_FEED_OVERWRITE=1
```

See [`.env.example`](.env.example) for the full list of supported runtime settings.

### 5. Run the spider

```bash
scrapy crawl avature
```

### 6. Inspect outputs

```bash
ls -la output/
ls -td output/run_* | head -1 # latest run directory
```

A healthy run usually shows:

- `jobs.jsonl` populated with valid records
- `metrics.json` containing both Scrapy stats and custom crawl/pipeline stats
- `run_manifest.json` summarizing run lineage and artifact paths
- `portal_summary.jsonl` with one row per scraped portal
- `quarantine.jsonl` either empty or containing only invalid records that failed hard validation

### 7. Run QA and reliability tooling

Run the full test and code-quality suite:

```bash
pytest -q
ruff check .
ty check
```

`pytest -q` includes the infra test suite under [`infra/tests`](infra/tests), so you should have Node.js available on `PATH` for CDK/jsii.

Audit a seed file before a crawl:

```bash
uv run python -m core.tools.seed_audit seed_urls.csv
```

This writes a machine-readable report and a recommended seed CSV under `output/seed_audit/<timestamp>/`.

Run a small live smoke crawl against the curated low-volume portals in [`data/smoke_seed_urls.csv`](data/smoke_seed_urls.csv):

```bash
uv run python -m core.tools.smoke
```

The smoke command writes to `output/smoke_<timestamp>/` and fails if the crawl does not finish, exports zero jobs, or misses the configured quality thresholds.

---

## Running with Docker

The Docker image is built from the repository root.

Build locally:

```bash
docker build -t avature-etl:local .
```

Run with explicit local mounts ([`seed_urls.csv`](seed_urls.csv), `.env`, and `output/`):

```bash
docker run --rm \
  --env-file .env \
  -v "$(pwd)/seed_urls.csv:/app/seed_urls.csv:ro" \
  -v "$(pwd)/output:/app/output" \
  avature-etl:local
```

---

## Running on AWS

This project runs on AWS as a **containerized batch scraping workload**.
Infrastructure is provisioned with **AWS CDK** from the `infra/` directory and organized into modular stacks across environments such as `dev` and `prod`.

- **ECR stack** — shared Amazon ECR repository with immutable tags and image scanning
- **Base stack** — S3 output bucket, DynamoDB dedupe table, and CloudWatch log group
- **ECS stack** — ARM64 ECS Fargate task definition and runtime for the scraper
- **Workflow stack** — EventBridge Scheduler, Step Functions orchestration for daily runs, manual overrides, silver promotion, and history snapshot promotion
- **Notifications stack** — SNS topic for operational alerts
- **Runtime alarm stack** — CloudWatch alarms for scraper and workflow health
- **Analytics stack** — Glue database, Athena workgroup, and named queries for bronze / silver / gold analytics plus history/change views
- **Cost guardrails stack** — AWS Budget alerting for spend visibility

### Environment-specific configuration

Environment-specific settings live in:

- [`infra/environments/dev.yaml`](infra/environments/dev.yaml)
- [`infra/environments/prod.yaml`](infra/environments/prod.yaml)

These files control ECS sizing, workflow behavior, alert routing, Athena guardrails, budget settings, and scraper runtime parameters.

For full deployment instructions, including manual AWS deployment and GitHub Actions CI/CD setup, see:

- [`docs/deployment/aws-deployment.md`](docs/deployment/aws-deployment.md)

For post-deployment operational guidance, including Athena bootstrap, manual workflow runs, alert handling, rollback basics, and the daily history snapshot flow, see:

- [`docs/runbooks/avature-aws-runbook.md`](docs/runbooks/avature-aws-runbook.md)


### Cross-run deduplication via DynamoDB

Scrapy’s built-in dedupe is run-local only. For AWS runs, this project adds a DynamoDB-backed idempotency layer:

- each job gets a stable `job_hash`
- a conditional write is used before accepting the item
- previously seen hashes are dropped
- TTL prevents unbounded table growth

### Operational notes

* ECS/Fargate is configured for `ARM64` for cost/performance efficiency.
* In production, the **workflow** owns the schedule (`schedule_target: workflow`).
* The ECS stack uses the default VPC, so the target AWS account must have one unless the infrastructure is changed.
* Athena bootstrap queries must be executed once after deploy before the analytics layer is live.
* The scheduled workflow now runs both the silver incremental insert and the history snapshot incremental insert after each ECS crawl.

> Note: If you build images manually, publish either an `ARM64` image or a multi-arch image that includes `linux/arm64`.
> By default, ECS/Fargate tasks run on `X86_64` unless `runtimePlatform` is explicitly set.

## Screenshots

### Step Functions workflow

<details>
<summary>Shows a successful end-to-end workflow execution: ECS scrape → Athena silver promotion → completion</summary>

<img width="3490" height="1494" alt="stepfunctions_graph_Avature-ATS-ETL" src="https://github.com/user-attachments/assets/a7b54e67-7346-46e8-8859-1039413c395b" />

</details>

### S3 output

#### S3 job export

<details>
<summary>Shows Hive-partitioned outputs generated by the ECS task</summary>

<img width="1121" height="344" alt="s3-lake-jobs" src="https://github.com/user-attachments/assets/a699c21d-3049-4a0a-bb16-006c496bb70a" />

</details>

#### S3 ops artifacts

<details>
<summary>Shows run-level artifacts such as metrics and run manifest</summary>

<img width="1119" height="389" alt="s3-lake-ops" src="https://github.com/user-attachments/assets/28478b9b-6a17-4a54-8816-4caf781aa73e" />

</details>


<details>
<summary>Shows portal summary artifacts with portal-level breakdowns and completeness metrics</summary>

<img width="1066" height="305" alt="s3-lake-portal-summaries" src="https://github.com/user-attachments/assets/3aeb613a-99b3-4bb7-b3f0-30081aa80a84" />

</details>

#### S3 quarantine artifact

<details>
<summary>Shows quarantined records that failed validation checks</summary>

<img width="1120" height="342" alt="s3-lake-quarantine" src="https://github.com/user-attachments/assets/a6ef9446-383f-4a60-ad02-f30a2fa6af54" />

</details>

### DynamoDB dedupe

<details>
<summary>Shows stored <code>job_hash</code> keys preventing future duplicates</summary>

<img width="570" height="386" alt="dynamoDB-dedupe" src="https://github.com/user-attachments/assets/aa97dcc3-06dd-435e-9a38-1b67eef54cdf" />

</details>

### CloudWatch

#### CloudWatch Dashboard

<details>
<summary>CloudWatch dashboard to visualize EMF</summary>

<img width="1501" height="481" alt="cloudwatch-dashboard" src="https://github.com/user-attachments/assets/159b4e77-b427-4b41-9b16-8837561d4d50" />

</details>

#### CloudWatch logs and metrics

<details>
<summary>Shows structured logs and emitted metrics from the ECS task</summary>
  <img width="1033" height="639" alt="cloudwatch-logs" src="https://github.com/user-attachments/assets/ed9b6b52-e7aa-4fe5-a671-69184177643e" />
</details>

#### CloudWatch Alarms

<details>
<summary>Shows example alarms for failed scheduler invocations and runtime errors</summary>
  <img width="1354" height="663" alt="cloudwatch-alarms" src="https://github.com/user-attachments/assets/01d91668-abcf-49c1-8aef-fac77f516927" />
</details>

### Athena Analytics

<details>
<summary>Job portal summary query execution</summary>
  <img width="1475" height="675" alt="athena-portal-summary" src="https://github.com/user-attachments/assets/31a715e4-93bb-4764-8a8c-1c38a125f133" />
</details>
