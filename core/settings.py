import logging
import os
from datetime import UTC, datetime
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=".env", override=False)
    logging.info("Loaded local .env file.")
except ImportError:
    logging.info("Running in production mode. Relying on system environment variables.")

BOT_NAME = "core"

SPIDER_MODULES = ["core.spiders"]
NEWSPIDER_MODULE = "core.spiders"

ADDONS = {}

# -----------------------------------------------------------------------------
# Dynamic Run Routing (Local vs AWS)
# -----------------------------------------------------------------------------
DEPLOY_ENV = os.getenv("DEPLOY_ENV", "local").lower()

now_utc = datetime.now(UTC)
DATE_STR = now_utc.strftime("%Y-%m-%d")
TS_STR = now_utc.strftime("%Y%m%d_%H%M%S")

# AWS: run_id is DATE, Local: run_id is timestamped by default
RUN_ID = os.getenv("RUN_ID") or (DATE_STR if DEPLOY_ENV == "aws" else TS_STR)

# Where logs/metrics should go locally (AWS uses stdout logs, and uploads metrics to S3)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_RUN_DIR = Path(os.getenv("RUN_DIR", str(PROJECT_ROOT / "output" / f"run_{RUN_ID}")))


# -----------------------------------------------------------------------------
# Logging + stats
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"

# optionally comment out LOG_FILE to disable logging to file and just log to console
# use this below to log to file and console at the same time (with uv):
# uv run scrapy crawl avature 2>&1 | tee output/spider.log
# Disable file logging in AWS (CloudWatch captures stdout)
_log_file_env = os.getenv("LOG_FILE", "scrapy.log")
if DEPLOY_ENV == "aws":
    LOG_FILE = None
else:
    LOCAL_RUN_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE = (LOCAL_RUN_DIR / _log_file_env) if _log_file_env else None

# Periodic crawl stats (requests, items/sec, etc.)
LOGSTATS_INTERVAL = float(os.getenv("LOGSTATS_INTERVAL", "30"))
STATS_DUMP = True

# Disable telnet console in normal use
# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Concurrency and throttling settings
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", "8"))
CONCURRENT_REQUESTS_PER_DOMAIN = int(os.getenv("CONCURRENT_REQUESTS_PER_DOMAIN", "4"))

DOWNLOAD_DELAY = float(os.getenv("DOWNLOAD_DELAY", "1.0"))
RANDOMIZE_DOWNLOAD_DELAY = True
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "30"))

# AutoThrottle helps with heterogeneous portals / soft rate limits
# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = os.getenv("AUTOTHROTTLE_ENABLED", "1") == "1"
# The initial download delay
AUTOTHROTTLE_START_DELAY = float(os.getenv("AUTOTHROTTLE_START_DELAY", "1.0"))
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = float(os.getenv("AUTOTHROTTLE_MAX_DELAY", "20.0"))
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = float(os.getenv("AUTOTHROTTLE_TARGET_CONCURRENCY", "1.0"))
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False

# -----------------------------------------------------------------------------
# Default headers / UA
# -----------------------------------------------------------------------------
# or use a pool of user agents and rotate them in a custom downloader middleware for better anti-scraping evasion
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
)

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

# Obey robots.txt rules
ROBOTSTXT_OBEY = os.getenv("ROBOTSTXT_OBEY", "0") == "1"

# Public job pages don't need cookies
# Disable cookies (enabled by default)
COOKIES_ENABLED = os.getenv("COOKIES_ENABLED", "0") == "1"
REDIRECT_ENABLED = True

# -----------------------------------------------------------------------------
# Retry / transient failures
# -----------------------------------------------------------------------------
RETRY_ENABLED = True
RETRY_TIMES = int(os.getenv("RETRY_TIMES", "4"))
RETRY_HTTP_CODES = [408, 429, 500, 502, 503, 504, 522, 524]


# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
if DEPLOY_ENV == "aws":
    HTTPCACHE_ENABLED = False
else:
    HTTPCACHE_ENABLED = os.getenv("HTTPCACHE_ENABLED", "0") == "1"
    # HTTPCACHE_EXPIRATION_SECS = 0
    HTTPCACHE_DIR = os.getenv("HTTPCACHE_DIR", ".httpcache")
    HTTPCACHE_IGNORE_HTTP_CODES = [400, 401, 403, 404, 405, 410, 429, 500, 502, 503, 504, 522, 524]
    HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"


# -----------------------------------------------------------------------------
# FEEDS routing (Local file vs S3)
# -----------------------------------------------------------------------------
SCRAPY_FEED_NAME = os.getenv("SCRAPY_FEED_NAME", "jobs.jsonl")

FEED_EXPORT_ENCODING = "utf-8"
FEED_STORE_EMPTY = False

METRICS_FILE = os.getenv("METRICS_FILE", "metrics.json")

if DEPLOY_ENV == "aws":
    # Required in ECS task env
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

    if not S3_BUCKET_NAME:
        raise RuntimeError("S3_BUCKET_NAME must be set when DEPLOY_ENV=aws")

    # Example: avature/dt=2026-03-05/jobs.jsonl
    S3_OUTPUT_PREFIX = os.getenv("S3_OUTPUT_PREFIX", f"avature/dt={RUN_ID}")

    JOBS_FEED_URI = f"s3://{S3_BUCKET_NAME}/{S3_OUTPUT_PREFIX}/{SCRAPY_FEED_NAME}"
    METRICS_S3_URI = f"s3://{S3_BUCKET_NAME}/{S3_OUTPUT_PREFIX}/{METRICS_FILE}"

    JOBDIR = None
    SCHEDULER_PERSIST = False

    DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
    if not DYNAMODB_TABLE_NAME:
        raise RuntimeError("DYNAMODB_TABLE_NAME must be set when DEPLOY_ENV=aws")

    DYNAMODB_TTL_DAYS = int(os.getenv("DYNAMODB_TTL_DAYS", "60"))
    DDB_DEDUPE_FAIL_OPEN = os.getenv("DDB_DEDUPE_FAIL_OPEN", "0") == "1"

    # Run DynamoDB deduplication FIRST, then standard metrics pipeline
    ITEM_PIPELINES = {
        "core.pipelines.DynamoDBDedupePipeline": 100,
        "core.pipelines.JobPipeline": 300,
    }

    # Disable periodic extension in AWS for now
    EXTENSIONS = {"core.extensions.StatsDumpExtension": None}
else:
    # Local run directory output
    JOBS_FEED_URI = str(LOCAL_RUN_DIR / SCRAPY_FEED_NAME)
    METRICS_S3_URI = None

    # Scrapy's JOBDIR persists the scheduler + dupefilter fingerprints.
    # This enables resuming after crash/kill without re-requesting the same pages.
    # Set SCRAPY_JOBDIR="" to disable persistence.
    _jobdir = os.getenv("SCRAPY_JOBDIR", "jobstate")
    JOBDIR = _jobdir if _jobdir else None
    SCHEDULER_PERSIST = True

    # Configure item pipelines
    # See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
    ITEM_PIPELINES = {
        "core.pipelines.JobPipeline": 300,
    }

    EXTENSIONS = {"core.extensions.StatsDumpExtension": 500}
    METRICS_DUMP_PATH = str(LOCAL_RUN_DIR / METRICS_FILE)
    METRICS_DUMP_INTERVAL = int(os.getenv("METRICS_DUMP_INTERVAL", "30"))

# IMPORTANT:
# - S3 does not support append, so overwrite must be True for s3:// feeds.
# - Local: may overwrite via env; default True since run dir is unique.
SCRAPY_FEED_OVERWRITE = os.getenv("SCRAPY_FEED_OVERWRITE", "1") == "1"

# https://docs.scrapy.org/en/latest/topics/feed-exports.html#feeds
FEEDS = {
    JOBS_FEED_URI: {
        "format": "jsonlines",
        "encoding": "utf-8",
        "overwrite": True if DEPLOY_ENV == "aws" else SCRAPY_FEED_OVERWRITE,
        "item_export_kwargs": {"ensure_ascii": False},
    }
}

# Keep Scrapy’s modern request fingerprinting behavior stable across versions.
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
