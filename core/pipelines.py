import json
import time
from collections import defaultdict
from types import ModuleType
from typing import Any
from urllib.parse import urlparse

# useful for handling different item types with a single interface
from scrapy import signals
from scrapy.exceptions import DropItem

# class CorePipeline:
#     def process_item(self, item, spider):
#         return item


class JobPipeline:
    """Track and report metrics for scraped JobItem objects."""

    def __init__(self):
        # Counters keyed by field name
        self.stats = None
        self.field_counts: dict[str, int] = defaultdict(int)
        self.total_items = 0
        self.seen_ids = set()
        self.seen_keys = set()
        self.duplicates_dropped = 0
        self.hosts = set()
        self.portals = set()

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        # Give the pipeline access to the crawler's stats collector for logging metrics
        pipeline.stats = crawler.stats
        # Connect signals
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        spider.logger.info("JobPipeline opened.")

    def spider_closed(self, spider, reason):
        # At the end of the crawl compute unique counts and log them
        metrics = {
            "unique_jobs_scraped": self.total_items,
            "duplicate_items_dropped": self.duplicates_dropped,
            "unique_hosts": len(self.hosts),
            "unique_portals": len(self.portals),
        }
        # Per‑field completeness
        for field, count in self.field_counts.items():
            metrics[f"jobs_with_{field}"] = count
        spider.logger.info("JobPipeline summary: %s", metrics)

        # Save pipeline summary into Scrapy stats so extensions/exporters can include it
        if self.stats:
            self.stats.set_value("pipeline/summary", metrics)

        # Optional: also expose flat keys (for dashboards / filters)
        if self.stats:
            for k, v in metrics.items():
                self.stats.set_value(f"pipeline/{k}", v)

        deploy_env = spider.settings.get("DEPLOY_ENV").lower()

        if deploy_env != "aws":
            return

        metrics_s3_uri = spider.settings.get("METRICS_S3_URI")
        if not metrics_s3_uri:
            spider.logger.error("METRICS_S3_URI not set; cannot upload metrics.json")
            return

        parsed = urlparse(metrics_s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        try:
            import boto3

            s3 = boto3.client("s3")

            final_stats = (self.stats.get_stats() if self.stats else {}) or {}
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(final_stats, indent=2, ensure_ascii=False, default=str).encode("utf-8"),
                ContentType="application/json",
            )
            spider.logger.info("Uploaded metrics.json to s3://%s/%s", bucket, key)
        except Exception as e:
            spider.logger.error("Failed to upload metrics.json to S3: %s", e)

        # Write these pipeline-specific metrics to a JSON file
        # try:
        #     # Resolve the output directory (defaults to "output")
        #     output_dir = Path("output")
        #     output_dir.mkdir(parents=True, exist_ok=True)
        #
        #     output_file = output_dir / "pipeline_metrics.json"
        #     output_file.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        #
        #     spider.logger.info("Pipeline metrics saved to %s", output_file)
        # except Exception as e:
        #     spider.logger.error("Failed to write pipeline metrics to file: %s", e)

    def process_item(self, item, spider=None):
        # Deduplicate by job_id if present.  If no job_id is provided
        # duplicates will not be detected.  This logic can be replaced
        # with more sophisticated mechanisms (e.g. canonical URL) if
        # needed.

        # todo: remove it
        # job_id = item.get('job_id')
        # if job_id:
        #     if job_id in self.seen_ids:
        #         self.duplicates_dropped += 1
        #         raise DropItem(f"Duplicate job_id {job_id} dropped")
        #     self.seen_ids.add(job_id)

        # Deduplicate by job_hash (preferred), fallback to job_id.
        dedupe_key = item.get("job_hash")
        if dedupe_key:
            if dedupe_key in self.seen_keys:
                self.duplicates_dropped += 1
                raise DropItem(f"Duplicate item dropped (key={dedupe_key})")
            self.seen_keys.add(dedupe_key)

        # Count fields present
        self.total_items += 1
        for key in ["description_text", "locations", "posted_date", "apply_url"]:
            value = item.get(key)
            if value:
                # For list fields check if non‑empty
                if isinstance(value, list) and not value:
                    continue
                self.field_counts[key] += 1

        # Track host and portal for high level metrics
        url = item.get("source_url")
        if url:
            from urllib.parse import urlparse

            parsed = urlparse(url)
            if parsed.hostname:
                self.hosts.add(parsed.hostname)
                parts = [p for p in parsed.path.split("/") if p]
                base = "/".join(parts[:2]) if parts else ""
                self.portals.add(f"{parsed.hostname}/{base}")
        return item


try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3: ModuleType | None = None
    ClientError: type | None = None


class DynamoDBDedupePipeline:
    """
    Drops items if their job_hash has already been seen in previous AWS runs.

    Table schema:
      - Partition key: job_hash (String)
      - Optional TTL attribute: expires_at (Number, epoch seconds)
    """

    deploy_env: str
    table_name: str | None
    ttl_days: int
    fail_open: bool
    table: Any
    _local_seen: set

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls.__new__(cls)  # bypass __init__
        obj.deploy_env = crawler.settings.get("DEPLOY_ENV", "local").lower()
        obj.table_name = crawler.settings.get("DYNAMODB_TABLE_NAME")
        obj.ttl_days = crawler.settings.get("DYNAMODB_TTL_DAYS")

        # DynamoDB error handling: fail-fast by default to maintain idempotency.
        # fail_open=False (default): exception is raised, drops that single item and continues processing other items
        # fail_open=True: item is allowed through despite the DynamoDB error, processing continues with the next item
        obj.fail_open = crawler.settings.getbool("DDB_DEDUPE_FAIL_OPEN", False)

        obj.table = None
        obj._local_seen = set()  # reduce extra DDB calls within the same run
        return obj

    def open_spider(self, spider):
        if self.deploy_env != "aws":
            return

        if boto3 is None or ClientError is None:
            raise RuntimeError(
                "boto3/botocore not installed but DEPLOY_ENV=aws. Install boto3 (and botocore) in the container."
            )

        dynamodb = boto3.resource("dynamodb")
        self.table = dynamodb.Table(self.table_name)
        spider.logger.info("DynamoDB dedupe enabled (table=%s, ttl_days=%s)", self.table_name, self.ttl_days)

    def process_item(self, item, spider):
        # Not in AWS mode or table not initialized => no-op
        if not self.table:
            return item

        job_hash = item.get("job_hash") or item.get("job_id")
        if not job_hash:
            return item

        # In-run fast dedupe to avoid extra DynamoDB writes
        if job_hash in self._local_seen:
            spider.crawler.stats.inc_value("pipeline/dynamodb_duplicates_dropped")
            raise DropItem(f"Duplicate within run (job_hash={job_hash})")
        self._local_seen.add(job_hash)

        expires_at = int(time.time()) + self.ttl_days * 86400

        try:
            self.table.put_item(
                Item={
                    "job_hash": job_hash,
                    "first_seen_ts": int(time.time()),
                    "expires_at": expires_at,
                },
                ConditionExpression="attribute_not_exists(job_hash)",
            )
            return item

        except Exception as e:
            # ClientError is only available when boto3 is installed;
            # at this point open_spider already verified it's not None.
            if ClientError is not None and not isinstance(e, ClientError):
                raise
            code = e.response.get("Error", {}).get("Code", "")  # type: ignore[union-attr]
            if code == "ConditionalCheckFailedException":
                spider.crawler.stats.inc_value("pipeline/dynamodb_duplicates_dropped")
                raise DropItem(f"Duplicate across runs (job_hash={job_hash})") from None

            # Real error: permissions/table missing/throttle/etc.
            spider.logger.error("DynamoDB put_item error (%s): %s", code, e)

            # Best practice: fail-fast to keep idempotency guarantees,
            # unless explicitly allow fail-open.
            if self.fail_open:
                return item
            raise
