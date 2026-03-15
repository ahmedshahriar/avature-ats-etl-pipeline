import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import urlparse

from dateutil import parser as date_parser
from scrapy import signals
from scrapy.exceptions import DropItem


class ValidationPipeline:
    """Normalize records, enforce minimum quality, and quarantine invalid rows."""

    def __init__(self):
        self.stats = None
        self.deploy_env = "local"
        self.quarantine_path: Path | None = None
        self.quarantine_s3_uri: str | None = None
        self._fh = None
        self.jobs_quarantined_total = 0
        self.validation_warning_total = 0

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls()
        obj.stats = crawler.stats
        obj.deploy_env = (crawler.settings.get("DEPLOY_ENV") or "local").lower()
        obj.quarantine_path = Path(crawler.settings.get("QUARANTINE_LOCAL_PATH") or "output/quarantine.jsonl")
        obj.quarantine_s3_uri = crawler.settings.get("QUARANTINE_S3_URI")
        crawler.signals.connect(obj.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(obj.spider_closed, signal=signals.spider_closed)
        return obj

    def spider_opened(self, spider):
        if self.quarantine_path:
            self.quarantine_path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = self.quarantine_path.open("a", encoding="utf-8")
        spider.logger.info("ValidationPipeline opened (quarantine=%s)", self.quarantine_path)

    def spider_closed(self, spider, reason):
        if self._fh:
            self._fh.close()
            self._fh = None

        summary = {
            "jobs_quarantined_total": self.jobs_quarantined_total,
            "validation_warning_total": self.validation_warning_total,
            "quarantine_local_path": str(self.quarantine_path) if self.quarantine_path else None,
            "quarantine_s3_uri": self.quarantine_s3_uri,
        }
        if self.stats:
            self.stats.set_value("pipeline/jobs_quarantined_total", self.jobs_quarantined_total)
            self.stats.set_value("pipeline/validation_warning_total", self.validation_warning_total)
            self.stats.set_value("pipeline/validation_summary", summary)

        if (
            self.deploy_env == "aws"
            and self.quarantine_path
            and self.quarantine_path.exists()
            and self.quarantine_s3_uri
        ):
            parsed = urlparse(self.quarantine_s3_uri)
            try:
                import boto3

                boto3.client("s3").upload_file(str(self.quarantine_path), parsed.netloc, parsed.path.lstrip("/"))
                spider.logger.info("Uploaded quarantine.jsonl to %s", self.quarantine_s3_uri)
            except Exception as exc:  # pragma: no cover
                spider.logger.error("Failed to upload quarantine.jsonl to S3: %s", exc)

    def process_item(self, item, spider=None):
        errors: list[str] = []
        warnings: list[str] = []

        item["canonical_source_url"] = (item.get("canonical_source_url") or item.get("source_url") or "").strip()
        item["source_url"] = item["canonical_source_url"]
        item["raw_source_url"] = (item.get("raw_source_url") or item.get("source_url") or "").strip()

        item["title"] = (item.get("title") or "").strip() or None
        item["description_text"] = (item.get("description_text") or "").strip() or None
        item["company"] = (item.get("company") or "").strip() or None
        item["apply_url"] = (item.get("apply_url") or "").strip() or None

        locations = item.get("locations") or []
        if isinstance(locations, str):
            locations = [locations]
        clean_locations: list[str] = []
        seen_locations: set[str] = set()
        for loc in locations:
            cleaned = " ".join(str(loc).split()).strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen_locations:
                continue
            seen_locations.add(key)
            clean_locations.append(cleaned)
        item["locations"] = clean_locations

        remote = str(item.get("remote") or "").strip().lower()
        if not remote:
            item["remote"] = "unknown"
        elif "hybrid" in remote:
            item["remote"] = "hybrid"
        elif any(token in remote for token in ["yes", "remote", "work from home", "wfh"]):
            item["remote"] = "remote"
        elif any(token in remote for token in ["no", "onsite", "on-site", "office"]):
            item["remote"] = "onsite"
        else:
            item["remote"] = "unknown"

        posted_date = item.get("posted_date")
        if posted_date:
            normalized = self._normalize_date(posted_date)
            item["posted_date"] = normalized or str(posted_date).strip()

        # quarantine if missing critical fields required for identity or basic understanding of the job
        if not item.get("title"):
            errors.append("missing_title")
        if not item.get("description_text"):
            errors.append("missing_description_text")
        if not item.get("canonical_source_url"):
            errors.append("missing_canonical_source_url")
        if not (item.get("job_id") or item.get("canonical_source_url")):
            errors.append("missing_job_identity")

        # validation warnings
        if not item.get("apply_url"):
            warnings.append("missing_apply_url")
        if not item.get("company"):
            warnings.append("missing_company")
        if not item.get("locations"):
            warnings.append("missing_locations")
        if not item.get("posted_date"):
            warnings.append("missing_posted_date")

        item["validation_errors"] = errors
        item["validation_warnings"] = warnings
        item["record_status"] = "quarantined" if errors else "valid"
        self.validation_warning_total += len(warnings)

        if errors:
            self.jobs_quarantined_total += 1
            if self.stats:
                self.stats.inc_value("pipeline/jobs_quarantined_total")
            if self._fh:
                payload = {
                    "quarantined_at": datetime.now(UTC).isoformat(),
                    "errors": errors,
                    "warnings": warnings,
                    "item": dict(item),
                }
                self._fh.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
                self._fh.flush()
            raise DropItem(f"Validation failed: {', '.join(errors)}")

        return item

    @staticmethod
    def _normalize_date(value: Any) -> str | None:
        text = str(value).strip()
        if not text:
            return None
        if date_parser is not None:
            try:
                return date_parser.parse(text).date().isoformat()
            except Exception:
                return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            return None


class JobPipeline:
    """Track final exported items, emit lean EMF, and upload run artifacts."""

    TRACKED_COMPLETENESS_FIELDS = ("title", "description_text", "locations", "posted_date", "apply_url", "company")

    def __init__(self):
        self.stats = None
        self.field_counts: dict[str, int] = defaultdict(int)
        self.total_items = 0
        self.seen_keys: set[str] = set()
        self.duplicates_dropped = 0
        self.hosts: set[str] = set()
        self.portals: set[str] = set()

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

    def _stats_get_value(self, key: str, default: Any = None) -> Any:
        if not self.stats:
            return default
        return self.stats.get_value(key, default)

    def spider_closed(self, spider, reason):
        metrics = {
            "jobs_exported_total": self.total_items,
            "duplicates_dropped_total": self.duplicates_dropped,
            "ddb_duplicates_dropped_total": int(self._stats_get_value("pipeline/dynamodb_duplicates_dropped") or 0),
            "jobs_quarantined_total": int(self._stats_get_value("pipeline/jobs_quarantined_total") or 0),
            "unique_hosts": len(self.hosts),
            "unique_portals": len(self.portals),
        }

        for field in self.TRACKED_COMPLETENESS_FIELDS:
            count = self.field_counts.get(field, 0)
            metrics[f"{field}_present_total"] = count
            metrics[f"{field}_completeness_pct"] = round(100.0 * count / self.total_items, 4) if self.total_items else 0

        if self.stats:
            self.stats.set_value("pipeline/summary", metrics)

        # Optional: also expose flat keys (for dashboards / filters)
        if self.stats:
            for k, v in metrics.items():
                self.stats.set_value(f"pipeline/{k}", v)

        deploy_env = (spider.settings.get("DEPLOY_ENV") or "local").lower()

        job_detail_requests_total = int(self._stats_get_value("crawl/job_detail_requests_total") or 0)
        started_at_ts = self._stats_get_value("run/started_at_ts")
        jobs_discovered_total = int(self._stats_get_value("crawl/jobs_discovered_total") or 0)

        run_duration_seconds = round(max(0.0, time.time() - float(started_at_ts)), 3) if started_at_ts else 0.0
        jobs_quarantined_total = int(metrics["jobs_quarantined_total"])
        duplicate_items_total = int(metrics["duplicates_dropped_total"]) + int(metrics["ddb_duplicates_dropped_total"])
        job_detail_success_rate = (
            round(self.total_items / job_detail_requests_total, 6) if job_detail_requests_total else None
        )

        final_stats = (self.stats.get_stats() if self.stats else {}) or {}

        # --- Emit CloudWatch EMF ---
        if deploy_env == "aws":
            # --- Emit CloudWatch EMF as a raw JSON log line ---
            # Keep dimensions LOW cardinality. Do NOT include run_id / job_id / URLs.
            project = spider.settings.get("PROJECT_NAME", "avature-etl")
            stage = spider.settings.get("ENV_NAME") or spider.settings.get("DEPLOY_ENV", "dev")
            spider_name = spider.name

            emf_payload = {
                "_aws": {
                    "Timestamp": int(time.time() * 1000),
                    "CloudWatchMetrics": [
                        {
                            "Namespace": "AvatureETL",
                            "Dimensions": [["Project", "Stage", "Spider"]],
                            "Metrics": [
                                {"Name": "RunSuccess", "Unit": "Count"},
                                {"Name": "RunFailed", "Unit": "Count"},
                                {"Name": "JobsExported", "Unit": "Count"},
                                {"Name": "JobsQuarantined", "Unit": "Count"},
                                {"Name": "DuplicateItemsDropped", "Unit": "Count"},
                                {"Name": "JobDetailSuccessRate", "Unit": "None"},
                                {"Name": "RunDurationSeconds", "Unit": "Seconds"},
                            ],
                        }
                    ],
                },
                "Project": project,
                "Stage": stage,
                "Spider": spider_name,
                "RunSuccess": 1 if reason == "finished" else 0,
                "RunFailed": 0 if reason == "finished" else 1,
                "JobsExported": self.total_items,
                "JobsQuarantined": jobs_quarantined_total,
                "DuplicateItemsDropped": duplicate_items_total,
                "JobDetailSuccessRate": job_detail_success_rate,
                "RunDurationSeconds": run_duration_seconds,
            }

            # Important: EMF must be the raw log event JSON, not prefixed by logger formatting.
            print(json.dumps(emf_payload, ensure_ascii=False))

        self._write_or_upload_artifacts(
            spider=spider,
            final_stats=final_stats,
            reason=reason,
            run_duration_seconds=run_duration_seconds,
            job_detail_success_rate=job_detail_success_rate,
            job_detail_requests_total=job_detail_requests_total,
            jobs_discovered_total=jobs_discovered_total,
        )

    def _write_or_upload_artifacts(
        self,
        spider,
        final_stats: dict[str, Any],
        reason: str,
        *,
        run_duration_seconds: float | None,
        job_detail_success_rate: float | None,
        job_detail_requests_total: int,
        jobs_discovered_total: int,
    ) -> None:
        manifest = self._build_run_manifest(
            spider=spider,
            final_stats=final_stats,
            reason=reason,
            run_duration_seconds=run_duration_seconds,
            job_detail_success_rate=job_detail_success_rate,
            job_detail_requests_total=job_detail_requests_total,
            jobs_discovered_total=jobs_discovered_total,
        )
        manifest_body = json.dumps(manifest, indent=2, ensure_ascii=False, default=str).encode("utf-8")

        artifacts = [
            (
                spider.settings.get("RUN_MANIFEST_S3_URI"),
                spider.settings.get("RUN_MANIFEST_LOCAL_PATH"),
                manifest_body,
                "application/json",
            ),
        ]

        deploy_env = (spider.settings.get("DEPLOY_ENV") or "local").lower()
        if deploy_env != "aws":
            for _s3_uri, local_path, body, _content_type in artifacts:
                if not local_path:
                    continue
                out_path = Path(local_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(body)
            spider.logger.info("Wrote run_manifest artifacts")
            return

        try:
            import boto3

            s3 = boto3.client("s3")
            for s3_uri, _local_path, body, content_type in artifacts:
                if not s3_uri:
                    continue
                parsed = urlparse(s3_uri)
                s3.put_object(
                    Bucket=parsed.netloc,
                    Key=parsed.path.lstrip("/"),
                    Body=body,
                    ContentType=content_type,
                )
                spider.logger.info("Uploaded artifact to %s", s3_uri)
        except Exception as exc:  # pragma: no cover - depends on AWS runtime
            spider.logger.error("Failed to upload run artifacts to S3: %s", exc)

    def _build_run_manifest(
        self,
        spider,
        final_stats: dict[str, Any],
        reason: str,
        *,
        run_duration_seconds: float | None,
        job_detail_success_rate: float | None,
        job_detail_requests_total: int,
        jobs_discovered_total: int,
    ) -> dict[str, Any]:
        pipeline_summary = final_stats.get("pipeline/summary") or {}

        return {
            "run_id": spider.settings.get("RUN_ID"),
            "run_date": spider.settings.get("RUN_DATE"),
            "started_at": final_stats.get("run/started_at"),
            "finished_at": final_stats.get("run/finished_at") or datetime.now(UTC).isoformat(),
            "close_reason": reason,
            "project": spider.settings.get("PROJECT_NAME"),
            "stage": spider.settings.get("ENV_NAME"),
            "spider": spider.name,
            "deploy_env": spider.settings.get("DEPLOY_ENV"),
            "image_tag": spider.settings.get("IMAGE_TAG"),
            "git_sha": spider.settings.get("GIT_SHA"),
            "input_seed_file": final_stats.get("crawl/input_seed_file"),
            "input_file_sha256": final_stats.get("crawl/input_file_sha256"),
            "input_seed_count": final_stats.get("crawl/input_seed_count"),
            "run_duration_seconds": run_duration_seconds,
            "counts": {
                "jobs_discovered_total": jobs_discovered_total,
                "job_detail_requests_total": job_detail_requests_total,
                "jobs_exported_total": pipeline_summary.get(
                    "jobs_exported_total", final_stats.get("item_scraped_count", 0)
                ),
                "jobs_quarantined_total": final_stats.get("pipeline/jobs_quarantined_total", 0),
                "duplicates_dropped_total": (pipeline_summary.get("duplicates_dropped_total", 0) or 0)
                + (pipeline_summary.get("ddb_duplicates_dropped_total", 0) or 0),
            },
            "quality": {
                "job_detail_success_rate": job_detail_success_rate,
                "description_completeness_pct": pipeline_summary.get("description_text_completeness_pct"),
                "apply_url_completeness_pct": pipeline_summary.get("apply_url_completeness_pct"),
                "posted_date_completeness_pct": pipeline_summary.get("posted_date_completeness_pct"),
            },
            "artifacts": {
                "jobs_feed_uri": spider.settings.get("JOBS_FEED_URI"),
                "metrics_uri": spider.settings.get("METRICS_S3_URI") or spider.settings.get("METRICS_DUMP_PATH"),
                "run_manifest_uri": spider.settings.get("RUN_MANIFEST_S3_URI")
                or spider.settings.get("RUN_MANIFEST_LOCAL_PATH"),
                "quarantine_uri": spider.settings.get("QUARANTINE_S3_URI")
                or spider.settings.get("QUARANTINE_LOCAL_PATH"),
                "portal_summary_uri": spider.settings.get("PORTAL_SUMMARY_S3_URI")
                or spider.settings.get("PORTAL_SUMMARY_LOCAL_PATH"),
            },
        }

    def process_item(self, item, spider=None):
        dedupe_key = item.get("job_hash")
        if dedupe_key:
            if dedupe_key in self.seen_keys:
                self.duplicates_dropped += 1
                if self.stats:
                    self.stats.inc_value("pipeline/duplicates_dropped_total")
                raise DropItem(f"Duplicate item dropped (key={dedupe_key})")
            self.seen_keys.add(dedupe_key)

        # Count fields present
        self.total_items += 1
        for key in self.TRACKED_COMPLETENESS_FIELDS:
            value = item.get(key)
            if value:
                # For list fields check if non‑empty
                if isinstance(value, list) and not value:
                    continue
                self.field_counts[key] += 1

        url = item.get("source_url") or item.get("canonical_source_url")
        if url:
            parsed = urlparse(url)
            if parsed.hostname:
                self.hosts.add(parsed.hostname.lower())

        if item.get("portal_key"):
            self.portals.add(str(item["portal_key"]))

        return item


try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3: ModuleType | None = None
    ClientError: type | None = None


class DynamoDBDedupePipeline:
    """Drop items already seen in previous AWS runs using conditional puts."""

    deploy_env: str
    table_name: str | None
    ttl_days: int
    fail_open: bool
    table: Any
    _local_seen: set[str]

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
            raise RuntimeError("boto3/botocore not installed but DEPLOY_ENV=aws.")

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

        expires_at = int(time.time()) + int(self.ttl_days) * 86400
        try:
            self.table.put_item(
                Item={"job_hash": job_hash, "first_seen_ts": int(time.time()), "expires_at": expires_at},
                ConditionExpression="attribute_not_exists(job_hash)",
            )
            return item
        except Exception as exc:
            if ClientError is not None and not isinstance(exc, ClientError):
                raise
            code = exc.response.get("Error", {}).get("Code", "")  # type: ignore[union-attr]
            if code == "ConditionalCheckFailedException":
                spider.crawler.stats.inc_value("pipeline/dynamodb_duplicates_dropped")
                raise DropItem(f"Duplicate across runs (job_hash={job_hash})") from None

            spider.logger.error("DynamoDB put_item error (%s): %s", code, exc)
            if self.fail_open:
                return item
            raise
