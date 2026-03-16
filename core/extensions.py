import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from scrapy import signals
from twisted.internet.task import LoopingCall


class CrawlMetricsExtension:
    """Collect lean crawl telemetry for metrics.json and portal_summary.jsonl.

    Ownership split:
    - spider: extraction + request metadata (request_kind, portal_key)
    - validation/job pipelines: final item outcomes (quarantine, duplicates, exported items)
    - extension: crawl/request behavior and portal-level crawl breakdowns
    """

    TRACKED_COMPLETENESS_FIELDS = ("title", "description_text", "apply_url", "posted_date", "company")

    def __init__(self, stats, dump_path: str | None, interval_s: int, portal_summary_path: str | None):
        self.stats = stats
        self.dump_path = Path(dump_path) if dump_path else None
        self.interval_s = max(0, int(interval_s))
        self.portal_summary_path = Path(portal_summary_path) if portal_summary_path else None
        self._loop: LoopingCall | None = None
        self.started_at_ts: float | None = None
        self.request_counts: dict[str, int] = defaultdict(int)
        self.response_buckets: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.portal_summary: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.job_detail_exception_count = 0

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(
            stats=crawler.stats,
            dump_path=crawler.settings.get("METRICS_DUMP_PATH"),
            interval_s=crawler.settings.getint("METRICS_DUMP_INTERVAL", 0),
            portal_summary_path=crawler.settings.get("PORTAL_SUMMARY_LOCAL_PATH"),
        )
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.request_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(ext.response_received, signal=signals.response_received)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.item_dropped, signal=signals.item_dropped)
        crawler.signals.connect(ext.spider_error, signal=signals.spider_error)
        return ext

    def spider_opened(self, spider):
        self.started_at_ts = time.time()
        self.stats.set_value("run/started_at", self._iso_now())
        self.stats.set_value("run/started_at_ts", self.started_at_ts)

        if self.dump_path:
            self.dump_path.parent.mkdir(parents=True, exist_ok=True)
            self._dump(reason="spider_opened")
            if self.interval_s > 0:
                self._loop = LoopingCall(self._dump, "periodic")
                self._loop.start(self.interval_s, now=False)

        if self.portal_summary_path:
            self.portal_summary_path.parent.mkdir(parents=True, exist_ok=True)

        spider.logger.info("Crawl metrics enabled (dump_path=%s, interval=%ss)", self.dump_path, self.interval_s)

    def request_scheduled(self, request, spider):
        kind = self._request_kind(request)
        portal_key = self._portal_key(request=request)

        self.request_counts[kind] += 1
        self.portal_summary[portal_key][f"{kind}_requests_total"] += 1
        self.portal_summary[portal_key]["requests_total"] += 1

        self.stats.set_value(f"crawl/{kind}_requests_total", self.request_counts[kind])

    def response_received(self, response, request, spider):
        kind = self._request_kind(request)
        portal_key = self._portal_key(request=request)
        bucket = self._status_bucket(getattr(response, "status", 0))

        self.response_buckets[kind][bucket] += 1
        self.portal_summary[portal_key][f"{kind}_responses_{bucket}"] += 1
        self.portal_summary[portal_key]["responses_total"] += 1
        if getattr(response, "status", 0) >= 400:
            self.portal_summary[portal_key]["error_responses_total"] += 1

        self.stats.set_value(f"crawl/{kind}_responses_{bucket}", self.response_buckets[kind][bucket])

    def item_scraped(self, item, response, spider):
        portal_key = self._portal_key(item=item, response=response)
        self.portal_summary[portal_key]["jobs_exported_total"] += 1

        for field in self.TRACKED_COMPLETENESS_FIELDS:
            if item.get(field):
                self.portal_summary[portal_key][f"{field}_present_total"] += 1

    def item_dropped(self, item, response, exception, spider):
        portal_key = self._portal_key(item=item, response=response)
        drop_reason = self._classify_drop_reason(item=item, exception=exception)
        if drop_reason == "quarantine":
            self.portal_summary[portal_key]["jobs_quarantined_total"] += 1
        elif drop_reason == "duplicate":
            self.portal_summary[portal_key]["duplicates_dropped_total"] += 1

    def spider_error(self, failure, response, spider):
        request_kind = self._request_kind(getattr(response, "request", None)) if response else "other"
        if request_kind == "job_detail":
            self.job_detail_exception_count += 1
            self.stats.set_value("crawl/job_detail_parse_exception_total", self.job_detail_exception_count)

    def spider_closed(self, spider, reason):
        try:
            if self._loop and self._loop.running:
                self._loop.stop()
        except Exception:
            pass

        summary = self._build_summary(reason=reason)
        self.stats.set_value("crawl/run_duration_seconds", summary["run_duration_seconds"])
        self.stats.set_value("crawl/job_detail_success_rate", summary["job_detail_success_rate"])
        self.stats.set_value("crawl/job_detail_requests_total", summary["job_detail_requests_total"])
        self.stats.set_value("crawl/job_detail_parse_failure_total", summary["job_detail_parse_failure_total"])
        self.stats.set_value("crawl/summary", summary)
        self.stats.set_value("run/finished_at", self._iso_now())

        self._write_portal_summary()
        self._dump(reason=f"spider_closed:{reason}")
        self._upload_portal_summary_to_s3(spider)
        self._upload_metrics_to_s3(spider)

    def _build_summary(self, reason: str) -> dict[str, Any]:
        current_stats = dict(self.stats.get_stats() or {})
        job_detail_requests_total = self.request_counts.get("job_detail", 0)
        jobs_exported_total = int(current_stats.get("item_scraped_count", 0) or 0)
        jobs_quarantined_total = int(current_stats.get("pipeline/jobs_quarantined_total", 0) or 0)
        duplicate_items_dropped = int(current_stats.get("pipeline/duplicates_dropped_total", 0) or 0)
        duplicate_items_dropped += int(current_stats.get("pipeline/dynamodb_duplicates_dropped", 0) or 0)
        started_at_ts = self.started_at_ts or time.time()
        run_duration_seconds = round(max(0.0, time.time() - started_at_ts), 3)

        # This is a funnel-loss approximation, not a guaranteed parser-only failure count.
        non_export_total = max(0, job_detail_requests_total - jobs_exported_total)
        if self.job_detail_exception_count:
            non_export_total = max(non_export_total, self.job_detail_exception_count)

        return {
            "close_reason": reason,
            "run_duration_seconds": run_duration_seconds,
            "jobs_discovered_total": int(current_stats.get("crawl/jobs_discovered_total", 0) or 0),
            "jobs_exported_total": jobs_exported_total,
            "jobs_quarantined_total": jobs_quarantined_total,
            "duplicates_dropped_total": duplicate_items_dropped,
            "job_detail_requests_total": job_detail_requests_total,
            "job_detail_responses": dict(self.response_buckets.get("job_detail", {})),
            "job_detail_parse_exception_total": self.job_detail_exception_count,
            "job_detail_parse_failure_total": non_export_total,
            "job_detail_success_rate": round(jobs_exported_total / job_detail_requests_total, 6)
            if job_detail_requests_total
            else None,
            "portal_summary_count": len(self.portal_summary),
        }

    def _portal_rows(self) -> list[dict[str, Any]]:
        rows = []
        for portal_key, metrics in self.portal_summary.items():
            exported_total = int(metrics.get("jobs_exported_total", 0))
            job_detail_requests = int(metrics.get("job_detail_requests_total", 0))
            duplicates_total = int(metrics.get("duplicates_dropped_total", 0))
            title_present_total = int(metrics.get("title_present_total", 0))
            description_present_total = int(metrics.get("description_text_present_total", 0))
            apply_present_total = int(metrics.get("apply_url_present_total", 0))
            posted_present_total = int(metrics.get("posted_date_present_total", 0))
            company_present_total = int(metrics.get("company_present_total", 0))

            rows.append(
                {
                    "portal_key": portal_key,
                    "requests_total": int(metrics.get("requests_total", 0)),
                    "responses_total": int(metrics.get("responses_total", 0)),
                    "error_responses_total": int(metrics.get("error_responses_total", 0)),
                    "listing_requests_total": int(metrics.get("listing_requests_total", 0)),
                    "pagination_requests_total": int(metrics.get("pagination_requests_total", 0)),
                    "job_detail_requests_total": job_detail_requests,
                    "jobs_exported_total": exported_total,
                    "jobs_quarantined_total": int(metrics.get("jobs_quarantined_total", 0)),
                    "duplicates_dropped_total": duplicates_total,
                    "duplicate_rate": round(duplicates_total / job_detail_requests, 6) if job_detail_requests else 0.0,
                    "job_detail_success_rate": round(exported_total / job_detail_requests, 6)
                    if job_detail_requests
                    else None,
                    "title_completeness_pct": round(100.0 * title_present_total / exported_total, 4)
                    if exported_total
                    else None,
                    "description_completeness_pct": round(100.0 * description_present_total / exported_total, 4)
                    if exported_total
                    else None,
                    "apply_url_completeness_pct": round(100.0 * apply_present_total / exported_total, 4)
                    if exported_total
                    else None,
                    "posted_date_completeness_pct": round(100.0 * posted_present_total / exported_total, 4)
                    if exported_total
                    else None,
                    "company_completeness_pct": round(100.0 * company_present_total / exported_total, 4)
                    if exported_total
                    else None,
                }
            )
        rows.sort(key=lambda x: (x["error_responses_total"], x["job_detail_requests_total"]), reverse=True)
        return rows

    def _write_portal_summary(self):
        if not self.portal_summary_path:
            return
        rows = self._portal_rows()
        body = "".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows)
        tmp = self.portal_summary_path.with_suffix(".tmp")
        tmp.write_text(body, encoding="utf-8")
        tmp.replace(self.portal_summary_path)

    def _dump(self, reason: str):
        if not self.dump_path:
            return
        stats: dict[str, Any] = dict(self.stats.get_stats() or {})
        stats["_dump_ts"] = time.time()
        stats["_dump_reason"] = reason
        stats["_summary"] = self._build_summary(reason=reason)
        tmp = self.dump_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(stats, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        tmp.replace(self.dump_path)

    def _upload_portal_summary_to_s3(self, spider):
        deploy_env = (spider.settings.get("DEPLOY_ENV") or "local").lower()
        s3_uri = spider.settings.get("PORTAL_SUMMARY_S3_URI")

        if deploy_env != "aws":
            return
        if not self.portal_summary_path or not self.portal_summary_path.exists():
            return
        if not s3_uri:
            spider.logger.warning("PORTAL_SUMMARY_S3_URI not set; skipping portal_summary upload")
            return

        parsed = urlparse(s3_uri)
        try:
            import boto3

            boto3.client("s3").upload_file(
                str(self.portal_summary_path),
                parsed.netloc,
                parsed.path.lstrip("/"),
            )
            spider.logger.info("Uploaded portal_summary.jsonl to %s", s3_uri)
        except Exception as exc:  # pragma: no cover
            spider.logger.error("Failed to upload portal_summary.jsonl to S3: %s", exc)

    def _upload_metrics_to_s3(self, spider):
        deploy_env = (spider.settings.get("DEPLOY_ENV") or "local").lower()
        s3_uri = spider.settings.get("METRICS_S3_URI")

        if deploy_env != "aws":
            return
        if not self.dump_path or not self.dump_path.exists():
            return
        if not s3_uri:
            spider.logger.warning("METRICS_S3_URI not set; skipping metrics.json upload")
            return

        parsed = urlparse(s3_uri)
        try:
            import boto3

            boto3.client("s3").upload_file(
                str(self.dump_path),
                parsed.netloc,
                parsed.path.lstrip("/"),
            )
            spider.logger.info("Uploaded metrics.json to %s", s3_uri)
        except Exception as exc:  # pragma: no cover
            spider.logger.error("Failed to upload metrics.json to S3: %s", exc)

    @staticmethod
    def _request_kind(request) -> str:
        if not request:
            return "other"
        meta = getattr(request, "meta", None) or {}
        kind = str(meta.get("request_kind", "other")).strip().lower()
        return kind or "other"

    @staticmethod
    def _portal_key(item=None, response=None, request=None) -> str:
        if item and item.get("portal_key"):
            return str(item["portal_key"])
        if response is not None:
            meta = getattr(response, "meta", None) or {}
            if meta.get("portal_key"):
                return str(meta["portal_key"])
            request = getattr(response, "request", request)
        if request is not None:
            meta = getattr(request, "meta", None) or {}
            if meta.get("portal_key"):
                return str(meta["portal_key"])
        return "unknown"

    @staticmethod
    def _classify_drop_reason(item=None, exception=None) -> str | None:
        if item and str(item.get("record_status", "")).lower() == "quarantined":
            return "quarantine"
        message = str(exception or "").strip().lower()
        if "validation failed" in message or "quarantine" in message:
            return "quarantine"
        if "duplicate" in message:
            return "duplicate"
        return None

    @staticmethod
    def _status_bucket(status: int) -> str:
        return f"{int(status / 100)}xx" if status else "0xx"

    @staticmethod
    def _iso_now() -> str:
        from datetime import UTC, datetime

        return datetime.now(UTC).isoformat()
