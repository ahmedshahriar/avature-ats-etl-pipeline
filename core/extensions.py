import json
import time
from pathlib import Path
from typing import Any, Dict

from scrapy import signals
from twisted.internet.task import LoopingCall


class StatsDumpExtension:
    def __init__(self, stats, dump_path: str, interval_s: int):
        self.stats = stats
        self.dump_path = Path(dump_path)
        self.interval_s = max(0, int(interval_s))
        self._loop: LoopingCall | None = None

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(
            stats=crawler.stats,
            dump_path=crawler.settings.get("METRICS_DUMP_PATH") or "output/metrics.json",
            interval_s=crawler.settings.getint("METRICS_DUMP_INTERVAL", 0),
        )
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        self.dump_path.parent.mkdir(parents=True, exist_ok=True)
        self._dump(reason="spider_opened")
        if self.interval_s > 0:
            self._loop = LoopingCall(self._dump, "periodic")
            self._loop.start(self.interval_s, now=False)
        spider.logger.info("Metrics dumping to %s every %ss", str(self.dump_path), self.interval_s)

    def spider_closed(self, spider, reason):
        try:
            if self._loop and self._loop.running:
                self._loop.stop()
        except Exception:
            pass
        self._dump(reason=f"spider_closed:{reason}")

    def _dump(self, reason: str):
        stats: Dict[str, Any] = dict(self.stats.get_stats() or {})
        stats["_dump_ts"] = time.time()
        stats["_dump_reason"] = reason

        # Convenience success/failure summary
        status_counts = {
            k.split("/")[-1]: v
            for k, v in stats.items()
            if isinstance(k, str) and k.startswith("downloader/response_status_count/")
        }
        total_responses = sum(status_counts.values()) if status_counts else 0
        ok_200 = status_counts.get("200", 0)

        stats["_summary"] = {
            "total_responses": total_responses,
            "status_counts": status_counts,
            "success_rate_200": (ok_200 / total_responses) if total_responses else None,
            "items_scraped": stats.get("item_scraped_count", 0),
            "exceptions": stats.get("downloader/exception_count", 0),
        }

        pipeline_summary = stats.get("pipeline/summary")
        if pipeline_summary:
            stats["_pipeline"] = pipeline_summary

        tmp = self.dump_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(stats, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        tmp.replace(self.dump_path)
