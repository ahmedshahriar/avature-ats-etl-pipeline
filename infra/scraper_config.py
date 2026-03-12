from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class ScraperRuntimeConfig:
    concurrent_requests: int
    concurrent_requests_per_domain: int

    download_delay: float
    download_timeout: int

    autothrottle_enabled: bool
    autothrottle_start_delay: float
    autothrottle_max_delay: float
    autothrottle_target_concurrency: float
    retry_times: int

    log_level: str
    logstats_interval: float

    dynamodb_ttl_days: int
    ddb_dedupe_fail_open: bool

    scrapy_feed_name: str
    metrics_file: str

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "ScraperRuntimeConfig":
        """This method happily accepts the dictionary created by your YAML file!"""
        return cls(
            concurrent_requests=int(data["concurrent_requests"]),
            concurrent_requests_per_domain=int(data["concurrent_requests_per_domain"]),
            download_delay=float(data["download_delay"]),
            download_timeout=int(data["download_timeout"]),
            autothrottle_enabled=_as_bool(data["autothrottle_enabled"]),
            autothrottle_start_delay=float(data["autothrottle_start_delay"]),
            autothrottle_max_delay=float(data["autothrottle_max_delay"]),
            autothrottle_target_concurrency=float(data["autothrottle_target_concurrency"]),
            retry_times=int(data["retry_times"]),
            log_level=str(data["log_level"]),
            logstats_interval=float(data["logstats_interval"]),
            dynamodb_ttl_days=int(data["dynamodb_ttl_days"]),
            ddb_dedupe_fail_open=_as_bool(data["ddb_dedupe_fail_open"]),
            scrapy_feed_name=str(data["scrapy_feed_name"]),
            metrics_file=str(data["metrics_file"]),
        )

    def to_env(self) -> dict[str, str]:
        """Converts the config back to strings so ECS can inject them into the Docker container."""
        return {
            "CONCURRENT_REQUESTS": str(self.concurrent_requests),
            "CONCURRENT_REQUESTS_PER_DOMAIN": str(self.concurrent_requests_per_domain),
            "DOWNLOAD_DELAY": str(self.download_delay),
            "DOWNLOAD_TIMEOUT": str(self.download_timeout),
            "AUTOTHROTTLE_ENABLED": "1" if self.autothrottle_enabled else "0",
            "AUTOTHROTTLE_START_DELAY": str(self.autothrottle_start_delay),
            "AUTOTHROTTLE_MAX_DELAY": str(self.autothrottle_max_delay),
            "AUTOTHROTTLE_TARGET_CONCURRENCY": str(self.autothrottle_target_concurrency),
            "RETRY_TIMES": str(self.retry_times),
            "LOG_LEVEL": self.log_level,
            "LOGSTATS_INTERVAL": str(self.logstats_interval),
            "DYNAMODB_TTL_DAYS": str(self.dynamodb_ttl_days),
            "DDB_DEDUPE_FAIL_OPEN": "1" if self.ddb_dedupe_fail_open else "0",
            "SCRAPY_FEED_NAME": self.scrapy_feed_name,
            "METRICS_FILE": self.metrics_file,
        }
