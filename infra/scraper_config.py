import os
from dataclasses import dataclass


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
    def from_env(cls) -> "ScraperRuntimeConfig":
        return cls(
            concurrent_requests=int(os.environ["CONCURRENT_REQUESTS"]),
            concurrent_requests_per_domain=int(os.environ["CONCURRENT_REQUESTS_PER_DOMAIN"]),
            download_delay=float(os.environ["DOWNLOAD_DELAY"]),
            download_timeout=int(os.environ["DOWNLOAD_TIMEOUT"]),
            autothrottle_enabled=os.environ["AUTOTHROTTLE_ENABLED"].lower() in ("1", "true", "yes"),
            autothrottle_start_delay=float(os.environ["AUTOTHROTTLE_START_DELAY"]),
            autothrottle_max_delay=float(os.environ["AUTOTHROTTLE_MAX_DELAY"]),
            autothrottle_target_concurrency=float(os.environ["AUTOTHROTTLE_TARGET_CONCURRENCY"]),
            retry_times=int(os.environ["RETRY_TIMES"]),
            log_level=os.environ["LOG_LEVEL"],
            logstats_interval=float(os.environ["LOGSTATS_INTERVAL"]),
            dynamodb_ttl_days=int(os.environ["DYNAMODB_TTL_DAYS"]),
            ddb_dedupe_fail_open=os.environ["DDB_DEDUPE_FAIL_OPEN"].lower() in ("1", "true", "yes"),
            scrapy_feed_name=os.environ["SCRAPY_FEED_NAME"],
            metrics_file=os.environ["METRICS_FILE"],
        )

    def to_env(self) -> dict[str, str]:
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
