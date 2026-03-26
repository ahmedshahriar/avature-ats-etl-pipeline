from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from scrapy.crawler import Crawler
from scrapy.http import HtmlResponse, Request
from scrapy.settings import Settings
from scrapy.statscollectors import MemoryStatsCollector

from core.spiders.avature_spider import AvatureSpider

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "html"


@pytest.fixture
def html_fixture_loader() -> Callable[[str], str]:
    def _load(name: str) -> str:
        return (FIXTURES_DIR / name).read_text(encoding="utf-8")

    return _load


@pytest.fixture
def spider_factory() -> Callable[[dict | None], AvatureSpider]:
    def _build(settings_overrides: dict | None = None) -> AvatureSpider:
        settings = Settings({"RUN_ID": "test-run", "RUN_DATE": "2026-03-18", **(settings_overrides or {})})
        crawler = Crawler(AvatureSpider, settings)
        crawler.stats = MemoryStatsCollector(crawler)
        return AvatureSpider.from_crawler(crawler)

    return _build


@pytest.fixture
def html_response_factory() -> Callable[..., HtmlResponse]:
    def _build(
        *,
        url: str,
        html: str,
        meta: dict | None = None,
        status: int = 200,
    ) -> HtmlResponse:
        request = Request(url, meta=meta or {})
        return HtmlResponse(
            url=url,
            request=request,
            body=html.encode("utf-8"),
            encoding="utf-8",
            status=status,
        )

    return _build
