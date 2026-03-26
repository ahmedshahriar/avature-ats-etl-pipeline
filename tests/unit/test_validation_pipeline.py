from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest
from scrapy.exceptions import DropItem

from core.pipelines import ValidationPipeline
from core.spiders.avature_spider import AvatureSpider


def build_pipeline(tmp_path: Path, spider_factory: Callable[[dict | None], AvatureSpider]) -> ValidationPipeline:
    pipeline = ValidationPipeline()
    pipeline.deploy_env = "local"
    pipeline.quarantine_path = tmp_path / "quarantine.jsonl"
    pipeline.quarantine_s3_uri = None
    pipeline.stats = None
    pipeline.spider_opened(spider_factory())
    return pipeline


def test_validation_pipeline_normalizes_warnings_only_items(
    tmp_path: Path,
    spider_factory: Callable[[dict | None], AvatureSpider],
) -> None:
    pipeline = build_pipeline(tmp_path, spider_factory)

    item = {
        "canonical_source_url": "https://example.avature.net/careers/JobDetail/Foo/1",
        "source_url": "https://example.avature.net/careers/JobDetail/Foo/1",
        "job_id": "1",
        "title": "  Example Role  ",
        "description_text": "  Useful description  ",
        "locations": [" New York ", "new york", "Boston"],
        "remote": "Work From Home",
        "posted_date": "02-18-26",
        "apply_url": None,
        "company": None,
    }

    processed = pipeline.process_item(item, spider=spider_factory())

    assert processed["record_status"] == "valid"
    assert processed["remote"] == "remote"
    assert processed["posted_date"] == "2026-02-18"
    assert processed["locations"] == ["New York", "Boston"]
    assert processed["validation_errors"] == []
    assert set(processed["validation_warnings"]) == {"missing_apply_url", "missing_company"}


def test_validation_pipeline_quarantines_missing_description_and_writes_payload(
    tmp_path: Path,
    spider_factory: Callable[[dict | None], AvatureSpider],
) -> None:
    pipeline = build_pipeline(tmp_path, spider_factory)
    spider = spider_factory()
    item = {
        "canonical_source_url": "https://example.avature.net/careers/JobDetail/Foo/2",
        "source_url": "https://example.avature.net/careers/JobDetail/Foo/2",
        "job_id": "2",
        "title": "Broken Role",
        "description_text": None,
        "locations": [],
        "remote": "",
        "apply_url": None,
        "company": None,
    }

    with pytest.raises(DropItem, match="missing_description_text"):
        pipeline.process_item(item, spider=spider)

    payload = json.loads((tmp_path / "quarantine.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert payload["errors"] == ["missing_description_text"]
    assert payload["item"]["record_status"] == "quarantined"
    assert payload["item"]["title"] == "Broken Role"
