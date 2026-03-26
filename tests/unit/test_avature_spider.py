from __future__ import annotations

import hashlib
from collections.abc import Callable

from scrapy.http import HtmlResponse

from core.spiders.avature_spider import AvatureSpider


def test_parse_listing_extracts_unique_job_links_and_explicit_next_page(
    html_fixture_loader: Callable[[str], str],
    html_response_factory: Callable[..., HtmlResponse],
    spider_factory: Callable[[dict | None], AvatureSpider],
) -> None:
    spider = spider_factory()
    seed_url = "https://example.avature.net/careers/SearchJobs?jobOffset=0"
    response = html_response_factory(
        url=seed_url,
        html=html_fixture_loader("listing_explicit_next.html"),
        meta={"portal_key": "example.avature.net/careers", "input_seed_url": seed_url},
    )

    requests = list(spider.parse_listing(response))

    assert [request.url for request in requests] == [
        "https://example.avature.net/careers/JobDetail/Foo-Role/1",
        "https://example.avature.net/careers/JobDetail/Second-Role/2",
        "https://example.avature.net/careers/SearchJobs?jobOffset=10",
    ]
    assert [request.meta["request_kind"] for request in requests] == ["job_detail", "job_detail", "pagination"]
    assert spider.crawler.stats.get_value("crawl/jobs_discovered_total") == 2


def test_parse_listing_uses_joboffset_fallback_for_next_page(
    html_fixture_loader: Callable[[str], str],
    html_response_factory: Callable[..., HtmlResponse],
    spider_factory: Callable[[dict | None], AvatureSpider],
) -> None:
    spider = spider_factory()
    seed_url = "https://example.avature.net/careers/SearchJobs?jobOffset=0"
    response = html_response_factory(
        url=seed_url,
        html=html_fixture_loader("listing_joboffset_only.html"),
        meta={"portal_key": "example.avature.net/careers", "input_seed_url": seed_url},
    )

    requests = list(spider.parse_listing(response))

    assert [request.url for request in requests] == [
        "https://example.avature.net/careers/JobDetail/Fallback-Role/10",
        "https://example.avature.net/careers/SearchJobs?jobOffset=20",
    ]


def test_parse_job_extracts_application_methods_detail_fixture(
    html_fixture_loader: Callable[[str], str],
    html_response_factory: Callable[..., HtmlResponse],
    spider_factory: Callable[[dict | None], AvatureSpider],
) -> None:
    spider = spider_factory()
    seed_url = "https://carlyle.avature.net/externalcareers/SearchJobs"
    response = html_response_factory(
        url="https://carlyle.avature.net/externalcareers/JobDetail/Security-Architect-Lead-Security-Assurance/5262",
        html=html_fixture_loader("carlyle_job_detail.html"),
        meta={"portal_key": "carlyle.avature.net/externalcareers/SearchJobs", "input_seed_url": seed_url},
    )

    item = list(spider.parse_job(response))[0]

    assert item["title"] == "Security Architect Lead, Security Assurance"
    assert item["job_id"] == "5262"
    assert item["apply_url"].endswith("ApplicationMethods?jobId=5262")
    assert item["portal_key"] == "carlyle.avature.net/externalcareers/SearchJobs"
    assert item["input_seed_url"] == seed_url
    assert item["raw_fields"]["job function"] == "Investor Services"
    assert item["description_text"]
    expected_hash = hashlib.sha256(b"carlyle.avature.net/externalcareers/SearchJobs|5262").hexdigest()
    assert item["job_hash"] == expected_hash


def test_parse_job_reproduces_login_apply_regression_shape(
    html_fixture_loader: Callable[[str], str],
    html_response_factory: Callable[..., HtmlResponse],
    spider_factory: Callable[[dict | None], AvatureSpider],
) -> None:
    spider = spider_factory()
    response = html_response_factory(
        url="https://astellasjapan.avature.net/en_GB/careers/JobDetail/Clinical-Trial-Manager/3381",
        html=html_fixture_loader("astellasjapan_job_detail.html"),
        meta={
            "portal_key": "astellasjapan.avature.net/en_GB/careers",
            "input_seed_url": "https://astellasjapan.avature.net/en_GB/careers/SearchJobs",
        },
    )

    item = list(spider.parse_job(response))[0]

    assert item["title"] == "Clinical Trial Manager"
    assert item["job_id"] == "3381"
    assert item["apply_url"].endswith("Login?jobId=3381")
    assert item.get("description_text") is None
    assert item["raw_fields"] == {}


def test_parse_job_reproduces_missing_description_regression_for_amerilife(
    html_fixture_loader: Callable[[str], str],
    html_response_factory: Callable[..., HtmlResponse],
    spider_factory: Callable[[dict | None], AvatureSpider],
) -> None:
    spider = spider_factory()
    response = html_response_factory(
        url="https://amerilife.avature.net/careers/JobDetail/Leesburg-Florida-United-States-Entry-Level-Licensed-Insurance-Agents/4525",
        html=html_fixture_loader("amerilife_job_detail.html"),
        meta={
            "portal_key": "amerilife.avature.net/careers",
            "input_seed_url": "https://amerilife.avature.net/careers",
        },
    )

    item = list(spider.parse_job(response))[0]

    assert item["title"] == "Entry Level & Licensed Insurance Agents"
    assert item["job_id"] == "4525"
    assert item["apply_url"].endswith("ApplicationMethods?jobId=4525")
    assert item.get("description_text") is None


def test_stable_identity_helpers_strip_query_and_derive_portal_key(
    spider_factory: Callable[[dict | None], AvatureSpider],
) -> None:
    url = "https://example.avature.net/careers/JobDetail/Foo//1?ref=abc#frag"

    assert spider_factory()._canonicalize_detail_url(url) == "https://example.avature.net/careers/JobDetail/Foo/1"
    assert spider_factory()._portal_key_from_url(url) == "example.avature.net/careers"
