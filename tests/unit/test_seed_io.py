from __future__ import annotations

from pathlib import Path

import pytest

from core.seed_io import load_seed_urls, read_seed_file


def test_read_seed_file_reads_url_column(tmp_path: Path) -> None:
    seed_file = tmp_path / "seed_urls.csv"
    seed_file.write_text(
        "url,label\n"
        "https://ally.avature.net/careers,first\n"
        "https://carlyle.avature.net/externalcareers/SearchJobs,second\n",
        encoding="utf-8",
    )

    assert read_seed_file(seed_file) == [
        "https://ally.avature.net/careers",
        "https://carlyle.avature.net/externalcareers/SearchJobs",
    ]


def test_read_seed_file_requires_url_column(tmp_path: Path) -> None:
    seed_file = tmp_path / "seed_urls.csv"
    seed_file.write_text("seed,label\nhttps://example.avature.net/careers/SearchJobs,keep\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must include a 'url' column"):
        read_seed_file(seed_file)


def test_load_seed_urls_filters_internal_and_invalid_urls(tmp_path: Path) -> None:
    seed_file = tmp_path / "seed_urls.csv"
    seed_file.write_text(
        "url,label\n"
        "https://example.avature.net/careers/SearchJobs,keep\n"
        "https://staging.example.com/careers/SearchJobs,also-keep\n"
        "https://example.avature.net/internalcareers/SearchJobs,drop\n"
        "notaurl,drop\n",
        encoding="utf-8",
    )

    assert load_seed_urls(seed_file) == [
        "https://example.avature.net/careers/SearchJobs",
        "https://staging.example.com/careers/SearchJobs",
    ]
