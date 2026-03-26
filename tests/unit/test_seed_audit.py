from __future__ import annotations

import json
from pathlib import Path

from core.tools import seed_audit


def test_seed_audit_parser_accepts_seed_file_argument() -> None:
    parser = seed_audit.build_parser()

    args = parser.parse_args(["seed_urls.csv"])

    assert args.seed_file == "seed_urls.csv"


def test_audit_seeds_classifies_all_requested_statuses(monkeypatch, tmp_path: Path) -> None:
    seed_file = tmp_path / "seed_urls.csv"
    seed_file.write_text(
        "url\n"
        + "\n".join(
            [
                "https://good.avature.net/careers/SearchJobs",
                "https://good.avature.net/careers/SearchJobs",
                "ftp://invalid.example.com/path",
                "https://staging.avature.net/careers/SearchJobs",
                "https://dnsbad.avature.net/careers/SearchJobs",
                "https://httpbad.avature.net/careers/SearchJobs",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_resolve_dns(hostname: str) -> tuple[bool, str]:
        if hostname == "dnsbad.avature.net":
            return False, "dns lookup failed"
        return True, "dns resolved"

    def fake_probe_http(url: str, timeout_seconds: float) -> tuple[int | None, str]:
        if "httpbad" in url:
            return 406, "http 406"
        return 200, "http 200"

    monkeypatch.setattr(seed_audit, "resolve_dns", fake_resolve_dns)
    monkeypatch.setattr(seed_audit, "probe_http", fake_probe_http)

    report = seed_audit.audit_seeds(
        input_path=seed_file,
        portal_summary_path=None,
        output_dir=tmp_path / "audit",
        timeout_seconds=5.0,
        max_workers=2,
    )

    classifications = {record["classification"] for record in report["records"]}
    assert classifications == {
        "valid_keep",
        "duplicate",
        "invalid_url",
        "excluded_internal_or_staging",
        "dns_unresolvable",
        "http_unhealthy",
    }
    assert (tmp_path / "audit" / "audit_report.json").exists()
    assert (tmp_path / "audit" / "recommended_seed_urls.csv").exists()


def test_audit_seeds_biases_recommendations_toward_recent_healthy_portals(monkeypatch, tmp_path: Path) -> None:
    seed_file = tmp_path / "seed_urls.csv"
    seed_file.write_text(
        "url\nhttps://strong.avature.net/careers/SearchJobs\nhttps://weak.avature.net/careers/SearchJobs\n",
        encoding="utf-8",
    )

    portal_summary_path = tmp_path / "portal_summary.jsonl"
    portal_summary_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "portal_key": "strong.avature.net/careers",
                        "responses_total": 120,
                        "error_responses_total": 4,
                        "jobs_exported_total": 50,
                        "job_detail_success_rate": 1.0,
                    }
                ),
                json.dumps(
                    {
                        "portal_key": "weak.avature.net/careers",
                        "responses_total": 100,
                        "error_responses_total": 40,
                        "jobs_exported_total": 5,
                        "job_detail_success_rate": 0.2,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(seed_audit, "resolve_dns", lambda hostname: (True, "dns resolved"))
    monkeypatch.setattr(seed_audit, "probe_http", lambda url, timeout_seconds: (200, "http 200"))

    seed_audit.audit_seeds(
        input_path=seed_file,
        portal_summary_path=portal_summary_path,
        output_dir=tmp_path / "audit",
        timeout_seconds=5.0,
        max_workers=2,
    )

    recommended = (tmp_path / "audit" / "recommended_seed_urls.csv").read_text(encoding="utf-8").splitlines()
    assert recommended == [
        "url",
        "https://strong.avature.net/careers/SearchJobs",
    ]
