"""Audit seed URLs before a crawl."""

from __future__ import annotations

import argparse
import json
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from core.seed_io import (
    has_http_scheme,
    is_nonproduction_seed_url,
    normalize_seed_url,
    portal_key_from_url,
    read_seed_file,
)

DEFAULT_TIMEOUT_SECONDS = 5.0
DEFAULT_MAX_WORKERS = 12
MIN_RECOMMENDED_SUCCESS_RATE = 0.9
MAX_RECOMMENDED_ERROR_RATE = 0.25
USER_AGENT = "avature-seed-audit/0.1 (+https://github.com/ahmedshahriar/avature-ats-etl-pipeline)"


@dataclass(slots=True)
class SeedProbeResult:
    classification: str
    reason: str
    dns_resolved: bool
    http_status: int | None


@dataclass(slots=True)
class SeedAuditRecord:
    raw_value: str
    normalized_url: str
    classification: str
    reason: str
    portal_key: str | None
    dns_resolved: bool | None
    http_status: int | None
    recommended: bool
    recommendation_reason: str | None
    observed_job_detail_success_rate: float | None
    observed_jobs_exported_total: int | None
    observed_error_rate: float | None


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def load_portal_summary(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}

    rows: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        rows[str(row["portal_key"])] = row
    return rows


def find_latest_portal_summary(root: Path) -> Path | None:
    candidates = sorted(root.glob("output/run_*/portal_summary.jsonl"))
    return candidates[-1] if candidates else None


def resolve_dns(hostname: str) -> tuple[bool, str]:
    try:
        socket.getaddrinfo(hostname, None)
    except socket.gaierror:
        return False, "dns lookup failed"
    except OSError as exc:
        return False, f"dns lookup error: {exc.__class__.__name__}"
    return True, "dns resolved"


def probe_http(url: str, timeout_seconds: float) -> tuple[int | None, str]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", 200) or 200)
            return status, f"http {status}"
    except urllib.error.HTTPError as exc:
        return int(exc.code), f"http {exc.code}"
    except urllib.error.URLError as exc:
        reason = getattr(exc.reason, "strerror", None) or str(exc.reason)
        return None, f"http probe error: {reason}"
    except Exception as exc:  # pragma: no cover - defensive
        return None, f"http probe error: {exc.__class__.__name__}"


def inspect_seed_url(url: str, timeout_seconds: float) -> SeedProbeResult:
    hostname = urllib.parse.urlparse(url).hostname
    if not hostname:
        return SeedProbeResult(
            classification="invalid_url",
            reason="missing hostname",
            dns_resolved=False,
            http_status=None,
        )

    dns_resolved, dns_reason = resolve_dns(hostname)
    if not dns_resolved:
        return SeedProbeResult(
            classification="dns_unresolvable",
            reason=dns_reason,
            dns_resolved=False,
            http_status=None,
        )

    http_status, http_reason = probe_http(url, timeout_seconds)
    if http_status is None or http_status >= 400:
        return SeedProbeResult(
            classification="http_unhealthy",
            reason=http_reason,
            dns_resolved=True,
            http_status=http_status,
        )

    return SeedProbeResult(
        classification="valid_keep",
        reason=http_reason,
        dns_resolved=True,
        http_status=http_status,
    )


def recommend_seed(
    classification: str,
    portal_key: str | None,
    portal_summary: dict[str, dict[str, Any]],
) -> tuple[bool, str | None, float | None, int | None, float | None]:
    if classification != "valid_keep":
        return False, None, None, None, None

    if not portal_summary:
        return True, "no recent portal summary provided; kept by live probe", None, None, None

    if portal_key is None or portal_key not in portal_summary:
        return False, "no recent portal summary for this portal", None, None, None

    row = portal_summary[portal_key]
    success_rate = row.get("job_detail_success_rate")
    jobs_exported_total = int(row.get("jobs_exported_total", 0) or 0)
    responses_total = int(row.get("responses_total", 0) or 0)
    error_responses_total = int(row.get("error_responses_total", 0) or 0)
    error_rate = (error_responses_total / responses_total) if responses_total else None

    if (
        jobs_exported_total > 0
        and success_rate is not None
        and float(success_rate) >= MIN_RECOMMENDED_SUCCESS_RATE
        and (error_rate is None or error_rate <= MAX_RECOMMENDED_ERROR_RATE)
    ):
        return (
            True,
            f"recent run exported {jobs_exported_total} jobs at success rate {float(success_rate):.3f}",
            float(success_rate),
            jobs_exported_total,
            error_rate,
        )

    return (
        False,
        f"recent run did not meet curated thresholds (exported={jobs_exported_total}, "
        f"success_rate={float(success_rate or 0.0):.3f})",
        float(success_rate) if success_rate is not None else None,
        jobs_exported_total,
        error_rate,
    )


def summarize_records(records: list[SeedAuditRecord]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for record in records:
        totals[record.classification] = totals.get(record.classification, 0) + 1
    totals["recommended"] = sum(1 for record in records if record.recommended)
    totals["total"] = len(records)
    return totals


def audit_seeds(
    *,
    input_path: Path,
    portal_summary_path: Path | None,
    output_dir: Path,
    timeout_seconds: float,
    max_workers: int,
) -> dict[str, Any]:
    raw_values = read_seed_file(input_path)
    portal_summary = load_portal_summary(portal_summary_path)

    records: list[SeedAuditRecord | None] = [None] * len(raw_values)
    pending_indices: list[tuple[int, str]] = []
    seen_urls: set[str] = set()

    for index, raw_value in enumerate(raw_values):
        normalized_url = normalize_seed_url(raw_value)
        portal_key = portal_key_from_url(normalized_url) if has_http_scheme(normalized_url) else None

        if not has_http_scheme(normalized_url):
            records[index] = SeedAuditRecord(
                raw_value=raw_value,
                normalized_url=normalized_url,
                classification="invalid_url",
                reason="seed must start with http:// or https://",
                portal_key=portal_key,
                dns_resolved=None,
                http_status=None,
                recommended=False,
                recommendation_reason=None,
                observed_job_detail_success_rate=None,
                observed_jobs_exported_total=None,
                observed_error_rate=None,
            )
            continue

        if is_nonproduction_seed_url(normalized_url):
            records[index] = SeedAuditRecord(
                raw_value=raw_value,
                normalized_url=normalized_url,
                classification="excluded_internal_or_staging",
                reason="seed looks like an internal, staging, sandbox, or training portal",
                portal_key=portal_key,
                dns_resolved=None,
                http_status=None,
                recommended=False,
                recommendation_reason=None,
                observed_job_detail_success_rate=None,
                observed_jobs_exported_total=None,
                observed_error_rate=None,
            )
            continue

        if normalized_url in seen_urls:
            records[index] = SeedAuditRecord(
                raw_value=raw_value,
                normalized_url=normalized_url,
                classification="duplicate",
                reason="duplicate normalized seed URL",
                portal_key=portal_key,
                dns_resolved=None,
                http_status=None,
                recommended=False,
                recommendation_reason=None,
                observed_job_detail_success_rate=None,
                observed_jobs_exported_total=None,
                observed_error_rate=None,
            )
            continue

        seen_urls.add(normalized_url)
        pending_indices.append((index, normalized_url))

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        future_map = {
            executor.submit(inspect_seed_url, normalized_url, timeout_seconds): (index, normalized_url)
            for index, normalized_url in pending_indices
        }
        for future in as_completed(future_map):
            index, normalized_url = future_map[future]
            probe = future.result()
            portal_key = portal_key_from_url(normalized_url)
            recommended, recommendation_reason, success_rate, jobs_exported_total, error_rate = recommend_seed(
                probe.classification,
                portal_key,
                portal_summary,
            )
            records[index] = SeedAuditRecord(
                raw_value=raw_values[index],
                normalized_url=normalized_url,
                classification=probe.classification,
                reason=probe.reason,
                portal_key=portal_key,
                dns_resolved=probe.dns_resolved,
                http_status=probe.http_status,
                recommended=recommended,
                recommendation_reason=recommendation_reason,
                observed_job_detail_success_rate=success_rate,
                observed_jobs_exported_total=jobs_exported_total,
                observed_error_rate=error_rate,
            )

    finalized_records = [record for record in records if record is not None]
    totals = summarize_records(finalized_records)

    output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "input_path": str(input_path),
        "portal_summary_path": str(portal_summary_path) if portal_summary_path else None,
        "totals": totals,
        "records": [asdict(record) for record in finalized_records],
    }
    (output_dir / "audit_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    recommended_csv_lines = ["url"]
    recommended_csv_lines.extend(record.normalized_url for record in finalized_records if record.recommended)
    (output_dir / "recommended_seed_urls.csv").write_text("\n".join(recommended_csv_lines) + "\n", encoding="utf-8")
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit crawler seed URLs and emit a curated report.")
    parser.add_argument("seed_file", nargs="?", default="seed_urls.csv", help="Seed CSV with a required `url` column.")
    parser.add_argument(
        "--portal-summary",
        default=None,
        help="Optional portal_summary.jsonl to bias curated recommendations toward healthy portals.",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help="Optional output directory root. Defaults to output/seed_audit/<timestamp>/",
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout per seed probe.")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="Concurrent network probes.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = repo_root()
    input_path = Path(args.seed_file)
    if not input_path.is_absolute():
        input_path = root / input_path

    if not input_path.exists():
        print(f"Seed file not found: {input_path}", file=sys.stderr)
        return 1

    portal_summary_path = Path(args.portal_summary) if args.portal_summary else find_latest_portal_summary(root)
    if portal_summary_path is not None and not portal_summary_path.is_absolute():
        portal_summary_path = root / portal_summary_path
    if portal_summary_path is not None and not portal_summary_path.exists():
        print(f"Portal summary not found: {portal_summary_path}", file=sys.stderr)
        return 1

    if args.output_root:
        output_dir = Path(args.output_root)
        if not output_dir.is_absolute():
            output_dir = root / output_dir
    else:
        output_dir = root / "output" / "seed_audit" / timestamp_slug()

    try:
        report = audit_seeds(
            input_path=input_path,
            portal_summary_path=portal_summary_path,
            output_dir=output_dir,
            timeout_seconds=args.timeout,
            max_workers=args.max_workers,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps({"output_dir": str(output_dir), "totals": report["totals"]}, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
