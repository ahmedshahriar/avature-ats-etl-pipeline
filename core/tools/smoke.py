"""Run a small local smoke crawl against a curated seed set."""

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SmokeThresholds:
    min_job_detail_success_rate: float = 0.75
    max_quarantine_rate: float = 0.20


class SmokeValidationError(RuntimeError):
    """Raised when a smoke run does not meet validation requirements."""


@dataclass(frozen=True, slots=True)
class SmokeResult:
    run_dir: Path
    jobs_exported_total: int
    jobs_quarantined_total: int
    job_detail_success_rate: float | None
    quarantine_rate: float


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_smoke_run(run_dir: Path, thresholds: SmokeThresholds) -> SmokeResult:
    run_manifest_path = run_dir / "run_manifest.json"
    metrics_path = run_dir / "metrics.json"
    jobs_path = run_dir / "jobs.jsonl"

    required_paths = [run_manifest_path, metrics_path, jobs_path]
    missing = [path.name for path in required_paths if not path.exists()]
    if missing:
        raise SmokeValidationError(f"missing required artifact(s): {', '.join(missing)}")

    manifest = load_json(run_manifest_path)
    close_reason = str(manifest.get("close_reason") or "")
    if close_reason != "finished":
        raise SmokeValidationError(f"smoke crawl did not finish cleanly (close_reason={close_reason!r})")

    counts = manifest.get("counts") or {}
    quality = manifest.get("quality") or {}
    jobs_exported_total = int(counts.get("jobs_exported_total", 0) or 0)
    jobs_quarantined_total = int(counts.get("jobs_quarantined_total", 0) or 0)
    job_detail_success_rate = quality.get("job_detail_success_rate")
    job_detail_success_rate = float(job_detail_success_rate) if job_detail_success_rate is not None else None

    if jobs_exported_total <= 0:
        raise SmokeValidationError("smoke crawl exported zero jobs")

    total_validated = jobs_exported_total + jobs_quarantined_total
    quarantine_rate = (jobs_quarantined_total / total_validated) if total_validated else 1.0
    if quarantine_rate > thresholds.max_quarantine_rate:
        raise SmokeValidationError(
            f"quarantine rate {quarantine_rate:.3f} exceeded {thresholds.max_quarantine_rate:.3f}"
        )

    if job_detail_success_rate is None or job_detail_success_rate < thresholds.min_job_detail_success_rate:
        raise SmokeValidationError(
            "job-detail success rate "
            f"{job_detail_success_rate if job_detail_success_rate is not None else 'None'} "
            f"was below {thresholds.min_job_detail_success_rate:.3f}"
        )

    return SmokeResult(
        run_dir=run_dir,
        jobs_exported_total=jobs_exported_total,
        jobs_quarantined_total=jobs_quarantined_total,
        job_detail_success_rate=job_detail_success_rate,
        quarantine_rate=quarantine_rate,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a small local smoke crawl against curated portals.")
    parser.add_argument(
        "--seed-file",
        default="data/smoke_seed_urls.csv",
        help="Curated smoke seed CSV. Defaults to data/smoke_seed_urls.csv.",
    )
    parser.add_argument(
        "--output-root",
        default="output",
        help="Root directory for smoke outputs. A smoke_<timestamp> directory will be created underneath it.",
    )
    parser.add_argument(
        "--min-success-rate",
        type=float,
        default=SmokeThresholds().min_job_detail_success_rate,
        help="Minimum acceptable job-detail success rate.",
    )
    parser.add_argument(
        "--max-quarantine-rate",
        type=float,
        default=SmokeThresholds().max_quarantine_rate,
        help="Maximum acceptable quarantine rate.",
    )
    parser.add_argument("--concurrent-requests", type=int, default=8, help="Scrapy CONCURRENT_REQUESTS override.")
    parser.add_argument(
        "--concurrent-requests-per-domain",
        type=int,
        default=2,
        help="Scrapy CONCURRENT_REQUESTS_PER_DOMAIN override.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = repo_root()
    seed_file = Path(args.seed_file)
    if not seed_file.is_absolute():
        seed_file = root / seed_file

    if not seed_file.exists():
        print(f"Smoke seed file not found: {seed_file}", file=sys.stderr)
        return 1

    output_root = Path(args.output_root)
    if not output_root.is_absolute():
        output_root = root / output_root

    run_id = f"smoke_{timestamp_slug()}"
    run_dir = output_root / run_id
    run_dir.parent.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env.update(
        {
            "DEPLOY_ENV": "local",
            "RUN_ID": run_id,
            "RUN_DATE": date.today().isoformat(),
            "RUN_DIR": str(run_dir),
            "SEED_URLS_FILE": str(seed_file),
            "SCRAPY_JOBDIR": "",
            "HTTPCACHE_ENABLED": "0",
            "CONCURRENT_REQUESTS": str(args.concurrent_requests),
            "CONCURRENT_REQUESTS_PER_DOMAIN": str(args.concurrent_requests_per_domain),
            "DOWNLOAD_TIMEOUT": "20",
            "LOG_LEVEL": env.get("LOG_LEVEL", "INFO"),
            "LOGSTATS_INTERVAL": "15",
            "METRICS_DUMP_INTERVAL": "0",
        }
    )

    command = [sys.executable, "-m", "scrapy", "crawl", "avature"]
    result = subprocess.run(command, cwd=root, env=env, check=False)
    if result.returncode != 0:
        print(f"Smoke crawl exited with code {result.returncode}", file=sys.stderr)
        return result.returncode

    try:
        smoke_result = validate_smoke_run(
            run_dir,
            SmokeThresholds(
                min_job_detail_success_rate=args.min_success_rate,
                max_quarantine_rate=args.max_quarantine_rate,
            ),
        )
    except SmokeValidationError as exc:
        print(f"Smoke validation failed: {exc}", file=sys.stderr)
        print(f"Smoke artifacts: {run_dir}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "run_dir": str(smoke_result.run_dir),
                "jobs_exported_total": smoke_result.jobs_exported_total,
                "jobs_quarantined_total": smoke_result.jobs_quarantined_total,
                "job_detail_success_rate": smoke_result.job_detail_success_rate,
                "quarantine_rate": smoke_result.quarantine_rate,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
