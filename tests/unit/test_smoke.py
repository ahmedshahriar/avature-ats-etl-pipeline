from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.tools.smoke import SmokeThresholds, SmokeValidationError, validate_smoke_run


def write_smoke_artifacts(
    run_dir: Path,
    *,
    close_reason: str = "finished",
    jobs_exported_total: int = 10,
    jobs_quarantined_total: int = 1,
    job_detail_success_rate: float = 0.9,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "jobs.jsonl").write_text("{}\n", encoding="utf-8")
    (run_dir / "metrics.json").write_text("{}", encoding="utf-8")
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "close_reason": close_reason,
                "counts": {
                    "jobs_exported_total": jobs_exported_total,
                    "jobs_quarantined_total": jobs_quarantined_total,
                },
                "quality": {
                    "job_detail_success_rate": job_detail_success_rate,
                },
            }
        ),
        encoding="utf-8",
    )


def test_validate_smoke_run_accepts_healthy_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "smoke_run"
    write_smoke_artifacts(run_dir)

    result = validate_smoke_run(run_dir, SmokeThresholds())

    assert result.jobs_exported_total == 10
    assert result.jobs_quarantined_total == 1
    assert result.job_detail_success_rate == 0.9
    assert result.quarantine_rate < 0.2


def test_validate_smoke_run_rejects_missing_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "smoke_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(SmokeValidationError, match="missing required artifact"):
        validate_smoke_run(run_dir, SmokeThresholds())


def test_validate_smoke_run_rejects_unhealthy_thresholds(tmp_path: Path) -> None:
    run_dir = tmp_path / "smoke_run"
    write_smoke_artifacts(run_dir, jobs_exported_total=5, jobs_quarantined_total=5, job_detail_success_rate=0.4)

    with pytest.raises(SmokeValidationError):
        validate_smoke_run(run_dir, SmokeThresholds())
