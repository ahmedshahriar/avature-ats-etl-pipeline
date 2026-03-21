import os
from dataclasses import dataclass, field
from pathlib import Path

from scraper_config import ScraperRuntimeConfig
from yaml import safe_load

ENVIRONMENTS_DIR = Path(__file__).with_name("environments")


@dataclass(frozen=True)
class AppConfig:
    """Strongly typed configuration object for the CDK App."""

    project_name: str
    env_name: str
    aws_region: str | None
    bucket_suffix: str
    ddb_table_suffix: str

    # Operational Controls
    ecs_task_cpu: int
    ecs_task_memory: int
    container_insights_mode: str

    # Optional alert email for ECS task failures; if not set, no alerts will be configured.
    alert_email: str | None

    # scheduler config
    schedule_hour: str
    schedule_minute: str
    schedule_timezone: str
    schedule_target: str

    # analytics / cost guardrails
    dataset_root: str
    enable_analytics: bool
    enable_dashboard: bool
    workflow_enabled: bool
    workflow_timeout_minutes: int
    athena_poll_seconds: int
    athena_bytes_scanned_cutoff_mb: int
    monthly_budget_usd: float | None
    ecs_task_timeout_minutes: int

    # Operational thresholds for monitoring and alerting
    empty_run_threshold: int
    job_detail_success_rate_threshold: float

    scraper_runtime: ScraperRuntimeConfig

    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def load(cls, env_name: str) -> "AppConfig":
        env_name = env_name.lower().strip()

        # Switched to look for .yaml files
        path = ENVIRONMENTS_DIR / f"{env_name}.yaml"
        if not path.exists():
            raise FileNotFoundError(f"Missing deploy config file: {path}")

        # Safely parse the YAML file
        with open(path) as f:
            data = safe_load(f)

        project_name = data["project_name"]

        raw_tags = data.get("tags", {})
        tags = {
            "Project": project_name,
            "Environment": env_name,
            **{k: v for k, v in raw_tags.items() if v is not None},
        }

        enable_analytics = bool(data.get("enable_analytics", True))
        workflow_enabled = bool(data.get("workflow_enabled", True))

        schedule_target = str(data.get("schedule_target", "workflow" if workflow_enabled else "none")).lower()

        if schedule_target not in {"none", "ecs", "workflow"}:
            raise ValueError("schedule_target must be one of: none, ecs, workflow")

        if schedule_target == "workflow" and not workflow_enabled:
            raise ValueError("schedule_target='workflow' requires workflow_enabled=true")

        if workflow_enabled and not enable_analytics:
            raise ValueError("workflow_enabled=true requires enable_analytics=true")

        return cls(
            project_name=project_name,
            env_name=env_name,
            aws_region=os.getenv("AWS_REGION"),
            bucket_suffix=data["bucket_suffix"],
            ddb_table_suffix=data["ddb_table_suffix"],
            # ecs
            ecs_task_cpu=int(data["ecs_task_cpu"]),
            ecs_task_memory=int(data["ecs_task_memory"]),
            container_insights_mode=str(data.get("container_insights_mode", "disabled")).lower(),
            # monitoring
            # receive alert_email from GitHub repo secret
            alert_email=(data.get("alert_email") or os.getenv("ALERT_EMAIL") or "").strip() or None,
            schedule_hour=str(data.get("schedule_hour", "9")),
            schedule_minute=str(data.get("schedule_minute", "0")),
            schedule_timezone=str(data.get("schedule_timezone", "UTC")),
            schedule_target=schedule_target,
            # analytics / cost guardrails
            dataset_root=str(data.get("dataset_root", "avature")),
            enable_analytics=enable_analytics,
            enable_dashboard=bool(data.get("enable_dashboard", True)),
            workflow_enabled=workflow_enabled,
            workflow_timeout_minutes=int(data.get("workflow_timeout_minutes", 180)),
            athena_poll_seconds=int(data.get("athena_poll_seconds", 15)),
            athena_bytes_scanned_cutoff_mb=int(data.get("athena_bytes_scanned_cutoff_mb", 512)),
            monthly_budget_usd=(
                float(data["monthly_budget_usd"]) if data.get("monthly_budget_usd") is not None else None
            ),
            ecs_task_timeout_minutes=int(data.get("ecs_task_timeout_minutes", 150)),
            # operational thresholds
            empty_run_threshold=int(data.get("empty_run_threshold", 1)),
            job_detail_success_rate_threshold=float(data.get("job_detail_success_rate_threshold", 0.95)),
            # scraper runtime config
            scraper_runtime=ScraperRuntimeConfig.from_mapping(data["scraper_runtime"]),
            tags=tags,
        )
