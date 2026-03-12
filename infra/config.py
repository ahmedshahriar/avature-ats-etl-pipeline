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

    # New Operational Controls
    ecs_task_cpu: int
    ecs_task_memory: int

    # Optional alert email for ECS task failures; if not set, no alerts will be configured.
    alert_email: str | None

    schedule_hour: str
    schedule_minute: str
    schedule_timezone: str
    schedule_enabled: bool

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

        return cls(
            project_name=project_name,
            env_name=env_name,
            aws_region=os.getenv("AWS_REGION"),
            bucket_suffix=data["bucket_suffix"],
            ddb_table_suffix=data["ddb_table_suffix"],
            ecs_task_cpu=int(data["ecs_task_cpu"]),
            ecs_task_memory=int(data["ecs_task_memory"]),
            alert_email=(data.get("alert_email") or os.getenv("ALERT_EMAIL") or "").strip() or None,
            schedule_hour=str(data.get("schedule_hour", "9")),
            schedule_minute=str(data.get("schedule_minute", "0")),
            schedule_timezone=str(data.get("schedule_timezone", "UTC")),
            schedule_enabled=bool(data.get("schedule_enabled", env_name == "prod")),
            scraper_runtime=ScraperRuntimeConfig.from_mapping(data["scraper_runtime"]),
            tags=tags,
        )
