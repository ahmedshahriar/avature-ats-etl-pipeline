import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

# Load the environment variables before doing anything else
load_dotenv("dev.env")


@dataclass(frozen=True)
class AppConfig:
    """Strongly typed configuration object for the CDK App."""

    project_name: str
    env_name: str
    aws_region: str | None
    bucket_suffix: str
    ddb_table_suffix: str

    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "AppConfig":
        project_name = os.environ["PROJECT_NAME"]
        env_name = os.environ["ENV_NAME"]
        return cls(
            project_name=project_name,
            env_name=env_name,
            aws_region=os.getenv("AWS_REGION"),
            bucket_suffix=os.environ["BUCKET_SUFFIX"],
            ddb_table_suffix=os.environ["DDB_TABLE_SUFFIX"],
            tags={
                k: v
                for k, v in {
                    "Project": os.getenv("TAG_Project", project_name),
                    "Environment": os.getenv("TAG_Environment", env_name),
                    "Owner": os.getenv("TAG_Owner"),
                    "ManagedBy": os.getenv("TAG_ManagedBy"),
                    "CostCenter": os.getenv("TAG_CostCenter"),
                }.items()
                if v is not None
            },
        )
