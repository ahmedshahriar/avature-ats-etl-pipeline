import os

from aws_cdk import App, Environment, Tags
from config import AppConfig
from stacks.base_stack import AvatureEtlBaseStack
from stacks.ecr_stack import AvatureEtlEcrStack

app = App()
cfg = AppConfig.from_env()

for k, v in cfg.tags.items():
    Tags.of(app).add(k, v)

AvatureEtlBaseStack(
    app,
    f"{cfg.project_name}-base",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    bucket_suffix=cfg.bucket_suffix,
    ddb_table_suffix=cfg.ddb_table_suffix,
    env=Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")),
)

AvatureEtlEcrStack(
    app,
    f"{cfg.project_name}-ecr",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

app.synth()
