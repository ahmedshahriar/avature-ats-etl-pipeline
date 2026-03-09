import os

from aws_cdk import App, Environment, Tags
from config import AppConfig
from stacks.base_stack import AvatureEtlBaseStack
from stacks.ecr_stack import AvatureEtlEcrStack
from stacks.ecs_stack import AvatureEtlEcsStack

app = App()
cfg = AppConfig.from_env()

for k, v in cfg.tags.items():
    Tags.of(app).add(k, v)

base_stack = AvatureEtlBaseStack(
    app,
    f"{cfg.project_name}-base",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    bucket_suffix=cfg.bucket_suffix,
    ddb_table_suffix=cfg.ddb_table_suffix,
    env=Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")),
)

ecr_stack = AvatureEtlEcrStack(
    app,
    f"{cfg.project_name}-ecr",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

ecs_stack = AvatureEtlEcsStack(
    app,
    f"{cfg.project_name}-ecs",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    ecs_task_cpu=cfg.ecs_task_cpu,
    ecs_task_memory=cfg.ecs_task_memory,
    outputs_bucket=base_stack.outputs_bucket,
    seen_jobs_table=base_stack.seen_jobs_table,
    ecs_log_group=base_stack.ecs_log_group,
    repository=ecr_stack.repository,
    scraper_runtime_env=cfg.scraper_runtime.to_env(),
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

ecs_stack.add_dependency(base_stack)
ecs_stack.add_dependency(ecr_stack)

app.synth()
