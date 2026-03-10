import os

from aws_cdk import App, Environment, Tags
from config import AppConfig
from stacks.base_stack import AvatureEtlBaseStack
from stacks.ecr_stack import AvatureEtlEcrStack
from stacks.ecs_schedule_stack import AvatureEtlEcsScheduleStack
from stacks.notifications_stack import AvatureEtlNotificationsStack
from stacks.runtime_alarm_stack import AvatureEtlRuntimeAlarmStack

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

# ECS + Scheduler are merged into one stack to eliminate Fn::ImportValue cross-stack locks.
# CDK auto-generates ExportsOutput/Fn::ImportValue for any cross-stack Python reference,
# which blocks CloudFormation from updating resources like TaskDefinition while another
# stack imports them. Merging eliminates that constraint entirely.
ecs_stack = AvatureEtlEcsScheduleStack(
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
    schedule_hour="9" if cfg.env_name == "prod" else "10",
    schedule_minute="0",
    schedule_timezone="UTC",
    schedule_enabled=(cfg.env_name == "prod"),
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

notifications_stack = AvatureEtlNotificationsStack(
    app,
    f"{cfg.project_name}-notifications",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    scheduler_dlq=ecs_stack.dlq,
    alert_email=cfg.alert_email,
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

runtime_alarm_stack = AvatureEtlRuntimeAlarmStack(
    app,
    f"{cfg.project_name}-runtime-alarms",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    topic=notifications_stack.topic,  # ty: ignore[invalid-argument-type]
    spider_name="avature",
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

ecs_stack.add_dependency(base_stack)
ecs_stack.add_dependency(ecr_stack)
notifications_stack.add_dependency(ecs_stack)
runtime_alarm_stack.add_dependency(notifications_stack)

app.synth()
