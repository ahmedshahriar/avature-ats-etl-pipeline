import os

from aws_cdk import App, Environment, Tags
from config import AppConfig
from stacks.base_stack import AvatureEtlBaseStack
from stacks.ecr_stack import AvatureEtlEcrStack
from stacks.ecs_schedule_stack import AvatureEtlEcsScheduleStack
from stacks.notifications_stack import AvatureEtlNotificationsStack
from stacks.runtime_alarm_stack import AvatureEtlRuntimeAlarmStack

app = App()

deploy_env = str(app.node.try_get_context("env") or os.getenv("ENV_NAME", "dev")).lower()
if deploy_env not in {"dev", "prod"}:
    raise ValueError("Deployment environment must be 'dev' or 'prod'")

image_tag = str(app.node.try_get_context("imageTag") or os.getenv("IMAGE_TAG", "latest"))

cfg = AppConfig.load(deploy_env)

aws_env = Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

for k, v in cfg.tags.items():
    Tags.of(app).add(k, v)

# Shared ECR repository for build-once/promote-many
ecr_stack = AvatureEtlEcrStack(
    app,
    f"{cfg.project_name}-ecr",
    prefix=cfg.project_name,
    env=aws_env,
)

base_stack = AvatureEtlBaseStack(
    app,
    f"{cfg.project_name}-base-{cfg.env_name}",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    bucket_suffix=cfg.bucket_suffix,
    ddb_table_suffix=cfg.ddb_table_suffix,
    env=aws_env,
)

# ECS + Scheduler are merged into one stack to eliminate Fn::ImportValue cross-stack locks.
# CDK auto-generates ExportsOutput/Fn::ImportValue for any cross-stack Python reference,
# which blocks CloudFormation from updating resources like TaskDefinition while another
# stack imports them. Merging eliminates that constraint entirely.
ecs_stack = AvatureEtlEcsScheduleStack(
    app,
    f"{cfg.project_name}-ecs-{cfg.env_name}",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    image_tag=image_tag,
    ecs_task_cpu=cfg.ecs_task_cpu,
    ecs_task_memory=cfg.ecs_task_memory,
    outputs_bucket=base_stack.outputs_bucket,
    seen_jobs_table=base_stack.seen_jobs_table,
    ecs_log_group=base_stack.ecs_log_group,
    repository=ecr_stack.repository,
    scraper_runtime_env=cfg.scraper_runtime.to_env(),
    schedule_hour=cfg.schedule_hour,
    schedule_minute=cfg.schedule_minute,
    schedule_timezone=cfg.schedule_timezone,
    schedule_enabled=cfg.schedule_enabled,
    env=aws_env,
)

notifications_stack = AvatureEtlNotificationsStack(
    app,
    f"{cfg.project_name}-notifications-{cfg.env_name}",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    scheduler_dlq=ecs_stack.dlq,
    alert_email=cfg.alert_email,
    env=aws_env,
)

runtime_alarm_stack = AvatureEtlRuntimeAlarmStack(
    app,
    f"{cfg.project_name}-runtime-alarms-{cfg.env_name}",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    topic=notifications_stack.topic,  # ty: ignore[invalid-argument-type]
    spider_name="avature",
    env=aws_env,
)

ecs_stack.add_dependency(base_stack)
ecs_stack.add_dependency(ecr_stack)
notifications_stack.add_dependency(ecs_stack)
runtime_alarm_stack.add_dependency(notifications_stack)

app.synth()
