import os

from aws_cdk import App, Environment, Tags
from config import AppConfig
from stacks.analytics_stack import AvatureEtlAnalyticsStack
from stacks.base_stack import AvatureEtlBaseStack
from stacks.dashboard_stack import AvatureEtlDashboardStack
from stacks.ecr_stack import AvatureEtlEcrStack
from stacks.ecs_schedule_stack import AvatureEtlEcsScheduleStack
from stacks.notifications_stack import AvatureEtlNotificationsStack
from stacks.runtime_alarm_stack import AvatureEtlRuntimeAlarmStack
from stacks.workflow_stack import AvatureEtlWorkflowStack

app = App()

deploy_env = str(app.node.try_get_context("env") or os.getenv("ENV_NAME", "dev")).lower()
if deploy_env not in {"dev", "prod"}:
    raise ValueError("Deployment environment must be 'dev' or 'prod'")

image_tag = str(app.node.try_get_context("imageTag") or os.getenv("IMAGE_TAG", "latest"))

cfg = AppConfig.load(deploy_env)

# workflow schedule (w/step function) is only enabled if workflow is enabled,
# but ECS schedule can be independently toggled on/off for testing or if users prefer it over workflow.
ecs_schedule_enabled = cfg.schedule_target == "ecs"
workflow_schedule_enabled = cfg.schedule_target == "workflow"

aws_env = Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

for k, v in cfg.tags.items():
    Tags.of(app).add(k, v)

base_stack = AvatureEtlBaseStack(
    app,
    f"{cfg.project_name}-base-{cfg.env_name}",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    bucket_suffix=cfg.bucket_suffix,
    ddb_table_suffix=cfg.ddb_table_suffix,
    env=aws_env,
)

# Shared ECR repository for build-once/promote-many
ecr_stack = AvatureEtlEcrStack(
    app,
    f"{cfg.project_name}-ecr",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    env=aws_env,
)

notifications_stack = AvatureEtlNotificationsStack(
    app,
    f"{cfg.project_name}-notifications-{cfg.env_name}",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    alert_email=cfg.alert_email,
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
    # config
    ecs_task_cpu=cfg.ecs_task_cpu,
    ecs_task_memory=cfg.ecs_task_memory,
    container_insights_mode=cfg.container_insights_mode,
    # feed
    outputs_bucket=base_stack.outputs_bucket,
    seen_jobs_table=base_stack.seen_jobs_table,
    ecs_log_group=base_stack.ecs_log_group,
    repository=ecr_stack.repository,
    scraper_runtime_env=cfg.scraper_runtime.to_env(),
    # schedule
    schedule_hour=cfg.schedule_hour,
    schedule_minute=cfg.schedule_minute,
    schedule_timezone=cfg.schedule_timezone,
    schedule_enabled=ecs_schedule_enabled,
    notification_topic_arn=notifications_stack.topic.topic_arn if "notifications_stack" in locals() else None,
    env=aws_env,
)

runtime_alarm_stack = AvatureEtlRuntimeAlarmStack(
    app,
    f"{cfg.project_name}-runtime-alarms-{cfg.env_name}",
    prefix=cfg.project_name,
    stage=cfg.env_name,
    topic=notifications_stack.topic,  # ty: ignore[invalid-argument-type]
    spider_name="avature",
    min_jobs_exported=cfg.empty_run_threshold,
    min_job_detail_success_rate=cfg.job_detail_success_rate_threshold,
    env=aws_env,
)

# Flow:
# 1. CDK deploy creates the Glue database, Athena workgroup, and saved Athena named queries.
# 2. The saved CREATE EXTERNAL TABLE query is not executed during deploy.
# 3. When that query is run later, Athena creates the Glue table metadata/schema in the Glue database.
# 4. Partition projection is stored as table properties on the Glue table.
# 5. At query time, Athena uses those properties to infer partitions/paths in S3.

analytics_stack = None
if cfg.enable_analytics:
    analytics_stack = AvatureEtlAnalyticsStack(
        app,
        f"{cfg.project_name}-analytics-{cfg.env_name}",
        prefix=cfg.project_name,
        stage=cfg.env_name,
        outputs_bucket=base_stack.outputs_bucket,
        dataset_root=cfg.dataset_root,
        env=aws_env,
    )

# EventBridge schedule
#   -> Step Functions Standard
#       -> ECS Fargate scraper (.sync)
#       -> Athena silver promotion
#       -> success / fail

workflow_stack = None
if cfg.workflow_enabled and analytics_stack is not None:
    workflow_stack = AvatureEtlWorkflowStack(
        app,
        f"{cfg.project_name}-workflow-{cfg.env_name}",
        prefix=cfg.project_name,
        stage=cfg.env_name,
        ecs_cluster=ecs_stack.cluster,
        ecs_task_definition=ecs_stack.task_definition,
        ecs_task_security_group=ecs_stack.task_security_group,
        analytics_database_name=analytics_stack.database_name,
        athena_workgroup_name=analytics_stack.workgroup_name,
        notification_topic_arn=notifications_stack.topic.topic_arn if notifications_stack is not None else None,
        schedule_enabled=workflow_schedule_enabled,
        schedule_hour=cfg.schedule_hour,
        schedule_minute=cfg.schedule_minute,
        schedule_timezone=cfg.schedule_timezone,
        athena_poll_seconds=cfg.athena_poll_seconds,
        workflow_timeout_minutes=cfg.workflow_timeout_minutes,
        env=aws_env,
    )

dashboard_stack = None
if cfg.enable_dashboard:
    dashboard_stack = AvatureEtlDashboardStack(
        app,
        f"{cfg.project_name}-dashboard-{cfg.env_name}",
        prefix=cfg.project_name,
        stage=cfg.env_name,
        spider_name="avature",
        env=aws_env,
    )

ecs_stack.add_dependency(base_stack)
ecs_stack.add_dependency(ecr_stack)
runtime_alarm_stack.add_dependency(notifications_stack)

if analytics_stack is not None:
    analytics_stack.add_dependency(base_stack)

if workflow_stack is not None:
    workflow_stack.add_dependency(ecs_stack)
    if analytics_stack is not None:
        workflow_stack.add_dependency(analytics_stack)

if dashboard_stack is not None:
    dashboard_stack.add_dependency(runtime_alarm_stack)

app.synth()
