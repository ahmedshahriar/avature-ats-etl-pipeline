"""
Unit tests for infra stacks.

"""

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template
from stacks.base_stack import AvatureEtlBaseStack
from stacks.ecr_stack import AvatureEtlEcrStack
from stacks.ecs_schedule_stack import AvatureEtlEcsScheduleStack
from stacks.notifications_stack import AvatureEtlNotificationsStack
from stacks.runtime_alarm_stack import AvatureEtlRuntimeAlarmStack

# =============================================================================
# Pytest Fixtures (Shared Setup)
# =============================================================================


@pytest.fixture
def aws_env():
    """Provides a standard dummy AWS environment for synthesis."""
    return cdk.Environment(account="111111111111", region="us-east-1")


@pytest.fixture
def app():
    """Provides a fresh CDK App context for each test."""
    return cdk.App()


@pytest.fixture
def base_stack(app, aws_env):
    """Instantiates the Base Stack with required arguments."""
    return AvatureEtlBaseStack(
        app,
        "avature-etl-base",
        prefix="avature-etl",
        stage="dev",
        bucket_suffix="test-bucket",
        ddb_table_suffix="seen-jobs",
        env=aws_env,
    )


@pytest.fixture
def ecr_stack(app, aws_env):
    """Instantiates the ECR Stack."""
    return AvatureEtlEcrStack(
        app,
        "avature-etl-ecr",
        prefix="avature-etl",
        stage="dev",
        env=aws_env,
    )


@pytest.fixture
def ecs_stack(app, aws_env, base_stack, ecr_stack):
    """Instantiates the ECS Schedule Stack with its dependencies."""
    return AvatureEtlEcsScheduleStack(
        app,
        "avature-etl-ecs",
        prefix="avature-etl",
        stage="dev",
        image_tag="latest",
        ecs_task_cpu=256,
        ecs_task_memory=1024,
        scraper_runtime_env={},
        outputs_bucket=base_stack.outputs_bucket,
        seen_jobs_table=base_stack.seen_jobs_table,
        ecs_log_group=base_stack.ecs_log_group,
        repository=ecr_stack.repository,
        schedule_hour="10",
        schedule_minute="0",
        schedule_timezone="UTC",
        schedule_enabled=False,
        env=aws_env,
    )


# =============================================================================
# Tests
# =============================================================================


def test_base_stack_resources_created(base_stack):
    template = Template.from_stack(base_stack)

    template.resource_count_is("AWS::S3::Bucket", 1)
    template.resource_count_is("AWS::DynamoDB::Table", 1)

    template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "TableName": "avature-etl-dev-seen-jobs",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{"AttributeName": "job_hash", "KeyType": "HASH"}],
            "TimeToLiveSpecification": {"AttributeName": "expires_at", "Enabled": True},
        },
    )

    template.resource_count_is("AWS::Logs::LogGroup", 1)
    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        {"LogGroupName": "/ecs/avature-etl-dev"},
    )


def test_ecr_stack_resources_created(ecr_stack):
    template = Template.from_stack(ecr_stack)

    template.resource_count_is("AWS::ECR::Repository", 1)
    template.has_resource_properties(
        "AWS::ECR::Repository",
        {
            "RepositoryName": "avature-etl-scraper",
            "ImageScanningConfiguration": {"ScanOnPush": True},
            "ImageTagMutability": "IMMUTABLE",
        },
    )


def test_ecs_stack_resources_created(ecs_stack):
    template = Template.from_stack(ecs_stack)

    template.resource_count_is("AWS::ECS::Cluster", 1)
    template.resource_count_is("AWS::ECS::TaskDefinition", 1)

    template.has_resource_properties(
        "AWS::ECS::TaskDefinition",
        {
            "Cpu": "256",
            "Memory": "1024",
            "RuntimePlatform": {
                "CpuArchitecture": "ARM64",
                "OperatingSystemFamily": "LINUX",
            },
        },
    )


def test_schedule_stack_resources_created(ecs_stack):
    template = Template.from_stack(ecs_stack)

    template.resource_count_is("AWS::Scheduler::Schedule", 1)
    template.resource_count_is("AWS::SQS::Queue", 1)

    template.has_resource_properties(
        "AWS::Scheduler::Schedule",
        {
            "State": "DISABLED",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "ScheduleExpression": "cron(0 10 ? * * *)",
            "ScheduleExpressionTimezone": "UTC",
        },
    )


def test_notifications_stack_resources_created(app, aws_env):
    # Dummy stack to fulfill dependencies
    dummy_stack = cdk.Stack(app, "dummy-queue-stack", env=aws_env)
    dlq = cdk.aws_sqs.Queue(dummy_stack, "TestDlq")

    notifications_stack = AvatureEtlNotificationsStack(
        app,
        "avature-etl-notifications-dev",
        prefix="avature-etl",
        stage="dev",
        scheduler_dlq=dlq,
        alert_email="alerts@example.com",
        env=aws_env,
    )

    template = Template.from_stack(notifications_stack)

    template.resource_count_is("AWS::SNS::Topic", 1)
    template.resource_count_is("AWS::CloudWatch::Alarm", 1)
    template.has_resource_properties(
        "AWS::CloudWatch::Alarm",
        {
            "Threshold": 1,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
        },
    )


def test_runtime_alarm_stack_resources_created(app, aws_env):
    # Dummy stack to fulfill dependencies
    dummy_stack = cdk.Stack(app, "dummy-topic-stack", env=aws_env)
    topic = cdk.aws_sns.Topic(dummy_stack, "TestTopic")

    runtime_alarm_stack = AvatureEtlRuntimeAlarmStack(
        app,
        "avature-etl-runtime-alarms-dev",
        prefix="avature-etl",
        stage="dev",
        topic=topic,  # ty: ignore[invalid-argument-type]
        spider_name="avature",
        env=aws_env,
    )

    template = Template.from_stack(runtime_alarm_stack)

    template.resource_count_is("AWS::CloudWatch::Alarm", 1)

    alarms = template.find_resources(
        "AWS::CloudWatch::Alarm",
        {
            "Properties": {
                "AlarmActions": [{"Fn::ImportValue": Match.any_value()}],
                "OKActions": [{"Fn::ImportValue": Match.any_value()}],
            }
        },
    )
    assert len(alarms) == 1, "Expected alarm to have both AlarmActions and OKActions set"
