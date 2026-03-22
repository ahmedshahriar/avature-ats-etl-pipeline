"""
Unit tests for infra stacks.

"""

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Template
from stacks.analytics_stack import AvatureEtlAnalyticsStack
from stacks.base_stack import AvatureEtlBaseStack
from stacks.cost_guardrails_stack import AvatureEtlCostGuardrailsStack
from stacks.dashboard_stack import AvatureEtlDashboardStack
from stacks.ecr_stack import AvatureEtlEcrStack
from stacks.ecs_schedule_stack import AvatureEtlEcsScheduleStack
from stacks.notifications_stack import AvatureEtlNotificationsStack
from stacks.runtime_alarm_stack import AvatureEtlRuntimeAlarmStack
from stacks.workflow_stack import AvatureEtlWorkflowStack

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


@pytest.fixture
def notifications_stack(app, aws_env):
    """Instantiates the Notifications Stack."""
    return AvatureEtlNotificationsStack(
        app,
        "avature-etl-notifications",
        prefix="avature-etl",
        stage="dev",
        alert_email="alerts@example.com",
        env=aws_env,
    )


@pytest.fixture
def runtime_alarm_stack(app, aws_env, notifications_stack):
    """Instantiates the Runtime Alarm Stack with its dependencies."""
    return AvatureEtlRuntimeAlarmStack(
        app,
        "avature-etl-runtime-alarms",
        prefix="avature-etl",
        stage="dev",
        topic=notifications_stack.topic,
        spider_name="avature",
        env=aws_env,
    )


@pytest.fixture
def analytics_stack(app, aws_env, base_stack):
    """Instantiates the Analytics Stack."""
    return AvatureEtlAnalyticsStack(
        app,
        "avature-etl-analytics",
        prefix="avature-etl",
        stage="dev",
        outputs_bucket=base_stack.outputs_bucket,
        dataset_root="avature",
        athena_bytes_scanned_cutoff_mb=512,
        env=aws_env,
    )


@pytest.fixture
def dashboard_stack(app, aws_env):
    """Instantiates the Dashboard Stack."""
    return AvatureEtlDashboardStack(
        app,
        "avature-etl-dashboard",
        prefix="avature-etl",
        stage="dev",
        spider_name="avature",
        env=aws_env,
    )


@pytest.fixture
def workflow_stack(app, aws_env, ecs_stack, analytics_stack, notifications_stack):
    """Instantiates the Workflow Stack."""
    return AvatureEtlWorkflowStack(
        app,
        "avature-etl-workflow",
        prefix="avature-etl",
        stage="dev",
        ecs_cluster=ecs_stack.cluster,
        ecs_task_definition=ecs_stack.task_definition,
        ecs_task_security_group=ecs_stack.task_security_group,
        analytics_database_name=analytics_stack.database_name,
        athena_workgroup_name=analytics_stack.workgroup_name,
        notification_topic_arn=notifications_stack.topic.topic_arn,
        schedule_enabled=True,
        schedule_hour="10",
        schedule_minute="0",
        schedule_timezone="UTC",
        athena_poll_seconds=15,
        workflow_timeout_minutes=180,
        ecs_task_timeout_minutes=150,
        env=aws_env,
    )


@pytest.fixture
def cost_guardrails_stack(app, aws_env):
    return AvatureEtlCostGuardrailsStack(
        app,
        "avature-etl-cost-guardrails",
        prefix="avature-etl",
        stage="dev",
        monthly_budget_usd=10,
        alert_email="alerts@example.com",
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


def test_ecs_stack_has_no_schedule_when_disabled(ecs_stack):
    template = Template.from_stack(ecs_stack)

    template.resource_count_is("AWS::Scheduler::Schedule", 0)
    template.resource_count_is("AWS::SQS::Queue", 0)


def test_ecs_execution_role_uses_no_managed_policy(ecs_stack):
    template = Template.from_stack(ecs_stack)
    roles = template.find_resources("AWS::IAM::Role")

    execution_role_props = next(
        resource["Properties"]
        for resource in roles.values()
        if resource["Properties"].get("RoleName") == "avature-etl-dev-ecs-execution-role"
    )

    assert "ManagedPolicyArns" not in execution_role_props


def test_notifications_stack_resources_created(notifications_stack):
    template = Template.from_stack(notifications_stack)

    template.resource_count_is("AWS::SNS::Topic", 1)
    template.resource_count_is("AWS::CloudWatch::Alarm", 0)


def test_runtime_alarm_stack_resources_created(runtime_alarm_stack):
    template = Template.from_stack(runtime_alarm_stack)

    template.resource_count_is("AWS::CloudWatch::Alarm", 3)

    alarms = template.find_resources("AWS::CloudWatch::Alarm")
    alarm_properties = [resource["Properties"] for resource in alarms.values()]

    run_failed_alarm = next(props for props in alarm_properties if props["AlarmName"] == "avature-etl-dev-run-failed")
    assert run_failed_alarm["ComparisonOperator"] == "GreaterThanOrEqualToThreshold"
    assert run_failed_alarm["Threshold"] == 1
    assert len(run_failed_alarm["AlarmActions"]) == 1
    assert len(run_failed_alarm["OKActions"]) == 1

    empty_run_alarm = next(props for props in alarm_properties if props["AlarmName"] == "avature-etl-dev-empty-run")
    assert empty_run_alarm["ComparisonOperator"] == "LessThanThreshold"
    assert empty_run_alarm["Threshold"] == 1
    assert len(empty_run_alarm["AlarmActions"]) == 1
    assert "OKActions" not in empty_run_alarm

    low_success_rate_alarm = next(
        props for props in alarm_properties if props["AlarmName"] == "avature-etl-dev-low-job-detail-success-rate"
    )
    assert low_success_rate_alarm["ComparisonOperator"] == "LessThanThreshold"
    assert low_success_rate_alarm["Threshold"] == 0.95
    assert len(low_success_rate_alarm["AlarmActions"]) == 1
    assert "OKActions" not in low_success_rate_alarm


def test_analytics_stack_resources_created(analytics_stack):
    template = Template.from_stack(analytics_stack)

    template.resource_count_is("AWS::Glue::Database", 1)
    template.resource_count_is("AWS::Athena::WorkGroup", 1)
    template.resource_count_is("AWS::Athena::NamedQuery", 5)

    template.has_resource_properties(
        "AWS::Glue::Database",
        {
            "DatabaseInput": {
                "Name": "avature_etl_dev_analytics",
            }
        },
    )

    template.has_resource_properties(
        "AWS::Athena::WorkGroup",
        {
            "Name": "avature-etl-dev-athena",
            "State": "ENABLED",
            "WorkGroupConfiguration": {
                "BytesScannedCutoffPerQuery": 536870912,
            },
        },
    )

    named_queries = template.find_resources("AWS::Athena::NamedQuery")
    query_props = [resource["Properties"] for resource in named_queries.values()]
    query_names = {props["Name"] for props in query_props}

    assert query_names == {
        "avature-etl-dev-01-bronze-jobs-raw",
        "avature-etl-dev-02-ops-portal-summary-raw",
        "avature-etl-dev-03-silver-jobs-curated-ctas",
        "avature-etl-dev-04-silver-jobs-incremental-insert",
        "avature-etl-dev-05-gold-portal-daily-summary",
    }

    for props in query_props:
        assert props["Database"] == "avature_etl_dev_analytics"
        assert props["WorkGroup"] == "avature-etl-dev-athena"


def test_dashboard_stack_resources_created(dashboard_stack):
    template = Template.from_stack(dashboard_stack)

    template.resource_count_is("AWS::CloudWatch::Dashboard", 1)
    template.has_resource_properties(
        "AWS::CloudWatch::Dashboard",
        {
            "DashboardName": "avature-etl-dev-ops",
        },
    )


def test_workflow_stack_resources_created(workflow_stack):
    template = Template.from_stack(workflow_stack)

    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
    template.resource_count_is("AWS::Scheduler::Schedule", 1)
    template.resource_count_is("AWS::SQS::Queue", 1)
    template.resource_count_is("AWS::Logs::LogGroup", 1)
    template.resource_count_is("AWS::CloudWatch::Alarm", 4)

    template.has_resource_properties(
        "AWS::StepFunctions::StateMachine",
        {
            "StateMachineName": "avature-etl-dev-workflow",
        },
    )

    template.has_resource_properties(
        "AWS::Scheduler::Schedule",
        {
            "State": "ENABLED",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "ScheduleExpression": "cron(0 10 ? * * *)",
            "ScheduleExpressionTimezone": "UTC",
        },
    )

    alarms = template.find_resources("AWS::CloudWatch::Alarm")
    alarm_properties = [resource["Properties"] for resource in alarms.values()]
    alarm_names = {props["AlarmName"] for props in alarm_properties}

    assert alarm_names == {
        "avature-etl-dev-workflow-failed",
        "avature-etl-dev-workflow-timed-out",
        "avature-etl-dev-workflow-throttled",
        "avature-etl-dev-workflow-scheduler-dlq-visible-messages",
    }

    for props in alarm_properties:
        assert props["ComparisonOperator"] == "GreaterThanOrEqualToThreshold"
        assert props["Threshold"] == 1
        assert len(props["AlarmActions"]) == 1


def test_cost_guardrails_stack_resources_created(cost_guardrails_stack):
    template = Template.from_stack(cost_guardrails_stack)

    template.resource_count_is("AWS::Budgets::Budget", 1)


def test_cost_guardrails_stack_skips_budget_without_email(app, aws_env):
    stack = AvatureEtlCostGuardrailsStack(
        app,
        "avature-etl-cost-guardrails-no-email",
        prefix="avature-etl",
        stage="dev",
        monthly_budget_usd=10,
        alert_email=None,
        env=aws_env,
    )
    template = Template.from_stack(stack)
    template.resource_count_is("AWS::Budgets::Budget", 0)
