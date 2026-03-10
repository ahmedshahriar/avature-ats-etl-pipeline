import aws_cdk as cdk
from aws_cdk.assertions import Match, Template
from stacks.base_stack import AvatureEtlBaseStack
from stacks.ecr_stack import AvatureEtlEcrStack
from stacks.ecs_schedule_stack import AvatureEtlEcsScheduleStack
from stacks.notifications_stack import AvatureEtlNotificationsStack

from infra.stacks.runtime_alarm_stack import AvatureEtlRuntimeAlarmStack


def test_base_stack_resources_created():
    app = cdk.App()
    stack = AvatureEtlBaseStack(
        app,
        "avature-etl-base",
        prefix="avature-etl",
        stage="dev",
        bucket_suffix="test-bucket",
        ddb_table_suffix="seen-jobs",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

    template = Template.from_stack(stack)

    # S3 bucket exists
    template.resource_count_is("AWS::S3::Bucket", 1)

    # DynamoDB table exists
    template.resource_count_is("AWS::DynamoDB::Table", 1)
    template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "TableName": "avature-etl-seen-jobs",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{"AttributeName": "job_hash", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "job_hash", "AttributeType": "S"}],
            # TTL enabled (CDK sets this structure)
            "TimeToLiveSpecification": {"AttributeName": "expires_at", "Enabled": True},
        },
    )

    # Log group exists (name should start with /ecs/avature-etl)
    template.resource_count_is("AWS::Logs::LogGroup", 1)
    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        {"LogGroupName": "/ecs/avature-etl"},
    )


def test_ecr_stack_resources_created():
    app = cdk.App()
    stack = AvatureEtlEcrStack(
        app,
        "avature-etl-ecr",
        prefix="avature-etl",
        stage="dev",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

    template = Template.from_stack(stack)

    template.resource_count_is("AWS::ECR::Repository", 1)
    template.has_resource_properties(
        "AWS::ECR::Repository",
        {
            "RepositoryName": "avature-etl-scraper",
            "ImageScanningConfiguration": {"ScanOnPush": True},
            "ImageTagMutability": "IMMUTABLE",
        },
    )


def test_ecs_stack_resources_created():
    app = cdk.App()

    base_stack = AvatureEtlBaseStack(
        app,
        "avature-etl-base",
        prefix="avature-etl",
        stage="dev",
        bucket_suffix="test-bucket",
        ddb_table_suffix="seen-jobs",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

    ecr_stack = AvatureEtlEcrStack(
        app,
        "avature-etl-ecr",
        prefix="avature-etl",
        stage="dev",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

    ecs_stack = AvatureEtlEcsScheduleStack(
        app,
        "avature-etl-ecs",
        prefix="avature-etl",
        stage="dev",
        ecs_task_cpu=256,
        ecs_task_memory=1024,
        scraper_runtime_env={},
        outputs_bucket=base_stack.outputs_bucket,
        seen_jobs_table=base_stack.seen_jobs_table,
        ecs_log_group=base_stack.ecs_log_group,
        repository=ecr_stack.repository,
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

    template = Template.from_stack(ecs_stack)

    template.resource_count_is("AWS::ECS::Cluster", 1)
    template.resource_count_is("AWS::ECS::TaskDefinition", 1)
    template.resource_count_is("AWS::EC2::SecurityGroup", 1)

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


def test_schedule_stack_resources_created():
    app = cdk.App()

    base_stack = AvatureEtlBaseStack(
        app,
        "avature-etl-base",
        prefix="avature-etl",
        stage="dev",
        bucket_suffix="test-bucket",
        ddb_table_suffix="seen-jobs",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

    ecr_stack = AvatureEtlEcrStack(
        app,
        "avature-etl-ecr",
        prefix="avature-etl",
        stage="dev",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

    # Scheduler resources are now merged into ecs_stack — no separate schedule_stack.
    ecs_stack = AvatureEtlEcsScheduleStack(
        app,
        "avature-etl-ecs",
        prefix="avature-etl",
        stage="dev",
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
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

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


def test_notifications_stack_resources_created():
    app = cdk.App()

    queue_stack = cdk.Stack(
        app,
        "queue-stack",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )
    dlq = cdk.aws_sqs.Queue(queue_stack, "TestDlq")

    notifications_stack = AvatureEtlNotificationsStack(
        app,
        "avature-etl-notifications-dev",
        prefix="avature-etl",
        stage="dev",
        scheduler_dlq=dlq,
        alert_email="alerts@example.com",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

    template = Template.from_stack(notifications_stack)

    template.resource_count_is("AWS::SNS::Topic", 1)
    template.resource_count_is("AWS::CloudWatch::Alarm", 1)
    template.resource_count_is("AWS::SNS::Subscription", 1)

    template.has_resource_properties(
        "AWS::CloudWatch::Alarm",
        {
            "Threshold": 1,
            "EvaluationPeriods": 1,
            "DatapointsToAlarm": 1,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
        },
    )


def test_runtime_alarm_stack_resources_created():
    app = cdk.App()

    topic_stack = cdk.Stack(
        app,
        "topic-stack",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )
    topic = cdk.aws_sns.Topic(topic_stack, "TestTopic")

    runtime_alarm_stack = AvatureEtlRuntimeAlarmStack(
        app,
        "avature-etl-runtime-alarms-dev",
        prefix="avature-etl",
        stage="dev",
        topic=topic,  # ty: ignore[invalid-argument-type]
        spider_name="avature",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )

    template = Template.from_stack(runtime_alarm_stack)

    template.resource_count_is("AWS::CloudWatch::Alarm", 1)
    template.has_resource_properties(
        "AWS::CloudWatch::Alarm",
        {
            "Threshold": 1,
            "EvaluationPeriods": 1,
            "DatapointsToAlarm": 1,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
        },
    )

    # Assert both AlarmActions and OKActions are present (added via add_alarm_action / add_ok_action)
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
