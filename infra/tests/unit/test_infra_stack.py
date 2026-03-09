import aws_cdk as cdk
from aws_cdk.assertions import Template
from stacks.base_stack import AvatureEtlBaseStack
from stacks.ecr_stack import AvatureEtlEcrStack
from stacks.ecs_stack import AvatureEtlEcsStack


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

    ecs_stack = AvatureEtlEcsStack(
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
