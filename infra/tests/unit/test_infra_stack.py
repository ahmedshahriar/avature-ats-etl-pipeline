import aws_cdk as cdk
from aws_cdk.assertions import Template
from stacks.base_stack import AvatureEtlBaseStack


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
