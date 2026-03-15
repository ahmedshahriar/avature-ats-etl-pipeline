from dataclasses import dataclass

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import (
    aws_dynamodb as dynamodb,
)
from aws_cdk import (
    aws_logs as logs,
)
from aws_cdk import (
    aws_s3 as s3,
)
from constructs import Construct


@dataclass(frozen=True)
class BaseStackOutputs:
    bucket_name: str
    table_name: str
    log_group_name: str


class AvatureEtlBaseStack(Stack):
    """
    Base infra:
      - S3 bucket for outputs
      - DynamoDB table for cross-run idempotency
      - CloudWatch Log Group for ECS logs
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str = "dev",
        bucket_suffix: str,
        ddb_table_suffix: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        stage = (stage or "dev").lower()
        is_prod = stage == "prod"

        # ---- Removal policies (simple toggle) ----
        bucket_removal = RemovalPolicy.RETAIN if is_prod else RemovalPolicy.DESTROY
        table_removal = RemovalPolicy.RETAIN if is_prod else RemovalPolicy.DESTROY
        log_removal = RemovalPolicy.RETAIN if is_prod else RemovalPolicy.DESTROY

        bucket_name = f"{prefix}-{stage}-{bucket_suffix}-{self.account}-{self.region}".lower()

        self.outputs_bucket = s3.Bucket(
            self,
            "OutputsBucket",
            bucket_name=bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,  # SSE-S3
            enforce_ssl=True,
            versioned=is_prod,  # versioning useful in prod; avoid extra noise in dev
            auto_delete_objects=not is_prod,
            removal_policy=bucket_removal,
        )

        # Lifecycle rules
        # No expiration rule on avature/bronze/jobs/
        # Ops artifacts (e.g. logs, reports) can be short-lived
        self.outputs_bucket.add_lifecycle_rule(
            prefix="avature/ops/",
            expiration=Duration.days(90 if is_prod else 14),
        )

        self.outputs_bucket.add_lifecycle_rule(
            prefix="avature/bronze/quarantine/",
            expiration=Duration.days(180 if is_prod else 30),
        )

        # ---- DynamoDB (idempotency / dedupe across runs) ----
        self.seen_jobs_table = dynamodb.Table(
            self,
            "SeenJobsTable",
            table_name=f"{prefix}-{stage}-{ddb_table_suffix}",
            partition_key=dynamodb.Attribute(name="job_hash", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,  # cheapest/least ops for daily batch
            time_to_live_attribute="expires_at",  # optional TTL; pipeline can set it
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=is_prod
            ),
            removal_policy=table_removal,
        )

        # ---- CloudWatch Log Group for ECS ----
        self.ecs_log_group = logs.LogGroup(
            self,
            "EcsLogGroup",
            log_group_name=f"/ecs/{prefix}-{stage}",
            retention=logs.RetentionDays.ONE_MONTH if is_prod else logs.RetentionDays.TWO_WEEKS,
            removal_policy=log_removal,
        )

        # ---- Outputs (so next stacks can reference) ----
        CfnOutput(self, "OutputsBucketName", value=self.outputs_bucket.bucket_name)
        CfnOutput(self, "SeenJobsTableName", value=self.seen_jobs_table.table_name)
        CfnOutput(self, "EcsLogGroupName", value=self.ecs_log_group.log_group_name)
