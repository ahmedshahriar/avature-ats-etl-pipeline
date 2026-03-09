from aws_cdk import CfnOutput, CfnParameter, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from constructs import Construct


class AvatureEtlEcsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
        ecs_task_cpu: int,
        ecs_task_memory: int,
        outputs_bucket: s3.IBucket,
        seen_jobs_table: dynamodb.ITable,
        ecs_log_group: logs.ILogGroup,
        repository: ecr.IRepository,
        scraper_runtime_env: dict[str, str],
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        stage = (stage or "dev").lower()
        is_prod = stage == "prod"

        # Existing default VPC lookup
        vpc = ec2.Vpc.from_lookup(self, "DefaultVpc", is_default=True)

        # ECS Cluster
        cluster = ecs.Cluster(
            self,
            "Cluster",
            cluster_name=f"{prefix}-cluster",
            vpc=vpc,
            # either use ENHANCED or ENABLED for optimal performance and features.
            # ENHANCED is newer and recommended if supported.
            container_insights_v2=ecs.ContainerInsights.ENABLED if is_prod else ecs.ContainerInsights.DISABLED,
        )

        # Security Group for one-off/scheduled batch runs
        task_sg = ec2.SecurityGroup(
            self,
            "TaskSecurityGroup",
            vpc=vpc,
            security_group_name=f"{prefix}-task-sg",
            description="Security group for Avature ETL ECS tasks",
            allow_all_outbound=True,
        )

        # ---- Roles ----
        # ServicePrincipal implements IPrincipal and Role implements IRole at runtime;
        # ty uses nominal subtyping and doesn't recognise CDK's structural interfaces.
        execution_role: iam.IRole = iam.Role(  # ty: ignore[invalid-assignment]
            self,
            "ExecutionRole",
            role_name=f"{prefix}-ecs-execution-role" if not is_prod else None,
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),  # ty: ignore[invalid-argument-type]
            description="ECS task execution role for pulling images and writing logs",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ],
        )

        task_role: iam.IRole = iam.Role(  # ty: ignore[invalid-assignment]
            self,
            "TaskRole",
            role_name=f"{prefix}-ecs-task-role" if not is_prod else None,
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),  # ty: ignore[invalid-argument-type]
            description="Application task role for S3/DynamoDB access",
        )

        # Least-privilege data permissions
        # Write-only: PutObject + AbortMultipartUpload — sufficient for Scrapy S3 feed export
        outputs_bucket.grant_put(task_role)
        seen_jobs_table.grant(task_role, "dynamodb:PutItem", "dynamodb:DescribeTable")

        # Image tag supplied at deploy time (Option A friendly)
        image_tag = CfnParameter(
            self,
            "ImageTag",
            type="String",
            default="latest",
            description="ECR image tag to run for the scraper container",
        )

        # Fargate task definition (ARM64)
        task_definition = ecs.FargateTaskDefinition(
            self,
            "TaskDefinition",
            family=f"{prefix}-task",
            cpu=ecs_task_cpu,
            memory_limit_mib=ecs_task_memory,
            execution_role=execution_role,
            task_role=task_role,
            runtime_platform=ecs.RuntimePlatform(
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
                cpu_architecture=ecs.CpuArchitecture.ARM64,
            ),
        )

        task_definition.add_container(
            "ScraperContainer",
            container_name=f"{prefix}-scraper",
            image=ecs.ContainerImage.from_ecr_repository(
                repository,
                tag=image_tag.value_as_string,
            ),
            essential=True,
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="ecs",
                log_group=ecs_log_group,
            ),
            environment={
                "DEPLOY_ENV": "aws",
                "S3_BUCKET_NAME": outputs_bucket.bucket_name,
                "DYNAMODB_TABLE_NAME": seen_jobs_table.table_name,
                **scraper_runtime_env,
            },
            command=["scrapy", "crawl", "avature"],
            working_directory="/app",
        )

        # ---- Outputs ----
        CfnOutput(self, "ClusterName", value=cluster.cluster_name)
        CfnOutput(self, "TaskDefinitionArn", value=task_definition.task_definition_arn)
        CfnOutput(self, "TaskSecurityGroupId", value=task_sg.security_group_id)
        CfnOutput(self, "ExecutionRoleArn", value=execution_role.role_arn)
        CfnOutput(self, "TaskRoleArn", value=task_role.role_arn)
        CfnOutput(self, "ImageTagParameterName", value=image_tag.logical_id)

        self.cluster = cluster
        self.task_definition = task_definition
        self.task_security_group = task_sg
        self.execution_role = execution_role
        self.task_role = task_role
