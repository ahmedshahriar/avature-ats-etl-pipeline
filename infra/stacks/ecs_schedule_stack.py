from aws_cdk import CfnOutput, Duration, Stack, TimeZone
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_scheduler_targets as scheduler_targets
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class AvatureEtlEcsScheduleStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
        image_tag: str,
        ecs_task_cpu: int,
        ecs_task_memory: int,
        outputs_bucket: s3.IBucket,
        seen_jobs_table: dynamodb.ITable,
        ecs_log_group: logs.ILogGroup,
        repository: ecr.IRepository,
        scraper_runtime_env: dict[str, str],
        container_insights_mode: str = "disabled",
        schedule_minute: str = "0",
        schedule_hour: str = "9",
        schedule_timezone: str = "UTC",
        schedule_enabled: bool = False,
        notification_topic_arn: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, description=f"Avature ETL ECS and Scheduler Stack [{stage}]", **kwargs)

        stage = (stage or "dev").lower()
        is_prod = stage == "prod"

        # Existing default VPC lookup
        vpc = ec2.Vpc.from_lookup(self, "DefaultVpc", is_default=True)

        # ECS Cluster
        container_insights_mode = (container_insights_mode or "disabled").lower()
        container_insights_lookup = {
            "disabled": ecs.ContainerInsights.DISABLED,
            "enabled": ecs.ContainerInsights.ENABLED,
            "enhanced": ecs.ContainerInsights.ENHANCED,
        }
        if container_insights_mode not in container_insights_lookup:
            raise ValueError("container_insights_mode must be one of: disabled, enabled, enhanced")

        cluster = ecs.Cluster(
            self,
            "Cluster",
            cluster_name=f"{prefix}-{stage}-cluster",
            vpc=vpc,
            # disabled for now as it will
            # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/cloudwatch-container-insights.html
            # either use ENHANCED or ENABLED for optimal performance and features.
            # ENHANCED is newer and recommended if supported.
            container_insights_v2=container_insights_lookup[container_insights_mode],
        )

        # Security Group for one-off/scheduled batch runs
        task_sg = ec2.SecurityGroup(
            self,
            "TaskSecurityGroup",
            vpc=vpc,
            security_group_name=f"{prefix}-{stage}-task-sg",
            description=f"Security group for {prefix} ECS tasks [{stage}]",
            allow_all_outbound=True,
        )

        # ---- Roles ----
        # ServicePrincipal implements IPrincipal and Role implements IRole at runtime;
        # ty uses nominal subtyping and doesn't recognise CDK's structural interfaces.
        execution_role = iam.Role(
            self,
            "ExecutionRole",
            role_name=f"{prefix}-{stage}-ecs-execution-role",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),  # ty: ignore[invalid-argument-type]
            description="Least-privilege ECS task execution role for pulling images and writing logs",
        )

        # ECR image pull permissions scoped to this repository
        repository.grant_pull(execution_role)

        # AmazonECSTaskExecutionRolePolicy includes logs:CreateLogStream and logs:PutLogEvents
        # Fargate agent uses this role to ship container stdout/stderr to CloudWatch via the awslogs driver.

        # ECR auth token must remain '*' per AWS API design
        execution_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ecr:GetAuthorizationToken"],
                resources=["*"],
            )
        )

        # awslogs driver permissions scoped to this log group
        execution_role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[f"{ecs_log_group.log_group_arn}:*"],
            )
        )

        task_role = iam.Role(
            self,
            "TaskRole",
            role_name=f"{prefix}-{stage}-ecs-task-role",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),  # ty: ignore[invalid-argument-type]
            description="Application task role for S3/DynamoDB access",
        )

        # Least-privilege data permissions
        # Write-only: PutObject + AbortMultipartUpload — sufficient for Scrapy S3 feed export
        outputs_bucket.grant_put(task_role)
        seen_jobs_table.grant(task_role, "dynamodb:PutItem", "dynamodb:DescribeTable")

        # Fargate task definition (ARM64)
        task_definition = ecs.FargateTaskDefinition(
            self,
            "TaskDefinition",
            family=f"{prefix}-{stage}-task",
            cpu=ecs_task_cpu,
            memory_limit_mib=ecs_task_memory,
            execution_role=execution_role,  # ty: ignore[invalid-argument-type]
            task_role=task_role,  # ty: ignore[invalid-argument-type]
            runtime_platform=ecs.RuntimePlatform(
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
                cpu_architecture=ecs.CpuArchitecture.ARM64,
            ),
        )

        task_definition.add_container(
            "ScraperContainer",
            container_name=f"{prefix}-{stage}-scraper",
            image=ecs.ContainerImage.from_ecr_repository(repository, tag=image_tag),
            essential=True,
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="ecs",
                log_group=ecs_log_group,
            ),
            environment={
                "DEPLOY_ENV": "aws",
                "PROJECT_NAME": prefix,
                "ENV_NAME": stage,
                "STACK_STAGE": stage,
                "S3_BUCKET_NAME": outputs_bucket.bucket_name,
                "DYNAMODB_TABLE_NAME": seen_jobs_table.table_name,
                **scraper_runtime_env,
            },
            command=["scrapy", "crawl", "avature"],
            working_directory="/app",
        )

        # NOTE:
        # The scheduler lives in the ECS stack, so TaskDefinition is no longer a
        # cross-stack reference. Cross-stack references still exist for more stable
        # resources like the DLQ -> notifications stack and SNS topic -> runtime alarms stack.

        # Standard SQS queue only (Scheduler DLQ does not support FIFO)
        notification_topic = None
        if notification_topic_arn:
            notification_topic = sns.Topic.from_topic_arn(
                self,
                "ImportedEcsScheduleAlarmTopic",
                notification_topic_arn,
            )

        self.dlq = None
        self.dlq_alarm = None

        if schedule_enabled:
            dlq = sqs.Queue(
                self,
                "SchedulerDlq",
                queue_name=f"{prefix}-{stage}-scheduler-dlq",
                retention_period=Duration.days(14) if is_prod else Duration.days(3),
            )

            # Explicit execution role for Scheduler
            scheduler_role = iam.Role(
                self,
                "SchedulerExecutionRole",
                role_name=f"{prefix}-{stage}-scheduler-role",
                assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),  # ty: ignore[invalid-argument-type]
                description="Execution role for EventBridge Scheduler to run ECS Fargate task",
            )

            # Allow Scheduler to send failed invocations to DLQ
            dlq.grant_send_messages(scheduler_role)

            # EventBridge Scheduler target:
            # - public subnets, assign public IP
            # - no retries + DLQ
            # - explicit task_count
            target = scheduler_targets.EcsRunFargateTask(
                cluster,
                task_definition=task_definition,
                role=scheduler_role,  # ty: ignore[invalid-argument-type]
                task_count=1,
                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
                assign_public_ip=True,
                security_groups=[task_sg],
                enable_ecs_managed_tags=True,
                propagate_tags=True,
                dead_letter_queue=dlq,
                # retry_attempts=0: safer default for batch scraper to avoid duplicate side effects
                retry_attempts=0,
            )

            sched = scheduler.Schedule(
                self,
                "DailySchedule",
                schedule_name=f"{prefix}-{stage}-daily-schedule",
                description=f"Daily schedule for Avature ETL ECS batch task [{stage}]",
                enabled=True,
                schedule=scheduler.ScheduleExpression.cron(
                    minute=schedule_minute,
                    hour=schedule_hour,
                    month="*",
                    week_day="*",
                    year="*",
                    # https://github.com/aws/aws-cdk/issues/21181#issuecomment-2941360602
                    time_zone=TimeZone.of(schedule_timezone),
                ),
                time_window=scheduler.TimeWindow.off(),
                target=target,  # ty: ignore[invalid-argument-type]
            )

            if notification_topic is not None:
                dlq_alarm = cloudwatch.Alarm(
                    self,
                    "SchedulerDlqVisibleMessagesAlarm",
                    alarm_name=f"{prefix}-{stage}-scheduler-dlq-visible-messages",
                    alarm_description="ECS schedule DLQ has one or more failed invocations waiting for investigation.",
                    metric=dlq.metric_approximate_number_of_messages_visible(
                        period=Duration.minutes(5),
                        statistic="Maximum",
                    ),
                    threshold=1,
                    evaluation_periods=1,
                    datapoints_to_alarm=1,
                    comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                    treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
                )
                dlq_alarm.add_alarm_action(cloudwatch_actions.SnsAction(notification_topic))  # ty: ignore[invalid-argument-type]
                self.dlq_alarm = dlq_alarm
                CfnOutput(self, "SchedulerDlqAlarmName", value=dlq_alarm.alarm_name)

            CfnOutput(self, "ScheduleName", value=sched.schedule_name)
            CfnOutput(self, "ScheduleArn", value=sched.schedule_arn)
            CfnOutput(self, "ScheduleTimezone", value=schedule_timezone)
            CfnOutput(self, "ScheduleExpression", value=f"cron({schedule_minute} {schedule_hour} ? * * *)")
            CfnOutput(self, "SchedulerDlqUrl", value=dlq.queue_url)
            CfnOutput(self, "SchedulerExecutionRoleArn", value=scheduler_role.role_arn)

            self.dlq = dlq

        # Outputs for easy reference in deploy time and tests
        CfnOutput(self, "ClusterName", value=cluster.cluster_name)
        CfnOutput(self, "TaskDefinitionArn", value=task_definition.task_definition_arn)
        CfnOutput(self, "TaskSecurityGroupId", value=task_sg.security_group_id)
        CfnOutput(self, "ExecutionRoleArn", value=execution_role.role_arn)
        CfnOutput(self, "TaskRoleArn", value=task_role.role_arn)
        CfnOutput(self, "ImageTag", value=image_tag)
        CfnOutput(self, "Stage", value=stage)
        CfnOutput(self, "ScheduleEnabled", value="true" if schedule_enabled else "false")

        self.cluster = cluster
        self.task_definition = task_definition
        self.task_security_group = task_sg
        self.execution_role = execution_role
        self.task_role = task_role
