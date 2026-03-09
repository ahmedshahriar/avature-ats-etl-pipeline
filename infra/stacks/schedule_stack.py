from aws_cdk import CfnOutput, Duration, Stack, TimeZone
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_scheduler_targets as scheduler_targets
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class AvatureEtlScheduleStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
        cluster: ecs.ICluster,
        task_definition: ecs.FargateTaskDefinition,
        task_security_group: ec2.ISecurityGroup,
        schedule_minute: str = "0",
        schedule_hour: str = "9",
        schedule_timezone: str = "UTC",
        schedule_enabled: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            description=f"Avature ETL Schedule Stack [{stage}]",
            **kwargs,
        )

        stage = stage.lower()
        is_prod = stage == "prod"

        # Standard SQS queue only (Scheduler DLQ does not support FIFO)
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
            role_name=f"{prefix}-{stage}-scheduler-role" if not is_prod else None,
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),  # ty: ignore[invalid-argument-type]
            description="Execution role for EventBridge Scheduler to run ECS Fargate task",
        )

        # Allow Scheduler to send failed events/invocations to DLQ
        dlq.grant_send_messages(scheduler_role)

        # EventBridge Scheduler target:
        # - public subnets
        # - assign public IP
        # - no retries + DLQ
        # - explicit task_count
        target = scheduler_targets.EcsRunFargateTask(
            cluster,
            task_definition=task_definition,
            role=scheduler_role,  # ty: ignore[invalid-argument-type]
            task_count=1,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            assign_public_ip=True,
            security_groups=[task_security_group],
            enable_ecs_managed_tags=True,
            propagate_tags=True,
            dead_letter_queue=dlq,
            # automatic retries increase the chance of partial duplicate side effects,
            # even if DynamoDB dedupe protects some paths.
            # setting retry_attempts=0 is the safer default for the batch scraper
            retry_attempts=0,
        )

        sched = scheduler.Schedule(
            self,
            "DailySchedule",
            schedule_name=f"{prefix}-{stage}-daily-schedule",
            description=f"Daily schedule for Avature ETL ECS batch task [{stage}]",
            enabled=schedule_enabled,
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

        CfnOutput(self, "Stage", value=stage)
        CfnOutput(self, "ScheduleName", value=sched.schedule_name)
        CfnOutput(self, "ScheduleArn", value=sched.schedule_arn)
        CfnOutput(self, "ScheduleTimezone", value=schedule_timezone)
        CfnOutput(self, "ScheduleEnabled", value="true" if schedule_enabled else "false")
        CfnOutput(
            self,
            "ScheduleExpression",
            #
            value=f"cron({schedule_minute} {schedule_hour} ? * * *)",
        )
        CfnOutput(self, "SchedulerDlqUrl", value=dlq.queue_url)
        CfnOutput(self, "SchedulerExecutionRoleArn", value=scheduler_role.role_arn)
