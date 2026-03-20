from pathlib import Path

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as event_targets
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sns as sns
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class AvatureEtlWorkflowStack(Stack):
    """
    End-to-end workflow:
      1. Run ECS scraper task synchronously
      2. Run Athena silver promotion query (valid bronze rows) via Athena
      3. Finish

    Notes:
      - silver promotion is SQL-idempotent via anti-join
      - gold is currently a VIEW, so it does not need a daily refresh step
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
        ecs_cluster,
        ecs_task_definition,
        ecs_task_security_group,
        analytics_database_name: str,
        athena_workgroup_name: str,
        notification_topic_arn: str | None = None,
        schedule_enabled: bool = True,
        schedule_hour: str = "9",
        schedule_minute: str = "0",
        athena_poll_seconds: int = 15,
        workflow_timeout_minutes: int = 180,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            description=f"Avature ETL Workflow Stack [{stage}]",
            **kwargs,
        )

        sql_dir = Path(__file__).resolve().parents[1] / "sql"
        silver_insert_sql = self._load_sql_template(
            sql_dir / "04_silver_jobs_incremental_insert.sql",
            database_name=analytics_database_name,
        )

        log_group = logs.LogGroup(
            self,
            "WorkflowLogGroup",
            log_group_name=f"/aws/vendedlogs/states/{prefix}-{stage}-workflow",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        container_definition = ecs_task_definition.default_container
        if container_definition is None:
            raise ValueError("ecs_task_definition.default_container must be set for ECS overrides")

        run_scraper_default = tasks.EcsRunTask(
            self,
            "RunScraperTask",
            cluster=ecs_cluster,
            task_definition=ecs_task_definition,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            launch_target=tasks.EcsFargateLaunchTarget(platform_version=ecs.FargatePlatformVersion.LATEST),
            assign_public_ip=True,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_groups=[ecs_task_security_group],
            result_path="$.ecs",
        )
        # manual override run
        # start the workflow with:
        # {
        #   "run_date_override": "2026-03-20",
        #   "run_id_override": "20260320T120000Z"
        # }

        run_scraper_with_manual_overrides = tasks.EcsRunTask(
            self,
            "RunScraperTaskWithManualOverrides",
            cluster=ecs_cluster,
            task_definition=ecs_task_definition,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            launch_target=tasks.EcsFargateLaunchTarget(platform_version=ecs.FargatePlatformVersion.LATEST),
            assign_public_ip=True,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_groups=[ecs_task_security_group],
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=container_definition,
                    environment=[
                        tasks.TaskEnvironmentVariable(
                            name="RUN_DATE",
                            value=sfn.JsonPath.string_at("$.run_date_override"),
                        ),
                        tasks.TaskEnvironmentVariable(
                            name="RUN_ID",
                            value=sfn.JsonPath.string_at("$.run_id_override"),
                        ),
                    ],
                )
            ],
            result_path="$.ecs",
        )

        run_scraper_default.add_retry(
            errors=["ECS.AmazonECSException", "ECS.ServerException", "States.TaskFailed"],
            interval=Duration.seconds(30),
            max_attempts=2,
            backoff_rate=2.0,
        )

        run_scraper_with_manual_overrides.add_retry(
            errors=["ECS.AmazonECSException", "ECS.ServerException", "States.TaskFailed"],
            interval=Duration.seconds(30),
            max_attempts=2,
            backoff_rate=2.0,
        )

        invalid_manual_override_input = sfn.Fail(
            self,
            "InvalidManualOverrideInput",
            cause="Provide both run_date_override and run_id_override, or neither.",
        )

        use_manual_ecs_overrides = sfn.Choice(self, "UseManualEcsOverrides?")

        both_overrides_present = sfn.Condition.and_(
            sfn.Condition.is_present("$.run_date_override"),
            sfn.Condition.is_present("$.run_id_override"),
        )

        only_one_override_present = sfn.Condition.or_(
            sfn.Condition.and_(
                sfn.Condition.is_present("$.run_date_override"),
                sfn.Condition.not_(sfn.Condition.is_present("$.run_id_override")),
            ),
            sfn.Condition.and_(
                sfn.Condition.not_(sfn.Condition.is_present("$.run_date_override")),
                sfn.Condition.is_present("$.run_id_override"),
            ),
        )

        start_silver_insert = tasks.AthenaStartQueryExecution(
            self,
            "StartSilverInsert",
            query_string=silver_insert_sql,
            work_group=athena_workgroup_name,
            query_execution_context=tasks.QueryExecutionContext(
                database_name=analytics_database_name,
            ),
            result_path="$.athenaStart",
        )

        wait_for_query = sfn.Wait(
            self,
            "WaitForAthena",
            time=sfn.WaitTime.duration(Duration.seconds(athena_poll_seconds)),
        )

        get_query = tasks.AthenaGetQueryExecution(
            self,
            "GetAthenaQueryExecution",
            query_execution_id=sfn.JsonPath.string_at("$.athenaStart.QueryExecutionId"),
            result_path="$.athenaQuery",
        )

        workflow_succeeded = sfn.Succeed(self, "WorkflowSucceeded")
        silver_promotion_failed = sfn.Fail(self, "SilverPromotionFailed")

        query_status = sfn.Choice(self, "AthenaQueryFinished?")
        query_status.when(
            sfn.Condition.string_equals("$.athenaQuery.QueryExecution.Status.State", "SUCCEEDED"),
            workflow_succeeded,
        )
        query_status.when(
            sfn.Condition.or_(
                sfn.Condition.string_equals("$.athenaQuery.QueryExecution.Status.State", "FAILED"),
                sfn.Condition.string_equals("$.athenaQuery.QueryExecution.Status.State", "CANCELLED"),
            ),
            silver_promotion_failed,
        )
        query_status.otherwise(wait_for_query)

        run_scraper_default.next(start_silver_insert)
        run_scraper_with_manual_overrides.next(start_silver_insert)

        start_silver_insert.next(wait_for_query)
        wait_for_query.next(get_query)
        get_query.next(query_status)

        use_manual_ecs_overrides.when(
            both_overrides_present,
            run_scraper_with_manual_overrides,
        )
        use_manual_ecs_overrides.when(
            only_one_override_present,
            invalid_manual_override_input,
        )
        use_manual_ecs_overrides.otherwise(run_scraper_default)

        definition = use_manual_ecs_overrides

        state_machine = sfn.StateMachine(
            self,
            "WorkflowStateMachine",
            state_machine_name=f"{prefix}-{stage}-workflow",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(workflow_timeout_minutes),
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
        )

        notification_topic = None
        if notification_topic_arn:
            notification_topic = sns.Topic.from_topic_arn(
                self,
                "ImportedWorkflowAlarmTopic",
                notification_topic_arn,
            )

        executions_failed_metric = cloudwatch.Metric(
            namespace="AWS/States",
            metric_name="ExecutionsFailed",
            dimensions_map={"StateMachineArn": state_machine.state_machine_arn},
            statistic="Sum",
            period=Duration.hours(24),
        )

        executions_timed_out_metric = cloudwatch.Metric(
            namespace="AWS/States",
            metric_name="ExecutionsTimedOut",
            dimensions_map={"StateMachineArn": state_machine.state_machine_arn},
            statistic="Sum",
            period=Duration.hours(24),
        )

        execution_throttled_metric = cloudwatch.Metric(
            namespace="AWS/States",
            metric_name="ExecutionThrottled",
            dimensions_map={"StateMachineArn": state_machine.state_machine_arn},
            statistic="Sum",
            period=Duration.hours(24),
        )

        workflow_failed_alarm = cloudwatch.Alarm(
            self,
            "WorkflowFailedAlarm",
            alarm_name=f"{prefix}-{stage}-workflow-failed",
            alarm_description="Step Functions workflow reported one or more failed executions in the last 24 hours.",
            metric=executions_failed_metric,
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        workflow_timed_out_alarm = cloudwatch.Alarm(
            self,
            "WorkflowTimedOutAlarm",
            alarm_name=f"{prefix}-{stage}-workflow-timed-out",
            alarm_description="Step Functions workflow reported one or more timed out executions in the last 24 hours.",
            metric=executions_timed_out_metric,
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        workflow_throttled_alarm = cloudwatch.Alarm(
            self,
            "WorkflowThrottledAlarm",
            alarm_name=f"{prefix}-{stage}-workflow-throttled",
            alarm_description="Step Functions workflow experienced state transition throttling in the last 24 hours.",
            metric=execution_throttled_metric,
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        if notification_topic is not None:
            action = cloudwatch_actions.SnsAction(notification_topic)
            workflow_failed_alarm.add_alarm_action(action)  # ty: ignore[invalid-argument-type]
            workflow_timed_out_alarm.add_alarm_action(action)  # ty: ignore[invalid-argument-type]
            workflow_throttled_alarm.add_alarm_action(action)  # ty: ignore[invalid-argument-type]

        if schedule_enabled:
            rule = events.Rule(
                self,
                "DailyWorkflowSchedule",
                schedule=events.Schedule.cron(
                    minute=schedule_minute,
                    hour=schedule_hour,
                    month="*",
                    week_day="*",
                    year="*",
                ),
            )
            rule.add_target(event_targets.SfnStateMachine(state_machine))  # ty: ignore[invalid-argument-type]

        CfnOutput(self, "WorkflowStateMachineArn", value=state_machine.state_machine_arn)
        CfnOutput(self, "WorkflowStateMachineName", value=state_machine.state_machine_name)
        CfnOutput(self, "WorkflowFailedAlarmName", value=workflow_failed_alarm.alarm_name)
        CfnOutput(self, "WorkflowTimedOutAlarmName", value=workflow_timed_out_alarm.alarm_name)
        CfnOutput(self, "WorkflowThrottledAlarmName", value=workflow_throttled_alarm.alarm_name)

    @staticmethod
    def _load_sql_template(path: Path, *, database_name: str) -> str:
        sql = path.read_text(encoding="utf-8")
        return sql.replace("__DATABASE_NAME__", database_name)
