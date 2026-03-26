from pathlib import Path

from aws_cdk import Aws, CfnOutput, Duration, Stack, TimeZone
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_scheduler as scheduler
from aws_cdk import aws_scheduler_targets as scheduler_targets
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class AvatureEtlWorkflowStack(Stack):
    """
    End-to-end workflow:
      1. Run ECS scraper task synchronously
      2. Run Athena silver promotion query (valid bronze rows) via Athena
      3. Run Athena history snapshot promotion query via Athena
      4. Finish

    Notes:
      - silver promotion is SQL-idempotent via anti-join
      - history snapshot promotion is SQL-idempotent via anti-join
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
        ecs_task_family: str,
        ecs_container_name: str,
        ecs_task_execution_role_arn: str,
        ecs_task_role_arn: str,
        ecs_task_security_group,
        analytics_database_name: str,
        athena_workgroup_name: str,
        notification_topic_arn: str | None = None,
        schedule_enabled: bool = True,
        schedule_hour: str = "9",
        schedule_minute: str = "0",
        schedule_timezone: str = "UTC",
        athena_poll_seconds: int = 15,
        workflow_timeout_minutes: int = 180,
        ecs_task_timeout_minutes: int = 150,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            description=f"Avature ETL Workflow Stack [{stage}]",
            **kwargs,
        )

        sql_dir = Path(__file__).resolve().parents[1] / "sql"

        silver_insert_default_sql = self._load_sql_template(
            sql_dir / "04_silver_jobs_incremental_insert.sql",
            database_name=analytics_database_name,
            run_date_filter="CAST(current_date AS varchar)",
        )

        silver_insert_manual_sql = self._load_sql_template(
            sql_dir / "04_silver_jobs_incremental_insert.sql",
            database_name=analytics_database_name,
            run_date_filter="'{}'",
        )
        history_snapshot_insert_default_sql = self._load_sql_template(
            sql_dir / "07_silver_jobs_history_snapshot_incremental_insert.sql",
            database_name=analytics_database_name,
            run_date_filter="CAST(current_date AS varchar)",
        )
        history_snapshot_insert_manual_sql = self._load_sql_template(
            sql_dir / "07_silver_jobs_history_snapshot_incremental_insert.sql",
            database_name=analytics_database_name,
            run_date_filter="'{}'",
        )

        log_group = logs.LogGroup(
            self,
            "WorkflowLogGroup",
            log_group_name=f"/aws/vendedlogs/states/{prefix}-{stage}-workflow",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        public_subnet_ids = [subnet.subnet_id for subnet in ecs_cluster.vpc.public_subnets]
        # Use Step Functions ECS integration via CustomState so the workflow stack does not
        # import the ECS TaskDefinition construct cross-stack. Passing the stable task family
        # lets ECS resolve the latest ACTIVE revision at runtime.
        ecs_run_task_common_parameters = {
            "Cluster": ecs_cluster.cluster_arn,
            "LaunchType": "FARGATE",
            "TaskDefinition": ecs_task_family,
            "PlatformVersion": "LATEST",
            "EnableECSManagedTags": True,
            "PropagateTags": "TASK_DEFINITION",
            "NetworkConfiguration": {
                "AwsvpcConfiguration": {
                    "Subnets": public_subnet_ids,
                    "SecurityGroups": [ecs_task_security_group.security_group_id],
                    "AssignPublicIp": "ENABLED",
                }
            },
        }

        run_scraper_default = sfn.CustomState(
            self,
            "RunScraperTask",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": ecs_run_task_common_parameters,
                "TimeoutSeconds": ecs_task_timeout_minutes * 60,
                "ResultPath": "$.ecs",
            },
        )

        # manual override run
        # start the workflow with:
        # {
        #   "run_date_override": "2026-03-20",
        #   "run_id_override": "20260320T120000Z"
        # }
        run_scraper_with_manual_overrides = sfn.CustomState(
            self,
            "RunScraperTaskWithManualOverrides",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    **ecs_run_task_common_parameters,
                    "Overrides": {
                        "ContainerOverrides": [
                            {
                                "Name": ecs_container_name,
                                "Environment": [
                                    {
                                        "Name": "RUN_DATE",
                                        "Value.$": "$.run_date_override",
                                    },
                                    {
                                        "Name": "RUN_ID",
                                        "Value.$": "$.run_id_override",
                                    },
                                ],
                            }
                        ]
                    },
                },
                "TimeoutSeconds": ecs_task_timeout_minutes * 60,
                "ResultPath": "$.ecs",
            },
        )

        # run_scraper_default.add_retry(
        #     errors=["ECS.AmazonECSException", "ECS.ServerException", "States.TaskFailed"],
        #     interval=Duration.seconds(30),
        #     max_attempts=2,
        #     backoff_rate=2.0,
        # )
        #
        # run_scraper_with_manual_overrides.add_retry(
        #     errors=["ECS.AmazonECSException", "ECS.ServerException", "States.TaskFailed"],
        #     interval=Duration.seconds(30),
        #     max_attempts=2,
        #     backoff_rate=2.0,
        # )

        invalid_manual_override_input = sfn.Fail(
            self,
            "InvalidManualOverrideInput",
            cause="Provide both run_date_override and run_id_override, or neither.",
        )

        scraper_task_failed = sfn.Fail(
            self,
            "ScraperTaskFailed",
            cause="ECS scraper task failed or timed out.",
        )

        athena_submission_failed = sfn.Fail(
            self,
            "AthenaSubmissionFailed",
            cause="Failed to submit Athena silver promotion query.",
        )
        history_snapshot_submission_failed = sfn.Fail(
            self,
            "HistorySnapshotSubmissionFailed",
            cause="Failed to submit Athena history snapshot promotion query.",
        )

        athena_poll_failed = sfn.Fail(
            self,
            "AthenaPollFailed",
            cause="Failed while polling Athena silver promotion query.",
        )
        history_snapshot_poll_failed = sfn.Fail(
            self,
            "HistorySnapshotPollFailed",
            cause="Failed while polling Athena history snapshot promotion query.",
        )

        run_scraper_default.add_catch(
            scraper_task_failed,
            errors=["States.ALL"],
            result_path="$.error",
        )

        run_scraper_with_manual_overrides.add_catch(
            scraper_task_failed,
            errors=["States.ALL"],
            result_path="$.error",
        )

        use_manual_ecs_overrides = sfn.Choice(self, "UseManualEcsOverrides?")
        use_manual_history_snapshot_overrides = sfn.Choice(self, "UseManualHistorySnapshotOverrides?")

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

        start_silver_insert_default = tasks.AthenaStartQueryExecution(
            self,
            "StartSilverInsertDefault",
            query_string=silver_insert_default_sql,
            work_group=athena_workgroup_name,
            query_execution_context=tasks.QueryExecutionContext(
                database_name=analytics_database_name,
            ),
            result_path="$.athenaStart",
        )

        start_silver_insert_manual = tasks.AthenaStartQueryExecution(
            self,
            "StartSilverInsertManual",
            query_string=sfn.JsonPath.format(
                silver_insert_manual_sql,
                sfn.JsonPath.string_at("$.run_date_override"),
            ),
            work_group=athena_workgroup_name,
            query_execution_context=tasks.QueryExecutionContext(
                database_name=analytics_database_name,
            ),
            result_path="$.athenaStart",
        )
        start_history_snapshot_default = tasks.AthenaStartQueryExecution(
            self,
            "StartHistorySnapshotDefault",
            query_string=history_snapshot_insert_default_sql,
            work_group=athena_workgroup_name,
            query_execution_context=tasks.QueryExecutionContext(
                database_name=analytics_database_name,
            ),
            result_path="$.historySnapshotStart",
        )
        start_history_snapshot_manual = tasks.AthenaStartQueryExecution(
            self,
            "StartHistorySnapshotManual",
            query_string=sfn.JsonPath.format(
                history_snapshot_insert_manual_sql,
                sfn.JsonPath.string_at("$.run_date_override"),
            ),
            work_group=athena_workgroup_name,
            query_execution_context=tasks.QueryExecutionContext(
                database_name=analytics_database_name,
            ),
            result_path="$.historySnapshotStart",
        )

        for athena_start in (start_silver_insert_default, start_silver_insert_manual):
            athena_start.add_retry(
                errors=[
                    "Athena.InternalServerException",
                    "Athena.TooManyRequestsException",
                    "States.TaskFailed",
                ],
                interval=Duration.seconds(20),
                max_attempts=3,
                backoff_rate=2.0,
            )
            athena_start.add_catch(
                athena_submission_failed,
                errors=["States.ALL"],
                result_path="$.error",
            )
        for history_start in (start_history_snapshot_default, start_history_snapshot_manual):
            history_start.add_retry(
                errors=[
                    "Athena.InternalServerException",
                    "Athena.TooManyRequestsException",
                    "States.TaskFailed",
                ],
                interval=Duration.seconds(20),
                max_attempts=3,
                backoff_rate=2.0,
            )
            history_start.add_catch(
                history_snapshot_submission_failed,
                errors=["States.ALL"],
                result_path="$.error",
            )

        wait_for_silver_query = sfn.Wait(
            self,
            "WaitForSilverAthena",
            time=sfn.WaitTime.duration(Duration.seconds(athena_poll_seconds)),
        )

        get_silver_query = tasks.AthenaGetQueryExecution(
            self,
            "GetSilverAthenaQueryExecution",
            query_execution_id=sfn.JsonPath.string_at("$.athenaStart.QueryExecutionId"),
            result_path="$.athenaQuery",
        )

        get_silver_query.add_retry(
            errors=[
                "Athena.InternalServerException",
                "Athena.TooManyRequestsException",
                "States.TaskFailed",
            ],
            interval=Duration.seconds(20),
            max_attempts=6,
            backoff_rate=2.0,
        )

        get_silver_query.add_catch(
            athena_poll_failed,
            errors=["States.ALL"],
            result_path="$.error",
        )
        wait_for_history_snapshot_query = sfn.Wait(
            self,
            "WaitForHistorySnapshotAthena",
            time=sfn.WaitTime.duration(Duration.seconds(athena_poll_seconds)),
        )
        get_history_snapshot_query = tasks.AthenaGetQueryExecution(
            self,
            "GetHistorySnapshotAthenaQueryExecution",
            query_execution_id=sfn.JsonPath.string_at("$.historySnapshotStart.QueryExecutionId"),
            result_path="$.historySnapshotQuery",
        )
        get_history_snapshot_query.add_retry(
            errors=[
                "Athena.InternalServerException",
                "Athena.TooManyRequestsException",
                "States.TaskFailed",
            ],
            interval=Duration.seconds(20),
            max_attempts=6,
            backoff_rate=2.0,
        )
        get_history_snapshot_query.add_catch(
            history_snapshot_poll_failed,
            errors=["States.ALL"],
            result_path="$.error",
        )

        workflow_succeeded = sfn.Succeed(self, "WorkflowSucceeded")
        silver_promotion_failed = sfn.Fail(self, "SilverPromotionFailed")
        history_snapshot_promotion_failed = sfn.Fail(self, "HistorySnapshotPromotionFailed")

        silver_query_status = sfn.Choice(self, "SilverAthenaQueryFinished?")
        silver_query_status.when(
            sfn.Condition.string_equals("$.athenaQuery.QueryExecution.Status.State", "SUCCEEDED"),
            use_manual_history_snapshot_overrides,
        )
        silver_query_status.when(
            sfn.Condition.or_(
                sfn.Condition.string_equals("$.athenaQuery.QueryExecution.Status.State", "FAILED"),
                sfn.Condition.string_equals("$.athenaQuery.QueryExecution.Status.State", "CANCELLED"),
            ),
            silver_promotion_failed,
        )
        silver_query_status.otherwise(wait_for_silver_query)
        history_snapshot_query_status = sfn.Choice(self, "HistorySnapshotAthenaQueryFinished?")
        history_snapshot_query_status.when(
            sfn.Condition.string_equals("$.historySnapshotQuery.QueryExecution.Status.State", "SUCCEEDED"),
            workflow_succeeded,
        )
        history_snapshot_query_status.when(
            sfn.Condition.or_(
                sfn.Condition.string_equals("$.historySnapshotQuery.QueryExecution.Status.State", "FAILED"),
                sfn.Condition.string_equals("$.historySnapshotQuery.QueryExecution.Status.State", "CANCELLED"),
            ),
            history_snapshot_promotion_failed,
        )
        history_snapshot_query_status.otherwise(wait_for_history_snapshot_query)

        run_scraper_default.next(start_silver_insert_default)
        run_scraper_with_manual_overrides.next(start_silver_insert_manual)

        start_silver_insert_default.next(wait_for_silver_query)
        start_silver_insert_manual.next(wait_for_silver_query)

        wait_for_silver_query.next(get_silver_query)
        get_silver_query.next(silver_query_status)

        start_history_snapshot_default.next(wait_for_history_snapshot_query)
        start_history_snapshot_manual.next(wait_for_history_snapshot_query)

        wait_for_history_snapshot_query.next(get_history_snapshot_query)
        get_history_snapshot_query.next(history_snapshot_query_status)

        use_manual_ecs_overrides.when(
            both_overrides_present,
            run_scraper_with_manual_overrides,
        )
        use_manual_ecs_overrides.when(
            only_one_override_present,
            invalid_manual_override_input,
        )
        use_manual_ecs_overrides.otherwise(run_scraper_default)
        use_manual_history_snapshot_overrides.when(
            both_overrides_present,
            start_history_snapshot_manual,
        )
        use_manual_history_snapshot_overrides.when(
            only_one_override_present,
            invalid_manual_override_input,
        )
        use_manual_history_snapshot_overrides.otherwise(start_history_snapshot_default)

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

        task_definition_arn_pattern = (
            f"arn:{Aws.PARTITION}:ecs:{Aws.REGION}:{Aws.ACCOUNT_ID}:task-definition/{ecs_task_family}:*"
        )
        stepfunctions_ecs_events_rule_arn = (
            f"arn:{Aws.PARTITION}:events:{Aws.REGION}:{Aws.ACCOUNT_ID}:rule/StepFunctionsGetEventsForECSTaskRule"
        )

        state_machine.role.grant_principal.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["ecs:RunTask"],
                resources=[task_definition_arn_pattern],
            )
        )
        state_machine.role.grant_principal.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["ecs:StopTask", "ecs:DescribeTasks"],
                resources=["*"],
            )
        )
        state_machine.role.grant_principal.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["events:PutTargets", "events:PutRule", "events:DescribeRule"],
                resources=[stepfunctions_ecs_events_rule_arn],
            )
        )
        state_machine.role.grant_principal.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[ecs_task_execution_role_arn, ecs_task_role_arn],
            )
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

        self.scheduler_dlq = None
        self.scheduler_dlq_alarm = None

        if schedule_enabled:
            scheduler_dlq = sqs.Queue(
                self,
                "WorkflowSchedulerDlq",
                queue_name=f"{prefix}-{stage}-workflow-scheduler-dlq",
                retention_period=Duration.days(14) if stage.lower() == "prod" else Duration.days(3),
            )

            schedule_target = scheduler_targets.StepFunctionsStartExecution(
                state_machine,
                dead_letter_queue=scheduler_dlq,
                retry_attempts=0,
            )

            workflow_schedule = scheduler.Schedule(
                self,
                "DailyWorkflowSchedule",
                schedule_name=f"{prefix}-{stage}-workflow-daily-schedule",
                description=f"Daily schedule for Avature ETL workflow [{stage}]",
                enabled=True,
                schedule=scheduler.ScheduleExpression.cron(
                    minute=schedule_minute,
                    hour=schedule_hour,
                    month="*",
                    week_day="*",
                    year="*",
                    time_zone=TimeZone.of(schedule_timezone),
                ),
                time_window=scheduler.TimeWindow.off(),
                target=schedule_target,  # ty: ignore[invalid-argument-type]
            )

            if notification_topic is not None:
                scheduler_dlq_alarm = cloudwatch.Alarm(
                    self,
                    "WorkflowSchedulerDlqVisibleMessagesAlarm",
                    alarm_name=f"{prefix}-{stage}-workflow-scheduler-dlq-visible-messages",
                    alarm_description="Workflow schedule DLQ has one or more "
                    "failed invocations waiting for investigation.",
                    metric=scheduler_dlq.metric_approximate_number_of_messages_visible(
                        # Use a longer period to avoid alerting on transient failures that resolve quickly
                        # (e.g., due to retries or eventual consistency).
                        period=Duration.minutes(5),
                        statistic="Maximum",
                    ),
                    threshold=1,
                    evaluation_periods=1,
                    datapoints_to_alarm=1,
                    comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                    treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
                )
                scheduler_dlq_alarm.add_alarm_action(cloudwatch_actions.SnsAction(notification_topic))  # ty: ignore[invalid-argument-type]
                self.scheduler_dlq_alarm = scheduler_dlq_alarm
                CfnOutput(self, "WorkflowSchedulerDlqAlarmName", value=scheduler_dlq_alarm.alarm_name)

            self.scheduler_dlq = scheduler_dlq
            CfnOutput(self, "WorkflowScheduleName", value=workflow_schedule.schedule_name)
            CfnOutput(self, "WorkflowScheduleArn", value=workflow_schedule.schedule_arn)
            CfnOutput(self, "WorkflowSchedulerDlqUrl", value=scheduler_dlq.queue_url)

    @staticmethod
    def _load_sql_template(path: Path, *, database_name: str, run_date_filter: str) -> str:
        sql = path.read_text(encoding="utf-8")
        return sql.replace("__DATABASE_NAME__", database_name).replace("__RUN_DATE_FILTER__", run_date_filter)
