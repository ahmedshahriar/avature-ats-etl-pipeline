from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_sns as sns
from constructs import Construct


class AvatureEtlRuntimeAlarmStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
        topic: sns.ITopic,
        spider_name: str = "avature",
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            description=f"Avature ETL Runtime Alarm Stack [{stage}]",
            **kwargs,
        )

        run_failed_metric = cloudwatch.Metric(
            namespace="AvatureETL",
            metric_name="RunFailed",
            dimensions_map={
                "Project": prefix,
                "Stage": stage,
                "Spider": spider_name,
            },
            statistic="Sum",
            period=Duration.hours(24),
        )

        run_failed_alarm = cloudwatch.Alarm(
            self,
            "RunFailedAlarm",
            alarm_name=f"{prefix}-{stage}-run-failed",
            alarm_description="Scraper emitted RunFailed=1 via EMF in the last 24 hours.",
            metric=run_failed_metric,
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        run_failed_alarm.add_alarm_action(cloudwatch_actions.SnsAction(topic))  # ty: ignore[invalid-argument-type]
        run_failed_alarm.add_ok_action(cloudwatch_actions.SnsAction(topic))  # ty: ignore[invalid-argument-type]

        unique_jobs_metric = cloudwatch.Metric(
            namespace="AvatureETL",
            metric_name="UniqueJobs",
            dimensions_map={
                "Project": prefix,
                "Stage": stage,
                "Spider": spider_name,
            },
            statistic="Maximum",
            period=Duration.hours(24),
        )

        empty_run_alarm = cloudwatch.Alarm(
            self,
            "EmptyRunAlarm",
            alarm_name=f"{prefix}-{stage}-empty-run",
            alarm_description="Scraper completed but produced zero jobs in the last 24 hours.",
            metric=unique_jobs_metric,
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        empty_run_alarm.add_alarm_action(cloudwatch_actions.SnsAction(topic))  # ty: ignore[invalid-argument-type]

        CfnOutput(self, "RunFailedAlarmName", value=run_failed_alarm.alarm_name)
        CfnOutput(self, "EmptyRunAlarmName", value=empty_run_alarm.alarm_name)
