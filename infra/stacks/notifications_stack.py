from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class AvatureEtlNotificationsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
        scheduler_dlq: sqs.IQueue,
        alert_email: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            description=f"Avature ETL Notifications Stack [{stage}]",
            **kwargs,
        )

        stage = stage.lower()

        topic = sns.Topic(
            self,
            "AlertsTopic",
            topic_name=f"{prefix}-{stage}-alerts",
            display_name=f"{prefix}-{stage}-alerts",
        )

        if alert_email:
            topic.add_subscription(
                sns_subscriptions.EmailSubscription(alert_email)  # ty: ignore[invalid-argument-type]
            )

        dlq_alarm = cloudwatch.Alarm(
            self,
            "SchedulerDlqVisibleMessagesAlarm",
            alarm_name=f"{prefix}-{stage}-scheduler-dlq-visible-messages",
            alarm_description="Scheduler DLQ has one or more failed invocations waiting for investigation.",
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

        dlq_alarm.add_alarm_action(cloudwatch_actions.SnsAction(topic))  # ty: ignore[invalid-argument-type]

        CfnOutput(self, "AlertsTopicArn", value=topic.topic_arn)
        CfnOutput(self, "DlqAlarmName", value=dlq_alarm.alarm_name)
        CfnOutput(self, "AlertEmailConfigured", value="true" if alert_email else "false")

        self.topic = topic
        self.dlq_alarm = dlq_alarm
