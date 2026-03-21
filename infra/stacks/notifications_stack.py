from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from constructs import Construct


class AvatureEtlNotificationsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
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

        CfnOutput(self, "AlertsTopicArn", value=topic.topic_arn)
        CfnOutput(self, "AlertEmailConfigured", value="true" if alert_email else "false")

        self.topic = topic
