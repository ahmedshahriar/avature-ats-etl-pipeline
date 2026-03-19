from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_cloudwatch as cloudwatch
from constructs import Construct


class AvatureEtlDashboardStack(Stack):
    """
    Small ops dashboard only.
    This is not intended to replace Athena for historical analysis.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
        spider_name: str = "avature",
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            description=f"Avature ETL Dashboard Stack [{stage}]",
            **kwargs,
        )

        dims = {
            "Project": prefix,
            "Stage": stage,
            "Spider": spider_name,
        }

        dashboard = cloudwatch.Dashboard(
            self,
            "Dashboard",
            dashboard_name=f"{prefix}-{stage}-ops",
        )

        def metric(name: str, stat: str = "Sum"):
            return cloudwatch.Metric(
                namespace="AvatureETL",
                metric_name=name,
                dimensions_map=dims,
                statistic=stat,
                period=Duration.hours(24),
            )

        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Run health",
                left=[
                    metric("RunSuccess", "Sum"),
                    metric("RunFailed", "Sum"),
                    metric("JobsExported", "Maximum"),
                ],
                right=[
                    metric("RunDurationSeconds", "Maximum"),
                ],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Quality guardrails",
                left=[
                    metric("JobDetailSuccessRate", "Average"),
                ],
                right=[
                    metric("JobsQuarantined", "Maximum"),
                    metric("DuplicateItemsDropped", "Maximum"),
                ],
                width=12,
            ),
        )

        CfnOutput(self, "DashboardName", value=dashboard.dashboard_name)
