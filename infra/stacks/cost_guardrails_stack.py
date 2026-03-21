from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_budgets as budgets
from constructs import Construct


class AvatureEtlCostGuardrailsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
        monthly_budget_usd: float | None = None,
        alert_email: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            description=f"Avature ETL Cost Guardrails Stack [{stage}]",
            **kwargs,
        )

        if monthly_budget_usd is None or alert_email is None:
            CfnOutput(self, "BudgetEnabled", value="false")
            self.budget = None
            return

        budget_name = f"{prefix}-{stage}-monthly-cost-budget"

        budget = budgets.CfnBudget(
            self,
            "MonthlyCostBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_name=budget_name,
                budget_type="COST",
                time_unit="MONTHLY",
                budget_limit=budgets.CfnBudget.SpendProperty(
                    amount=monthly_budget_usd,
                    unit="USD",
                ),
            ),
            notifications_with_subscribers=[
                budgets.CfnBudget.NotificationWithSubscribersProperty(
                    notification=budgets.CfnBudget.NotificationProperty(
                        comparison_operator="GREATER_THAN",
                        notification_type="FORECASTED",
                        threshold=80,
                        threshold_type="PERCENTAGE",
                    ),
                    subscribers=[
                        budgets.CfnBudget.SubscriberProperty(
                            subscription_type="EMAIL",
                            address=alert_email,
                        )
                    ],
                ),
                budgets.CfnBudget.NotificationWithSubscribersProperty(
                    notification=budgets.CfnBudget.NotificationProperty(
                        comparison_operator="GREATER_THAN",
                        notification_type="ACTUAL",
                        threshold=100,
                        threshold_type="PERCENTAGE",
                    ),
                    subscribers=[
                        budgets.CfnBudget.SubscriberProperty(
                            subscription_type="EMAIL",
                            address=alert_email,
                        )
                    ],
                ),
            ],
        )

        CfnOutput(self, "BudgetEnabled", value="true")
        CfnOutput(self, "BudgetName", value=budget_name)

        self.budget = budget
