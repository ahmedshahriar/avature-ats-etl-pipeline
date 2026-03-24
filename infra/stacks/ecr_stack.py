from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_ecr as ecr
from constructs import Construct


class AvatureEtlEcrStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, prefix: str, stage: str, **kwargs) -> None:
        super().__init__(scope, construct_id, description="Avature ETL ECR Stack", **kwargs)

        stage = (stage or "dev").lower()
        is_prod = stage == "prod"

        self.repository = ecr.Repository(
            self,
            "ScraperRepository",
            repository_name=f"{prefix}-scraper",
            image_scan_on_push=True,
            image_tag_mutability=ecr.TagMutability.IMMUTABLE,
            removal_policy=RemovalPolicy.RETAIN if is_prod else RemovalPolicy.DESTROY,
            empty_on_delete=False if is_prod else True,
        )

        # Rule 1: Delete untagged images (orphans/attestations) older than 3 days
        self.repository.add_lifecycle_rule(
            rule_priority=1,
            tag_status=ecr.TagStatus.UNTAGGED,
            max_image_age=Duration.days(3),
            description="Remove orphaned untagged images",
        )

        # Rule 2: Keep only the most recent 10-20 tagged images for rollbacks
        self.repository.add_lifecycle_rule(
            rule_priority=2,
            tag_status=ecr.TagStatus.ANY,
            max_image_count=20 if is_prod else 10,
            description="Keep recent deployments",
        )

        CfnOutput(self, "RepositoryName", value=self.repository.repository_name)
        CfnOutput(self, "RepositoryUri", value=self.repository.repository_uri)
