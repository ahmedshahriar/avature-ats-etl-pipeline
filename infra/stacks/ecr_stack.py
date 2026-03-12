from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_ecr as ecr
from constructs import Construct


class AvatureEtlEcrStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, prefix: str, stage: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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

        # keep recent images only; enough for rollback without clutter
        self.repository.add_lifecycle_rule(max_image_count=20 if is_prod else 10)

        CfnOutput(self, "RepositoryName", value=self.repository.repository_name)
        CfnOutput(self, "RepositoryUri", value=self.repository.repository_uri)
