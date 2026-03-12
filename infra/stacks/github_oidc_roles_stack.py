from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_iam as iam
from constructs import Construct


class GitHubOidcRolesStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_name: str,
        github_owner: str,
        github_repo: str,
        github_repository_id: str,
        ecr_repository_name: str,
        bootstrap_qualifier: str = "hnb659fds",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = Stack.of(self).account
        region = Stack.of(self).region

        provider = iam.OpenIdConnectProvider(
            self,
            "GitHubOidcProvider",
            url="https://token.actions.githubusercontent.com",
            client_ids=["sts.amazonaws.com"],
        )

        ecr_repo_arn = f"arn:aws:ecr:{region}:{account}:repository/{ecr_repository_name}"
        bootstrap_version_param_arn = (
            f"arn:aws:ssm:{region}:{account}:parameter/cdk-bootstrap/{bootstrap_qualifier}/version"
        )

        bootstrap_roles = [
            f"arn:aws:iam::{account}:role/cdk-{bootstrap_qualifier}-deploy-role-{account}-{region}",
            f"arn:aws:iam::{account}:role/cdk-{bootstrap_qualifier}-file-publishing-role-{account}-{region}",
            f"arn:aws:iam::{account}:role/cdk-{bootstrap_qualifier}-image-publishing-role-{account}-{region}",
            f"arn:aws:iam::{account}:role/cdk-{bootstrap_qualifier}-lookup-role-{account}-{region}",
        ]

        for env_name in ("dev", "prod"):
            role = iam.Role(
                self,
                f"GitHubActionsDeployRole{env_name.title()}",
                role_name=f"{project_name}-gha-deploy-{env_name}",
                description=f"GitHub Actions OIDC deploy role for {project_name} {env_name}",
                assumed_by=iam.WebIdentityPrincipal(  # ty: ignore[invalid-argument-type]
                    provider.open_id_connect_provider_arn,
                    conditions={
                        "StringEquals": {
                            "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                            "token.actions.githubusercontent.com:sub": (
                                f"repo:{github_owner}/{github_repo}:environment:{env_name}"
                            ),
                            "token.actions.githubusercontent.com:repository_id": github_repository_id,
                            "token.actions.githubusercontent.com:environment": env_name,
                        }
                    },
                ),
            )

            role.add_to_policy(
                iam.PolicyStatement(
                    sid="EcrAuth",
                    actions=["ecr:GetAuthorizationToken"],
                    resources=["*"],
                )
            )

            role.add_to_policy(
                iam.PolicyStatement(
                    sid="EcrPushProjectRepository",
                    actions=[
                        "ecr:BatchCheckLayerAvailability",
                        "ecr:BatchGetImage",
                        "ecr:CompleteLayerUpload",
                        "ecr:DescribeImages",
                        "ecr:DescribeRepositories",
                        "ecr:GetDownloadUrlForLayer",
                        "ecr:InitiateLayerUpload",
                        "ecr:ListImages",
                        "ecr:PutImage",
                        "ecr:UploadLayerPart",
                    ],
                    resources=[ecr_repo_arn],
                )
            )

            role.add_to_policy(
                iam.PolicyStatement(
                    sid="AssumeCdkBootstrapRoles",
                    actions=["sts:AssumeRole"],
                    resources=bootstrap_roles,
                )
            )

            role.add_to_policy(
                iam.PolicyStatement(
                    sid="ReadBootstrapMetadata",
                    actions=["ssm:GetParameter", "ssm:GetParameters", "cloudformation:DescribeStacks"],
                    resources=["*", bootstrap_version_param_arn],
                )
            )

            CfnOutput(
                self,
                f"{env_name.title()}RoleArn",
                value=role.role_arn,
            )
