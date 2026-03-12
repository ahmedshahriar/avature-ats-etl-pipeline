import os

from aws_cdk import App, Environment
from stacks.github_oidc_roles_stack import GitHubOidcRolesStack


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


app = App()

GitHubOidcRolesStack(
    app,
    "GitHubOidcBootstrapStack",
    env=Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
    project_name=required("PROJECT_NAME"),
    github_owner=required("GITHUB_OWNER"),
    github_repo=required("GITHUB_REPO"),
    github_repository_id=required("GITHUB_REPOSITORY_ID"),
    ecr_repository_name=required("ECR_REPOSITORY"),
    bootstrap_qualifier=os.getenv("CDK_BOOTSTRAP_QUALIFIER", "hnb659fds"),
)

app.synth()
