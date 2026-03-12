"""
Unit tests for GitHubOidcRolesStack.

Separate test for custom bootstrap_qualifier creates its own isolated App/stack.
"""

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template
from stacks.github_oidc_roles_stack import GitHubOidcRolesStack

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------
PROJECT_NAME = "my-project"
GITHUB_OWNER = "my-org"
GITHUB_REPO = "my-repo"
GITHUB_REPOSITORY_ID = "123456789"
ECR_REPO_NAME = "my-project-scraper"
ACCOUNT = "111111111111"
REGION = "us-east-1"
DEFAULT_QUALIFIER = "hnb659fds"

ECR_REPO_ARN = f"arn:aws:ecr:{REGION}:{ACCOUNT}:repository/{ECR_REPO_NAME}"
BOOTSTRAP_PARAM_ARN = f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter/cdk-bootstrap/{DEFAULT_QUALIFIER}/version"


def _bootstrap_role_arns(qualifier: str, account: str = ACCOUNT, region: str = REGION) -> list[str]:
    """Return the 4 CDK bootstrap role ARNs for a given qualifier."""
    return [
        f"arn:aws:iam::{account}:role/cdk-{qualifier}-deploy-role-{account}-{region}",
        f"arn:aws:iam::{account}:role/cdk-{qualifier}-file-publishing-role-{account}-{region}",
        f"arn:aws:iam::{account}:role/cdk-{qualifier}-image-publishing-role-{account}-{region}",
        f"arn:aws:iam::{account}:role/cdk-{qualifier}-lookup-role-{account}-{region}",
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def aws_env() -> cdk.Environment:
    """Standard dummy AWS environment (no real AWS calls)."""
    return cdk.Environment(account=ACCOUNT, region=REGION)


@pytest.fixture(scope="module")
def oidc_stack(aws_env: cdk.Environment) -> GitHubOidcRolesStack:
    """Synthesise the OIDC roles stack once for the whole module."""
    app = cdk.App()
    return GitHubOidcRolesStack(
        app,
        "test-github-oidc-stack",
        project_name=PROJECT_NAME,
        github_owner=GITHUB_OWNER,
        github_repo=GITHUB_REPO,
        github_repository_id=GITHUB_REPOSITORY_ID,
        ecr_repository_name=ECR_REPO_NAME,
        env=aws_env,
    )


@pytest.fixture(scope="module")
def template(oidc_stack: GitHubOidcRolesStack) -> Template:
    """CDK template assertions helper derived from the synthesised stack."""
    return Template.from_stack(oidc_stack)


# ===========================================================================
# 1. OIDC Provider
# ===========================================================================


def test_oidc_provider_is_created(template: Template) -> None:
    """Exactly one GitHub OIDC provider custom resource must be created.

    CDK uses a Custom Resource backed by a Lambda to manage the IAM OIDC provider
    (``Custom::AWSCDKOpenIdConnectProvider``).  There should be exactly one.
    """
    template.resource_count_is("Custom::AWSCDKOpenIdConnectProvider", 1)


def test_oidc_provider_url_and_client_id(template: Template) -> None:
    """The OIDC provider must reference GitHub's token endpoint with sts.amazonaws.com as the audience.

    The audience ``sts.amazonaws.com`` is required so that GitHub Actions tokens
    can be exchanged for AWS credentials via ``AssumeRoleWithWebIdentity``.
    """
    template.has_resource_properties(
        "Custom::AWSCDKOpenIdConnectProvider",
        {
            "Url": "https://token.actions.githubusercontent.com",
            "ClientIDList": ["sts.amazonaws.com"],
        },
    )


# ===========================================================================
# 2. IAM Role counts
# ===========================================================================


def test_total_iam_role_count(template: Template) -> None:
    """Stack must contain exactly 3 IAM roles.

    Breakdown:
    * ``GitHubActionsDeployRoleDev``  — deploy role for the dev environment
    * ``GitHubActionsDeployRoleProd`` — deploy role for the prod environment
    * CDK custom-resource provider Lambda execution role (internal, not deploy-related)
    """
    template.resource_count_is("AWS::IAM::Role", 3)


def test_two_attached_iam_policies(template: Template) -> None:
    """Each deploy role must produce exactly one consolidated ``AWS::IAM::Policy``.

    CDK consolidates all ``add_to_policy`` calls on the same role into a single
    CloudFormation policy resource, so 2 roles → 2 policy resources.
    """
    template.resource_count_is("AWS::IAM::Policy", 2)


# ===========================================================================
# 3. Role names and descriptions
# ===========================================================================


def test_dev_role_name_and_description(template: Template) -> None:
    """Dev deploy role must follow the ``{project}-gha-deploy-dev`` naming convention."""
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "RoleName": f"{PROJECT_NAME}-gha-deploy-dev",
            "Description": f"GitHub Actions OIDC deploy role for {PROJECT_NAME} dev",
        },
    )


def test_prod_role_name_and_description(template: Template) -> None:
    """Prod deploy role must follow the ``{project}-gha-deploy-prod`` naming convention."""
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "RoleName": f"{PROJECT_NAME}-gha-deploy-prod",
            "Description": f"GitHub Actions OIDC deploy role for {PROJECT_NAME} prod",
        },
    )


# ===========================================================================
# 4. Trust policies (AssumeRolePolicyDocument)
# ===========================================================================


def test_dev_role_trust_policy_conditions(template: Template) -> None:
    """Dev role trust policy must be scoped to the ``dev`` GitHub Actions environment.

    All four ``StringEquals`` conditions are required to prevent other repos or
    environments from assuming this role.
    """
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "RoleName": f"{PROJECT_NAME}-gha-deploy-dev",
            "AssumeRolePolicyDocument": Match.object_like(
                {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": "sts:AssumeRoleWithWebIdentity",
                                    "Effect": "Allow",
                                    "Condition": {
                                        "StringEquals": {
                                            "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                                            "token.actions.githubusercontent.com:sub": (
                                                f"repo:{GITHUB_OWNER}/{GITHUB_REPO}:environment:dev"
                                            ),
                                            "token.actions.githubusercontent.com:repository_id": GITHUB_REPOSITORY_ID,
                                            "token.actions.githubusercontent.com:environment": "dev",
                                        }
                                    },
                                }
                            )
                        ]
                    )
                }
            ),
        },
    )


def test_prod_role_trust_policy_conditions(template: Template) -> None:
    """Prod role trust policy must be scoped to the ``prod`` GitHub Actions environment."""
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "RoleName": f"{PROJECT_NAME}-gha-deploy-prod",
            "AssumeRolePolicyDocument": Match.object_like(
                {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": "sts:AssumeRoleWithWebIdentity",
                                    "Effect": "Allow",
                                    "Condition": {
                                        "StringEquals": {
                                            "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                                            "token.actions.githubusercontent.com:sub": (
                                                f"repo:{GITHUB_OWNER}/{GITHUB_REPO}:environment:prod"
                                            ),
                                            "token.actions.githubusercontent.com:repository_id": GITHUB_REPOSITORY_ID,
                                            "token.actions.githubusercontent.com:environment": "prod",
                                        }
                                    },
                                }
                            )
                        ]
                    )
                }
            ),
        },
    )


def test_dev_and_prod_trust_policies_are_isolated(template: Template) -> None:
    """Dev and prod trust conditions must differ — no cross-environment token acceptance.

    Extracts the ``sub`` claim from both deploy roles and asserts they target
    separate GitHub Actions environments.
    """
    roles = template.find_resources(
        "AWS::IAM::Role",
        {"Properties": {"RoleName": Match.string_like_regexp(f"^{PROJECT_NAME}-gha-deploy-(dev|prod)$")}},
    )
    assert len(roles) == 2, "Expected exactly 2 deploy roles"

    sub_claims = [
        role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]["Condition"]["StringEquals"][
            "token.actions.githubusercontent.com:sub"
        ]
        for role in roles.values()
    ]
    assert sorted(sub_claims) == sorted(
        [
            f"repo:{GITHUB_OWNER}/{GITHUB_REPO}:environment:dev",
            f"repo:{GITHUB_OWNER}/{GITHUB_REPO}:environment:prod",
        ]
    ), "Trust policy sub claims must be scoped to separate environments"


# ===========================================================================
# 5. IAM policy statements — EcrAuth
# ===========================================================================


def test_ecr_auth_statement_present_in_both_policies(template: Template) -> None:
    """``ecr:GetAuthorizationToken`` on ``*`` must appear in both deploy role policies.

    This action is account-scoped, not repository-scoped, so ``Resource: "*"`` is
    required by the ECR API.
    """
    ecr_auth_matcher = Match.object_like(
        {
            "Sid": "EcrAuth",
            "Effect": "Allow",
            "Action": "ecr:GetAuthorizationToken",
            "Resource": "*",
        }
    )
    policies = template.find_resources(
        "AWS::IAM::Policy",
        {"Properties": {"PolicyDocument": {"Statement": Match.array_with([ecr_auth_matcher])}}},
    )
    assert len(policies) == 2, f"Both deploy role policies must contain the EcrAuth statement; found {len(policies)}"


# ===========================================================================
# 6. IAM policy statements — EcrPushProjectRepository
# ===========================================================================


def test_ecr_push_statement_targets_correct_repository(template: Template) -> None:
    """ECR push statement must be scoped to the configured repository ARN only."""
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Sid": "EcrPushProjectRepository",
                                "Effect": "Allow",
                                "Resource": ECR_REPO_ARN,
                            }
                        )
                    ]
                )
            }
        },
    )


def test_ecr_push_statement_contains_all_required_actions(template: Template) -> None:
    """ECR push statement must grant all 10 actions needed for a full image push."""
    expected_actions = [
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
    ]
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Sid": "EcrPushProjectRepository",
                                "Resource": ECR_REPO_ARN,
                                "Action": Match.array_with(expected_actions),
                            }
                        )
                    ]
                )
            }
        },
    )


# ===========================================================================
# 7. IAM policy statements — AssumeCdkBootstrapRoles
# ===========================================================================


def test_assume_bootstrap_roles_statement_present(template: Template) -> None:
    """``sts:AssumeRole`` must be granted on all 4 CDK bootstrap roles.

    These roles are required for ``cdk deploy`` to write assets and deploy stacks.
    """
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Sid": "AssumeCdkBootstrapRoles",
                                "Effect": "Allow",
                                "Action": "sts:AssumeRole",
                                "Resource": Match.array_with(_bootstrap_role_arns(DEFAULT_QUALIFIER)),
                            }
                        )
                    ]
                )
            }
        },
    )


def test_assume_bootstrap_roles_covers_all_four_roles(template: Template) -> None:
    """All four CDK bootstrap roles (deploy, file-publishing, image-publishing, lookup) must be present."""
    expected = _bootstrap_role_arns(DEFAULT_QUALIFIER)

    all_policies = template.find_resources("AWS::IAM::Policy")
    # Collect the AssumeRole resources from any policy that has the AssumeCdkBootstrapRoles statement
    found_resources: list[str] = []
    for policy in all_policies.values():
        for stmt in policy["Properties"]["PolicyDocument"]["Statement"]:
            if stmt.get("Sid") == "AssumeCdkBootstrapRoles":
                resources = stmt.get("Resource", [])
                found_resources = resources if isinstance(resources, list) else [resources]
                break

    assert found_resources, "AssumeCdkBootstrapRoles statement not found in any policy"
    for arn in expected:
        assert arn in found_resources, f"Missing bootstrap role ARN: {arn}"


# ===========================================================================
# 8. IAM policy statements — ReadBootstrapMetadata
# ===========================================================================


def test_bootstrap_metadata_statement_present(template: Template) -> None:
    """ReadBootstrapMetadata must include SSM and CloudFormation read actions.

    This is required by ``cdk deploy`` to verify the bootstrap version and
    query existing stack state.
    """
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Sid": "ReadBootstrapMetadata",
                                "Effect": "Allow",
                                "Action": Match.array_with(
                                    [
                                        "ssm:GetParameter",
                                        "ssm:GetParameters",
                                        "cloudformation:DescribeStacks",
                                    ]
                                ),
                                "Resource": Match.array_with(["*", BOOTSTRAP_PARAM_ARN]),
                            }
                        )
                    ]
                )
            }
        },
    )


# ===========================================================================
# 9. CloudFormation Outputs
# ===========================================================================


def test_cfn_output_dev_role_arn_exists(template: Template) -> None:
    """``DevRoleArn`` CloudFormation output must be exported for downstream pipelines."""
    template.has_output("DevRoleArn", {})


def test_cfn_output_prod_role_arn_exists(template: Template) -> None:
    """``ProdRoleArn`` CloudFormation output must be exported for downstream pipelines."""
    template.has_output("ProdRoleArn", {})


# ===========================================================================
# 10. Custom bootstrap qualifier
# ===========================================================================


def test_custom_bootstrap_qualifier_reflected_in_bootstrap_role_arns() -> None:
    """A non-default ``bootstrap_qualifier`` must be used in all bootstrap role ARNs.

    Verifies that the qualifier is not hardcoded inside the stack.
    """
    custom_qualifier = "custom123"
    app = cdk.App()
    custom_stack = GitHubOidcRolesStack(
        app,
        "test-oidc-custom-qualifier",
        project_name=PROJECT_NAME,
        github_owner=GITHUB_OWNER,
        github_repo=GITHUB_REPO,
        github_repository_id=GITHUB_REPOSITORY_ID,
        ecr_repository_name=ECR_REPO_NAME,
        bootstrap_qualifier=custom_qualifier,
        env=cdk.Environment(account=ACCOUNT, region=REGION),
    )
    custom_template = Template.from_stack(custom_stack)

    custom_template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Sid": "AssumeCdkBootstrapRoles",
                                "Resource": Match.array_with(_bootstrap_role_arns(custom_qualifier)),
                            }
                        )
                    ]
                )
            }
        },
    )


def test_custom_bootstrap_qualifier_reflected_in_ssm_param_arn() -> None:
    """A non-default ``bootstrap_qualifier`` must appear in the SSM bootstrap parameter ARN."""
    custom_qualifier = "custom123"
    app = cdk.App()
    custom_stack = GitHubOidcRolesStack(
        app,
        "test-oidc-custom-qualifier-ssm",
        project_name=PROJECT_NAME,
        github_owner=GITHUB_OWNER,
        github_repo=GITHUB_REPO,
        github_repository_id=GITHUB_REPOSITORY_ID,
        ecr_repository_name=ECR_REPO_NAME,
        bootstrap_qualifier=custom_qualifier,
        env=cdk.Environment(account=ACCOUNT, region=REGION),
    )
    custom_template = Template.from_stack(custom_stack)

    custom_param_arn = f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter/cdk-bootstrap/{custom_qualifier}/version"
    custom_template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Sid": "ReadBootstrapMetadata",
                                "Resource": Match.array_with(["*", custom_param_arn]),
                            }
                        )
                    ]
                )
            }
        },
    )
