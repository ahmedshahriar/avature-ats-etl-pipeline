# AWS Deployment Guide

This guide covers AWS deployment setup and release workflows for the Avature ATS ETL pipeline.

For project overview, local development, architecture, and outputs, see the [README](../../README.md).
For Athena bootstrap, daily workflow checks, alert handling, and rollback basics, see the [AWS operations runbook](../runbooks/avature-aws-runbook.md).

---

## 1. Environment-specific configuration

Environment-specific settings are defined in:

- [`infra/environments/dev.yaml`](../../infra/environments/dev.yaml)
- [`infra/environments/prod.yaml`](../../infra/environments/prod.yaml)

These files control settings such as:

- ECS task sizing
- schedule ownership via `schedule_target`
- alert routing
- Athena scan guardrails
- monthly budget alerting
- workflow / ECS timeout behavior
- scraper runtime parameters

At deploy time, these values are injected into the ECS task and supporting AWS resources.

---

## 2. AWS setup

If you want to deploy this project directly from your machine, make sure you have the following available locally:

- an AWS account and credentials with sufficient permissions to deploy CDK stacks and manage ECR, ECS, S3, DynamoDB, and related resources
- a bootstrapped CDK environment
- the `infra/` dependencies installed
- Docker with `buildx` support
- the AWS CLI configured for the target account/region
- the AWS CDK CLI installed

Install the infrastructure dependencies:

```bash
cd infra
uv sync --only-group infra
```

Bootstrap the AWS account and region for CDK:

```bash
cdk bootstrap aws://<ACCOUNT_ID>/<AWS_REGION>
```

This sets up the CDK toolkit resources required for deployment in the target AWS account and region.

---

## 3. Manual AWS deployment

This project’s ECS task pulls the scraper image from Amazon ECR, so a manual deployment has two parts:

1. build and push the Docker image to ECR
2. deploy the CDK stacks using that same image tag

### Deploy to `dev`

```bash
cd infra
uv sync --only-group infra

export AWS_REGION=<your-region>
export ECR_REPOSITORY=<your-ecr-repository-name>
export IMAGE_TAG=$(git rev-parse HEAD)
export PROJECT_NAME=$(uv run python -c "from pathlib import Path; from yaml import safe_load; print(safe_load(Path('environments/dev.yaml').read_text())['project_name'])")

# Ensure the shared ECR stack exists
cdk deploy "${PROJECT_NAME}-ecr" \
  --app "uv run python app.py" \
  --require-approval never \
  -c env=dev

# Log in to ECR
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

# Build and push the ARM64 image
cd ..
docker buildx build \
  --platform linux/arm64 \
  --push \
  --tag "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPOSITORY:$IMAGE_TAG" \
  .

# Deploy the application stacks using the same image tag
cd infra
cdk deploy --app "uv run python app.py" --all \
  --require-approval never \
  -c env=dev \
  -c imageTag="$IMAGE_TAG"
```

### Deploy to `prod`

For production, reuse the exact image tag that was already tested in `dev`:

```bash
cd infra
uv sync --only-group infra

export AWS_REGION=<your-region>
export ECR_REPOSITORY=<your-ecr-repository-name>
export IMAGE_TAG=<existing-dev-image-tag>

# Optional but recommended: verify the tag exists
aws ecr describe-images \
  --region "$AWS_REGION" \
  --repository-name "$ECR_REPOSITORY" \
  --image-ids imageTag="$IMAGE_TAG" >/dev/null

cdk deploy --app "uv run python app.py" --all \
  --require-approval never \
  -c env=prod \
  -c imageTag="$IMAGE_TAG"
```

This manual path does not require GitHub Actions or GitHub OIDC. Those are only needed for automated deployments from GitHub.

---

## 4. GitHub Actions deployment

For automated deployments, this repository uses GitHub Actions with AWS OIDC so that GitHub can assume short-lived AWS roles without storing long-lived AWS access keys.

Before GitHub Actions can deploy, complete this one-time setup.

### 1. Deploy the GitHub OIDC bootstrap stack

The bootstrap stack creates the GitHub Actions deploy roles for `dev` and `prod`.
It requires these environment variables:

- `PROJECT_NAME`
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_REPOSITORY_ID`
- `ECR_REPOSITORY`

Example:

```bash
cd infra

export PROJECT_NAME=avature-etl
export GITHUB_OWNER=<your-github-username-or-org>
export GITHUB_REPO=avature-ats-etl-pipeline
export GITHUB_REPOSITORY_ID=<your-github-repository-id>
export ECR_REPOSITORY=<your-ecr-repository-name>

cdk deploy --app "uv run python bootstrap_app.py" --require-approval never
```

This creates the IAM roles that GitHub Actions will assume for AWS deployments, with permissions scoped to the target ECR repository and CDK deployment actions.
Copy the created role ARNs for both `dev` and `prod` to use in the next step.

### 2. Configure GitHub variables and secrets

Repository variables:

- `AWS_REGION`
- `ECR_REPOSITORY`

Environment secrets:

For `dev`:

- `AWS_ROLE_ARN` — the ARN of the GitHub OIDC role created for `dev` in the previous step

For `prod`:

- `AWS_ROLE_ARN` — the ARN of the GitHub OIDC role created for `prod` in the previous step
- `ALERT_EMAIL` if alerts are enabled

### 3. Deploy through GitHub Actions

The repository follows a build-once-promote-many workflow:

1. push to `main` builds the image and deploys `dev`
2. the image is tagged with the full Git commit SHA
3. production deployment promotes the same tested image tag through the manual GitHub Actions workflow trigger

This keeps deployments immutable and avoids rebuilding different artifacts per environment.
