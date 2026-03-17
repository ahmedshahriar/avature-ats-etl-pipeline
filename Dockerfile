# syntax=docker/dockerfile:1.7

FROM python:3.13-slim AS builder

# Set environment variables to prevent Python from writing .pyc files and to ensure output is not buffered
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Build dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv "$VIRTUAL_ENV"

COPY requirements.txt /app/requirements.txt

# Reuse BuildKit cache across builds
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip setuptools wheel && \
    pip install -r /app/requirements.txt

FROM python:3.13-slim AS runtime
LABEL authors="ahmedshahriar"
LABEL description="Docker image for Scrapy project to be deployed on AWS ECS"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Runtime libraries only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 \
    libxslt1.1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create unprivileged user before copy
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser

# Copy installed Python environment from builder
COPY --from=builder /opt/venv /opt/venv

# Copy app code with correct ownership in one step
COPY --chown=appuser:appuser . /app

USER appuser

# Useful for local runs; ECS already overrides this in the task definition
CMD ["scrapy", "crawl", "avature"]
