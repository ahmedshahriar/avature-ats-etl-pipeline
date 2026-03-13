FROM python:3.13-slim
LABEL authors="ahmedshahriar"
LABEL description="Docker image for Scrapy project to be deployed on AWS ECS"

# Set environment variables to prevent Python from writing .pyc files and to ensure output is not buffered
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies (adjust as needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies from requirements.txt (ensure you have a requirements.txt file in the same directory as this Dockerfile)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy project
COPY . /app

# Create a non-root user and switch to it
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

# Default command (can override in ECS)
CMD ["scrapy", "crawl", "avature"]
