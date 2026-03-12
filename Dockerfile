FROM python:3.13-slim
LABEL authors="ahmedshahriar"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps for lxml/bs4 parsing
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Install deps (adjust if you use requirements.txt/pyproject)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy project
COPY . /app

RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

# Default command (can override in ECS)
CMD ["scrapy", "crawl", "avature"]
