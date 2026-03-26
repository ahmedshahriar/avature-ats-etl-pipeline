"""Helpers for reading and classifying seed URLs."""

from __future__ import annotations

import csv
from pathlib import Path
from urllib.parse import urlparse, urlunparse

SEED_URL_COLUMN = "url"
INTERNAL_SEED_PATH_SEGMENTS: frozenset[str] = frozenset({"internalcareers", "internalcareer"})
NON_PRODUCTION_SEED_PREFIXES: frozenset[str] = frozenset(
    {"staging", "sandbox", "uat", "qa", "test", "pentest", "demo", "training"}
)


def read_seed_file(path: Path) -> list[str]:
    """Read raw URLs from the required `url` column in the seed CSV."""
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"Seed CSV at {path} is missing a header row")

        fieldnames = {name.strip().lower(): name for name in reader.fieldnames if name}
        url_field = fieldnames.get(SEED_URL_COLUMN)
        if url_field is None:
            raise ValueError(f"Seed CSV at {path} must include a '{SEED_URL_COLUMN}' column")

        urls: list[str] = []
        for row in reader:
            raw_url = (row.get(url_field) or "").strip()
            if raw_url and not raw_url.startswith("#"):
                urls.append(raw_url)
        return urls


def has_http_scheme(url: str) -> bool:
    parsed = urlparse(url.strip())
    return parsed.scheme.lower() in {"http", "https"}


def normalize_seed_url(url: str) -> str:
    text = url.strip().rstrip(",").strip()
    parsed = urlparse(text)
    host = (parsed.hostname or "").lower()
    if not host:
        return text

    netloc = f"{host}:{parsed.port}" if parsed.port else host
    normalized = parsed._replace(
        scheme=(parsed.scheme or "https").lower(),
        netloc=netloc,
        fragment="",
    )
    return str(urlunparse(normalized))


def is_internal_seed_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        path_parts = {part.lower() for part in parsed.path.split("/") if part}
        return bool(path_parts & INTERNAL_SEED_PATH_SEGMENTS)
    except Exception:
        return False


def is_nonproduction_seed_url(url: str) -> bool:
    if is_internal_seed_url(url):
        return True

    try:
        parsed = urlparse(url)
        subdomain = ((parsed.hostname or "").lower().split(".")[0]).strip()
        if subdomain in NON_PRODUCTION_SEED_PREFIXES:
            return True

        path_parts = {part.lower() for part in parsed.path.split("/") if part}
        return bool(path_parts & NON_PRODUCTION_SEED_PREFIXES)
    except Exception:
        return False


def load_seed_urls(path: Path) -> list[str]:
    urls: list[str] = []
    for raw_url in read_seed_file(path):
        normalized_url = normalize_seed_url(raw_url)
        if not has_http_scheme(normalized_url):
            continue
        if is_internal_seed_url(normalized_url):
            continue
        urls.append(normalized_url)
    return urls


def portal_key_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    parts = [part for part in parsed.path.split("/") if part]
    if "careers" in parts:
        idx = parts.index("careers")
        portal_path = "/".join(parts[: idx + 1])
    else:
        portal_path = "/".join(parts[:2]) if len(parts) >= 2 else "/".join(parts[:1])
    return f"{host}/{portal_path}".rstrip("/")
