"""Scrapy spider for crawling Avature career portals."""

import csv
import hashlib
import json
import re
import urllib.parse
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse, urlunparse

import scrapy
from bs4 import BeautifulSoup

from ..items import AvatureJobItem


class AvatureSpider(scrapy.Spider):
    name = "avature"

    # Normalisation helpers reused from scraper.py
    LABEL_MAP = {
        "job number": "job_id",
        "job id": "job_id",
        "req #": "job_id",
        "ref #": "ref_number",
        "ref no": "ref_number",
        "career area": "career_area",
        "career field": "career_area",
        "career field subcategory": "career_area",
        "company": "company",
        "work location(s)": "locations",
        "location(s)": "locations",
        "locations": "locations",
        "location": "locations",
        "remote?": "remote",
        "remote": "remote",
        "working time": "employment_type",
        "job type": "employment_type",
        # posted_date
        "posted date": "posted_date",
        "date posted": "posted_date",
        "date": "posted_date",
        "open date": "posted_date",
        "opening date": "posted_date",
        "publication date": "posted_date",
        "publish date": "posted_date",
        "requisition date": "posted_date",
        "job posting date": "posted_date",
        "posted": "posted_date",
        # company — additional Avature variants
        "organization": "company",
        "organisation": "company",
        "employer": "company",
        "business": "company",
        "division": "company",
        "brand": "company",
        "entity": "company",
        "legal entity": "company",
        "hiring company": "company",
        "hiring organization": "company",
        "hiring organisation": "company",
        # apply_url
        "apply": "apply_url",
        "career area (you may select more than one)": "career_area",
        "state": "locations",
        "city": "locations",
        # exception
        "state/province/city": "locations",
        "business unit": "career_area",
        "time type": "employment_type",
        "workplace arrangement": "remote",
    }

    _EXCLUDED_PATH_SEGMENTS: frozenset[str] = frozenset({"internalcareers", "internalcareer"})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.allowed_domains: list[str] = []
        self.discovered_job_urls: set[str] = set()

    async def start(self):
        seed_path = self._resolve_seed_file()
        if not seed_path.exists():
            raise FileNotFoundError(f"seed_urls.csv not found at {seed_path}")

        stats = self.crawler.stats
        if stats:
            stats.set_value("crawl/input_seed_file", str(seed_path))
            stats.set_value("crawl/input_file_sha256", self._sha256_file(seed_path))

        seed_urls = self._read_seed_urls(seed_path)
        if stats:
            stats.set_value("crawl/input_seed_count", len(seed_urls))

        for url in seed_urls:
            hostname = urllib.parse.urlparse(url).hostname
            if hostname and hostname not in self.allowed_domains:
                self.allowed_domains.append(hostname)

            yield scrapy.Request(
                url,
                callback=self.parse_listing,
                meta={
                    "portal_key": self._portal_key_from_url(url),
                    "input_seed_url": url,
                    "request_kind": "listing",
                },
            )

    def parse_listing(self, response):
        """Parse a listing page and schedule requests for each job link and the next page."""
        if not hasattr(response, "text"):
            self.logger.warning("Non-text response at listing URL %s, skipping", response.url)
            return
        soup = BeautifulSoup(response.text, "lxml")
        current_url = response.url

        unique_detail_urls: list[str] = []
        seen_on_page: set[str] = set()

        for a in soup.select('article.article--result a[href*="/JobDetail/"]'):
            href = a.get("href")
            if not href:
                continue

            resolved = self._unwrap_job_href(str(href))
            job_url = urljoin(current_url, resolved)
            # Guard: skip non-http URLs (mailto:, javascript:, etc.)
            if not job_url.startswith(("http://", "https://")):
                continue
            canonical_job_url = self._canonicalize_detail_url(job_url)

            if canonical_job_url in seen_on_page:
                continue
            seen_on_page.add(canonical_job_url)
            unique_detail_urls.append(canonical_job_url)

            if canonical_job_url not in self.discovered_job_urls:
                self.discovered_job_urls.add(canonical_job_url)
                stats = self.crawler.stats
                if stats:
                    stats.inc_value("crawl/jobs_discovered_total")

        for link in unique_detail_urls:
            yield scrapy.Request(
                link,
                callback=self.parse_job,
                meta={
                    "portal_key": response.meta.get("portal_key") or self._portal_key_from_url(link),
                    "input_seed_url": response.meta.get("input_seed_url"),
                    "request_kind": "job_detail",
                },
            )

        next_link = self._find_next_page(soup=soup, current_url=current_url)
        if next_link:
            yield scrapy.Request(
                next_link,
                callback=self.parse_listing,
                meta={
                    "portal_key": response.meta.get("portal_key") or self._portal_key_from_url(link),
                    "input_seed_url": response.meta.get("input_seed_url"),
                    "request_kind": "pagination",
                },
            )

    def parse_job(self, response):
        """Parse a job detail page into a JobItem."""
        if not hasattr(response, "text"):
            self.logger.warning("Non-text response at job URL %s, skipping", response.url)
            return
        soup = BeautifulSoup(response.text, "lxml")
        item = AvatureJobItem()

        raw_source_url = response.url
        canonical_source_url = self._canonicalize_detail_url(raw_source_url)

        item["raw_source_url"] = raw_source_url
        item["canonical_source_url"] = canonical_source_url
        item["source_url"] = canonical_source_url
        item["portal_key"] = response.meta.get("portal_key") or self._portal_key_from_url(canonical_source_url)
        item["input_seed_url"] = response.meta.get("input_seed_url")
        item["run_id"] = self.settings.get("RUN_ID")
        item["run_date"] = self.settings.get("RUN_DATE")
        item["scraped_at"] = self._iso_now()

        meta = soup.find("meta", attrs={"name": "avature.portallist.search"})
        if meta and meta.get("content"):
            item["job_id"] = str(meta["content"]).strip()
        else:
            match = re.search(r"/([0-9]{3,})(?:[/?#]|$)", canonical_source_url)
            if match:
                item["job_id"] = match.group(1)

        # title
        title = None
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            title = str(og["content"]).strip()
        if not title:
            banner = (
                soup.select_one("h2.banner__text__title")
                or soup.select_one("h2.banner__text__title--0")
                or soup.select_one("h2.banner__text__title--3")
            )
            if banner:
                title = banner.get_text(strip=True)
        if not title:
            head_title = soup.find("title")
            if head_title:
                title = head_title.get_text(strip=True)
        item["title"] = title

        jobposting = self.parse_json_ld(soup)
        ld_description = None
        if jobposting:
            if not item.get("company"):
                org = jobposting.get("hiringOrganization") or {}
                if isinstance(org, dict) and org.get("name"):
                    item["company"] = org.get("name")
            if not item.get("posted_date") and jobposting.get("datePosted"):
                item["posted_date"] = jobposting.get("datePosted")
            ld_description = jobposting.get("description")
        # general info fields
        raw_fields = {}
        locations = []

        for div in soup.select("div.article__content__view__field"):
            label_div = div.select_one(".article__content__view__field__label")
            value_div = div.select_one(".article__content__view__field__value")
            if not value_div:
                continue

            value_text = " ".join(value_div.stripped_strings)
            if label_div:
                label = label_div.get_text(strip=True)
                norm = self.normalise_label(label)
                raw_fields[norm] = value_text
                attr = self.LABEL_MAP.get(norm)
                if attr:
                    if attr == "locations":
                        # split on comma or slash
                        parts = [p.strip() for p in re.split(r"[,/\n]", value_text) if p.strip()]
                        locations.extend(parts)
                    else:
                        item[attr] = value_text
            else:
                # description fallback
                current_desc = item.get("description_text") or ""
                item["description_text"] = (current_desc + "\n" + value_text).strip()

        if locations:
            seen_locations = set()
            deduped_locations = []
            for loc in locations:
                key = loc.strip().lower()
                if not key or key == "unknown" or key in seen_locations:
                    continue
                seen_locations.add(key)
                deduped_locations.append(loc.strip())
            item["locations"] = deduped_locations

        sections = []
        for article in soup.select("article.article--details"):
            heading = article.select_one(".article__header__text__title")
            if not heading:
                continue
            body_fields = article.select(".article__content__view__field .article__content__view__field__value")
            texts = []
            for div2 in body_fields:
                txt = " ".join(div2.stripped_strings)
                if txt:
                    texts.append(txt)
            if texts:
                sections.append("\n".join(texts))

        if sections:
            item["description_text"] = "\n\n".join([s.strip() for s in sections if s.strip()])
        elif not item.get("description_text") and ld_description:
            # strip HTML tags
            cleaned = re.sub("<[^<]+?>", "", ld_description)
            item["description_text"] = cleaned.strip()

        for a in soup.find_all("a", href=True):
            href = str(a["href"])
            text = a.get_text(strip=True).lower()
            if "applicationmethods?jobid=" in href.lower():
                item["apply_url"] = urljoin(response.url, href)
                break
            if "login?jobid=" in href.lower() and ("apply" in text or "apply now" in text):
                item["apply_url"] = urljoin(response.url, href)
                break
        # fallback company detection
        if not item.get("company"):
            subtitle = soup.select_one(".banner__text__subtitle")
            if subtitle:
                txt = subtitle.get_text(strip=True)
                if txt and "careers" not in txt.lower():
                    item["company"] = txt
        # raw fields
        item["raw_fields"] = raw_fields
        identity = f"{item['portal_key']}|{item.get('job_id') or canonical_source_url}".encode()
        item["job_hash"] = hashlib.sha256(identity).hexdigest()
        yield item

    def parse_json_ld(self, soup: BeautifulSoup) -> dict | None:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or script.text or "")
            except Exception:
                continue
            items = data if isinstance(data, list) else [data]
            for obj in items:
                if isinstance(obj, dict) and obj.get("@type") == "JobPosting":
                    return obj
        return None

    @staticmethod
    def normalise_label(label: str) -> str:
        return label.strip().lower().replace(":", "")

    def _resolve_seed_file(self) -> Path:
        configured = self.settings.get("SEED_URLS_FILE") or "seed_urls.csv"
        path = Path(configured)
        if path.is_absolute():
            return path
        project_root = Path(__file__).resolve().parents[2]
        return project_root / configured

    @staticmethod
    def _is_excluded_url(url: str) -> bool:
        """Return True if the URL matches a non-production path pattern."""
        try:
            parsed = urlparse(url)
            # Check subdomain prefix
            host = (parsed.hostname or "").lower()
            subdomain = host.split(".")[0]
            if subdomain in AvatureSpider._EXCLUDED_PATH_SEGMENTS:
                return True
            # Check each path segment
            path_parts = {p.lower() for p in parsed.path.split("/") if p}
            if path_parts & AvatureSpider._EXCLUDED_PATH_SEGMENTS:
                return True
        except Exception:
            pass
        return False

    @staticmethod
    def _read_seed_urls(seed_path: Path) -> list[str]:
        urls: list[str] = []
        with seed_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            url_field = None
            for row in reader:
                if url_field is None:
                    url_field = next(
                        (k for k in row if k.strip().lower() in {"url", "seed_url"}),
                        None,
                    ) or next(iter(row))  # fallback: first column
                raw = row.get(url_field, "").strip().rstrip(",").strip()
                if not raw or raw.startswith("#"):
                    continue
                if not raw.startswith(("http://", "https://")):
                    continue
                if AvatureSpider._is_excluded_url(raw):
                    continue
                urls.append(raw)
        return urls

    @staticmethod
    def _sha256_file(path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _unwrap_job_href(href: str) -> str:
        parsed = urllib.parse.urlparse(href)
        if not parsed.query:
            return href
        qs = urllib.parse.parse_qs(parsed.query)
        share_param = qs.get("shareUrl") or qs.get("url")
        if share_param:
            return unquote(share_param[0])
        return href

    @staticmethod
    def _find_next_page(soup: BeautifulSoup, current_url: str) -> str | None:
        # Primary: explicit next-page link
        try:
            next_a = soup.select_one("a.paginationNextLink")
            if next_a and next_a.get("href"):
                return urljoin(current_url, str(next_a["href"]))
        except Exception:
            pass

        # Fallback: find the jobOffset= link with a strictly greater offset than current
        try:
            current_qs = parse_qs(urlparse(current_url).query)
            current_offset = int((current_qs.get("jobOffset") or ["0"])[0])
        except Exception:
            current_offset = 0

        best_href: str | None = None
        best_offset: int = current_offset  # must be strictly greater to qualify

        for a in soup.find_all("a", href=True):
            try:
                href = str(a["href"])
                if "jobOffset=" not in href:
                    continue
                resolved = urljoin(current_url, href)
                candidate_qs = parse_qs(urlparse(resolved).query)
                offset = int((candidate_qs.get("jobOffset") or ["0"])[0])
                if offset > best_offset:
                    best_href = href
                    best_offset = offset
            except Exception:
                continue

        return urljoin(current_url, best_href) if best_href else None

    @staticmethod
    def _canonicalize_detail_url(url: str) -> str:
        parsed = urlparse(url)
        clean_path = re.sub(r"/+", "/", parsed.path)
        clean = parsed._replace(query="", fragment="", path=clean_path)
        return str(urlunparse(clean))

    @staticmethod
    def _portal_key_from_url(url: str) -> str:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        parts = [p for p in parsed.path.split("/") if p]
        if "careers" in parts:
            idx = parts.index("careers")
            portal_path = "/".join(parts[: idx + 1])
        else:
            portal_path = "/".join(parts[:2]) if len(parts) >= 2 else "/".join(parts[:1])
        return f"{host}/{portal_path}".rstrip("/")

    @staticmethod
    def _iso_now() -> str:
        from datetime import UTC, datetime

        return datetime.now(UTC).isoformat()
