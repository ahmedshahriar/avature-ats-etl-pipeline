"""Scrapy spider for crawling Avature career portals.

This spider reads listing page URLs from the ``input_urls.csv`` file
located in the project root.  For each portal it crawls all listing
pages (following pagination), collects unique job detail URLs and
parses the job details into structured items defined in
``avature_scraper.items.JobItem``.

Although the standalone script ``scraper.py`` demonstrates how to scrape
Avature portals without Scrapy, this spider makes the same logic
available in a familiar Scrapy project structure.  It relies on
BeautifulSoup for parsing (bs4 and lxml are required).  The spider can
be executed with ``scrapy crawl avature`` from the project root.
"""

import csv
import hashlib
import json
import os
import re
import urllib.parse
from urllib.parse import urlparse

import scrapy
from bs4 import BeautifulSoup

from ..items import AvatureJobItem


class AvatureSpider(scrapy.Spider):
    name = "avature"
    allowed_domains = []  # dynamically set based on input

    async def start(self):
        # Read listing URLs from the input CSV.  The file is assumed to
        # reside in the project root (two levels above this file).
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        input_file = os.path.join(project_root, "input_urls.csv")
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"input_urls.csv not found at {input_file}")
        with open(input_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                url = row[0].strip()
                if not url or url.startswith("#"):
                    continue
                # Record domain for allowed_domains
                domain = urllib.parse.urlparse(url).hostname
                if domain and domain not in self.allowed_domains:
                    self.allowed_domains.append(domain)
                yield scrapy.Request(url, callback=self.parse_listing, meta={"portal_root": url})

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
        "posted date": "posted_date",
        "posted": "posted_date",
        "career area (you may select more than one)": "career_area",
        "state": "locations",
        "city": "locations",
    }

    def normalise_label(self, label: str) -> str:
        return label.strip().lower().replace(":", "")

    def parse_listing(self, response):
        """Parse a listing page and schedule requests for each job link and the next page."""
        soup = BeautifulSoup(response.text, "lxml")
        current_url = response.url
        # Extract job detail links
        job_links = []
        for a in soup.select('article.article--result a[href*="/JobDetail/"]'):
            href = a.get("href")
            if not href:
                continue
            if "shareUrl=" in href or "url=" in href:
                parsed = urllib.parse.urlparse(str(href))
                qs = urllib.parse.parse_qs(parsed.query)
                share_param = qs.get("shareUrl") or qs.get("url")
                if share_param:
                    href = urllib.parse.unquote(share_param[0])
            job_url = urllib.parse.urljoin(current_url, str(href))
            job_links.append(job_url)
        # Deduplicate
        seen = set()
        unique_links = []
        for link in job_links:
            if link not in seen:
                unique_links.append(link)
                seen.add(link)
        for link in unique_links:
            yield scrapy.Request(link, callback=self.parse_job, meta=response.meta)
        # Find next page
        next_link = None
        next_a = soup.select_one("a.paginationNextLink")
        if next_a and next_a.get("href"):
            next_link = urllib.parse.urljoin(current_url, str(next_a["href"]))
        else:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "jobOffset=" in href:
                    next_link = urllib.parse.urljoin(current_url, str(href))
                    break
        if next_link:
            yield scrapy.Request(next_link, callback=self.parse_listing, meta=response.meta)

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

    def parse_job(self, response):
        """Parse a job detail page into a JobItem."""
        soup = BeautifulSoup(response.text, "lxml")
        item = AvatureJobItem()
        item["source_url"] = response.url

        # job_id from meta
        meta = soup.find("meta", attrs={"name": "avature.portallist.search"})
        if meta and meta.get("content"):
            item["job_id"] = str(meta["content"]).strip()
        else:
            m = re.search(r"/([0-9]{3,})(?:[/?#]|$)", response.url)
            if m:
                item["job_id"] = m.group(1)

        parsed = urlparse(item["source_url"])
        host = parsed.hostname or ""

        parts = [p for p in parsed.path.split("/") if p]
        # keep portal stable up to ".../careers" (works for both /careers and /en_US/careers)
        if "careers" in parts:
            i = parts.index("careers")
            portal = "/".join(parts[: i + 1])  # "careers" or "en_US/careers"
        else:
            portal = "/".join(parts[:2])  # fallback

        job_id = item.get("job_id") or ""
        if job_id:
            identity = f"{host}|{portal}|{job_id}|{item['source_url']}".encode()
        else:
            identity = f"{host}|{portal}|{item['source_url']}".encode()

        item["job_hash"] = hashlib.sha256(identity).hexdigest()

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
        # JSON‑LD
        jobposting = self.parse_json_ld(soup)
        ld_description = None
        if jobposting:
            if not item.get("company"):
                org = jobposting.get("hiringOrganization") or {}
                if isinstance(org, dict):
                    name = org.get("name")
                    if name:
                        item["company"] = name
            if not item.get("posted_date"):
                posted = jobposting.get("datePosted")
                if posted:
                    item["posted_date"] = posted
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
                        parts = [p.strip() for p in re.split(r",|/|\n", value_text) if p.strip()]
                        locations.extend(parts)
                    else:
                        item[attr] = value_text
            else:
                # description fallback
                current_desc = item.get("description_text") or ""
                item["description_text"] = (current_desc + "\n" + value_text).strip()
        if locations:
            # dedupe
            seen = set()
            deduped = []
            for loc in locations:
                if loc and loc not in seen and loc.lower() != "unknown":
                    deduped.append(loc)
                    seen.add(loc)
            item["locations"] = deduped
        # narrative sections
        sections = []
        for article in soup.select("article.article--details"):
            # Only include narrative sections that have a heading; the first
            # ``article--details`` often contains general info without a
            # header and would otherwise pollute the description.
            heading = article.select_one(".article__header__text__title")
            if not heading:
                continue
            body_fields = article.select(".article__content__view__field .article__content__view__field__value")
            if body_fields:
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
        # apply URL
        for a in soup.find_all("a", href=True):
            href = str(a["href"])
            text = a.get_text(strip=True).lower()
            if "applicationmethods?jobid=" in href.lower():
                item["apply_url"] = urllib.parse.urljoin(response.url, href)
                break
            if "login?jobid=" in href.lower() and ("apply" in text or "apply now" in text):
                item["apply_url"] = urllib.parse.urljoin(response.url, href)
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
        yield item
