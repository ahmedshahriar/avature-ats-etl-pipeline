import scrapy


class CoreItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class AvatureJobItem(scrapy.Item):
    """
    Defines the structure of a single job posting scraped from an
    Avature‑hosted careers portal.  Fields mirror those in the standalone
    scraper and are intentionally liberal (many are optional) to allow
    for gradual normalisation.
    """
    job_hash = scrapy.Field()
    source_url = scrapy.Field()  # canonical URL of the job detail page

    job_id = scrapy.Field()  # Avature job ID (often appears in the URL)
    title = scrapy.Field()  # Position title
    company = scrapy.Field()  # Hiring organization or company name
    locations = scrapy.Field()  # List of location strings
    posted_date = scrapy.Field()  # Date the job was posted (raw string)
    remote = scrapy.Field()  # Remote flag (Yes/No/Unknown)
    employment_type = scrapy.Field()  # Full time / Part time / Contract / etc.
    career_area = scrapy.Field()  # High level career category
    ref_number = scrapy.Field()  # Internal reference number (Ref # / Req #)
    description_text = scrapy.Field()  # Plain text description
    apply_url = scrapy.Field()  # URL to start the application process
    raw_fields = scrapy.Field()  # All other raw label/value pairs for audit
