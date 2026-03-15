import scrapy


class AvatureJobItem(scrapy.Item):
    """
    Defines the structured data fields for a job posting scraped from an Avature-powered careers portal.
    """

    # Stable identity
    job_hash = scrapy.Field()
    source_url = scrapy.Field()  # canonical URL used downstream
    raw_source_url = scrapy.Field()  # original response URL
    canonical_source_url = scrapy.Field()
    portal_key = scrapy.Field()  # normalized host/portal identity

    # Lineage
    run_id = scrapy.Field()
    run_date = scrapy.Field()
    scraped_at = scrapy.Field()
    input_seed_url = scrapy.Field()

    # Core business fields
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

    # Data quality metadata
    record_status = scrapy.Field()  # valid | quarantined
    validation_errors = scrapy.Field()
    validation_warnings = scrapy.Field()
