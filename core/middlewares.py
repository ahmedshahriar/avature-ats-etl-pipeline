import random
import re

UA_TO_CLIENT_HINTS: dict[str, tuple[str, str]] = {
    "145": (
        '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        '"macOS"',
    ),
    "144": (
        '"Not:A-Brand";v="99", "Google Chrome";v="144", "Chromium";v="144"',
        '"Linux"',
    ),
    "145_edge": (
        '"Not A;Brand";v="99", "Chromium";v="145", "Microsoft Edge";v="145"',
        '"Windows"',
    ),
    "144_edge": (
        '"Not A;Brand";v="99", "Chromium";v="144", "Microsoft Edge";v="144"',
        '"Windows"',
    ),
}

# UAs that must NOT receive Sec-CH-UA headers (non-Chromium engines)
_NO_CLIENT_HINTS_PATTERN = re.compile(r"(Safari/|Firefox/|Gecko/)", re.IGNORECASE)


class RandomUserAgentMiddleware:
    def __init__(self, ua_pool: list[str]):
        self.ua_pool = ua_pool

    @classmethod
    def from_crawler(cls, crawler):
        pool = crawler.settings.getlist("UA_POOL")
        if not pool:
            raise ValueError("UA_POOL must be set in settings")
        return cls(pool)

    def process_request(self, request, **kwargs):  # **kwargs: forward-compat with Scrapy 2.14+
        ua = random.choice(self.ua_pool)
        request.headers["User-Agent"] = ua

        # Non-Chromium UAs must not send Client Hints — it's a fingerprint contradiction
        if _NO_CLIENT_HINTS_PATTERN.search(ua) and "Chrome" not in ua:
            request.headers.pop("Sec-CH-UA", None)
            request.headers.pop("Sec-CH-UA-Mobile", None)
            request.headers.pop("Sec-CH-UA-Platform", None)
            return

        match = re.search(r"Chrome/(\d+)", ua)
        version = match.group(1) if match else "145"
        is_edge = "Edg/" in ua
        key = f"{version}_edge" if is_edge else version

        hints = UA_TO_CLIENT_HINTS.get(key, UA_TO_CLIENT_HINTS["145"])
        request.headers["Sec-CH-UA"] = hints[0]
        request.headers["Sec-CH-UA-Platform"] = hints[1]
        request.headers["Sec-CH-UA-Mobile"] = "?0"
