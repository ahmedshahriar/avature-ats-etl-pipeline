import random

from scrapy import signals


class RandomUserAgentMiddleware:
    def __init__(self, ua_pool: list[str]):
        self.ua_pool = ua_pool

    @classmethod
    def from_crawler(cls, crawler):
        pool = crawler.settings.getlist("UA_POOL")
        if not pool:
            raise ValueError("UA_POOL must be set in settings")
        obj = cls(pool)
        obj._spider = None
        crawler.signals.connect(obj._on_spider_opened, signal=signals.spider_opened)
        return obj

    def _on_spider_opened(self, spider):
        self._spider = spider

    def process_request(self, request):  # no spider arg
        request.headers["User-Agent"] = random.choice(self.ua_pool)
