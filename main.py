import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',

}


class QuotesSpider(scrapy.Spider):
    name = "quotes"

    def start_requests(self):
        urls = [
            'http://www.stylenanda.com/product/list.html?cate_no=183',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, errback=self.errback_httpbin, headers=header)

    def errback_httpbin(self, failure):
        self.logger.error(repr(failure))

        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)

    def parse(self, response):
        for item in response.css('ul.column4 li'):
            yield {
                'title': item.css('div.name a > span::text').extract_first(),
                'img': item.css('a img::attr("src")').extract_first(),
            }

        next_page = response.css('div.xans-product-normalpaging p > a::attr(href)').extract()
        if next_page[1] != '#none':
            next_page = response.urljoin(next_page[1])
            yield scrapy.Request(url=next_page, callback=self.parse, errback=self.errback_httpbin, headers=header)
