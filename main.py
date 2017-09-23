from urllib.parse import urlparse

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',

}


class MallsSpider(scrapy.Spider):
    name = "Mall"

    def start_requests(self):
        urls = [
            'http://www.stylenanda.com/product/list.html?cate_no=82',
            'http://www.stylenanda.com/product/list.html?cate_no=1789',
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
        parsed_uri = urlparse(response.url)
        domain = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)

        for item in response.css('ul.column4 li'):
            item_url = domain + item.css('li::attr("onclick")').re("javascript: location.href='(.*?)'")[0]
            yield scrapy.Request(url=item_url, callback=self.detail_parse, errback=self.errback_httpbin, headers=header)

        next_page = response.css('div.xans-product-normalpaging p > a::attr(href)').extract()
        if next_page[1] != '#none':
            next_page = response.urljoin(next_page[1])
            yield scrapy.Request(url=next_page, callback=self.parse, errback=self.errback_httpbin, headers=header)

    def detail_parse(self, response):
        yield {
            'title': response.css('div.infoArea h2 > span.name::text').extract_first(),
            'price': response.css('div.infoArea strong#span_product_price_text::text').extract_first(),
            'url': response.url,
            'img': response.css('div.detailArea div.keyImg img::attr("src")').extract_first(),
            'detail_img': response.css('div.cont img::attr(src)').extract(),
            'category': response.css('div.detail_arr li a::text').extract()
        }
