import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

from util import *

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',

}

inserted_image = []


class ImvelyMallsSpider(scrapy.Spider):
    name = 'Naning9'
    domain = ''

    def start_requests(self):
        self.domain = 'http://www.naning9.com'
        yield scrapy.Request(url='http://www.naning9.com/', callback=self.main_parse, errback=self.errback_httpbin,
                             headers=header)

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

    def main_parse(self, response):
        urls = []
        for url in response.css('div.alc_content_inn li a::attr(href)').extract():
            if url.startswith('/shop/'):
                urls.append(make_url(self.domain, url))
        urls.reverse()
        for url in urls:
            yield scrapy.Request(url=url, callback=self.sub_parse, errback=self.errback_httpbin, headers=header)

    def sub_parse(self, response):

        for url in response.css('div.goods_conts div.thumb a::attr(href)').extract():
            yield scrapy.Request(url=self.domain + url + '&lang=kr', callback=self.detail_parse,
                                 errback=self.errback_httpbin, headers=header)

    def detail_parse(self, response):
        cate = response.css('meta[property="product:category"]::attr(content)').extract()
        title = response.css('meta[property="og:title"]::attr(content)').extract_first()
        img = response.css('div.det_cust_wrap img::attr(src)').extract()

        price = response.css('meta[property="product:sale_price:amount"]::attr(content)').extract_first()
        price_unit = response.css('meta[property="product:sale_price:currency"]::attr(content)').extract_first()

        url = response.url
        product_no = response.css('input[id="index_no"]::attr(value)').extract_first()
        image_url = response.css('meta[property="og:image"]::attr(content)').extract_first()
        if image_url not in inserted_image:
            yield {
                'host_url': self.domain,
                'tag': cate,
                'product_name': title,
                'image_url': make_url(self.domain, image_url),
                'product_price': price,
                'currency_unit': price_unit,
                'product_url': url,
                'product_no': product_no,
                'main': '1',
                'nation': 'ko'
            }
            inserted_image.append(image_url)

        for im in img:
            if im not in inserted_image:
                yield {
                    'host_url': self.domain,
                    'tag': cate,
                    'product_name': title,
                    'image_url': make_url(self.domain, im),
                    'product_price': price,
                    'currency_unit': price_unit,
                    'product_url': url,
                    'product_no': product_no,
                    'main': '0',
                    'nation': 'ko'
                }
                inserted_image.append(im)
