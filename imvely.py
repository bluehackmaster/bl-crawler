from urllib.parse import urlparse

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

from util import make_url

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',

}

inserted_image = []


class ImvelyMallsSpider(scrapy.Spider):
    name = 'Imvely'
    domain = ''

    def start_requests(self):
        yield scrapy.Request(url='http://www.imvely.com/', callback=self.main_parse, errback=self.errback_httpbin,
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
        urls = {}
        parsed_uri = urlparse(response.url)
        self.domain = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
        for item in response.css('div.fixed_menu li a'):
            for sub_item in item.css('a::attr(href)').extract():
                urls[item.css('img::attr(alt)').extract_first()] = self.domain + sub_item

        for url in urls.keys():
            yield scrapy.Request(url=urls[url], callback=self.sub_parse, errback=self.errback_httpbin, headers=header,
                                 meta={'cate': url})

    def sub_parse(self, response):
        for url in response.css('ul.prdList li p.name a::attr(href)').extract():
            yield scrapy.Request(url=self.domain + url, callback=self.detail_parse, errback=self.errback_httpbin,
                                 headers=header, meta={'cate': response.meta['cate']})

    def detail_parse(self, response):
        cate = response.meta['cate']

        img = response.css('div.cont img::attr(src)').extract()
        img.append(response.css('div.detailArea div.keyImg img::attr("src")').extract_first())

        price = response.css('meta[property="product:price:amount"]::attr(content)').extract_first()
        price_unit = response.css('meta[property="product:price:currency"]::attr(content)').extract_first()

        url = response.url
        product_no = url[42 + len('product_no='):].split('&')[0]
        image_url = response.css('meta[property="og:image"]::attr(content)').extract()[1]
        if image_url not in inserted_image:
            yield {
                'host_url': self.domain,
                'tag': cate,
                'product_name': response.css('meta[property="og:title"]::attr(content)').extract()[1],
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
                    'product_name': response.css('meta[property="og:title"]::attr(content)').extract()[1],
                    'image_url': make_url(self.domain, im),
                    'product_price': price,
                    'currency_unit': price_unit,
                    'product_url': url,
                    'product_no': product_no,
                    'main': '0',
                    'nation': 'ko'
                }
                inserted_image.append(im)
