import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

from util import *

header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',

}

inserted_image = []


class SecondsMallsSpider(scrapy.Spider):
    name = '8seconds'
    domain = ''

    def start_requests(self):
        urls = [
            'http://www.ssfshop.com/8Seconds/SFMA42/list?dspCtgryNo=SFMA42&filterCtgryNo=&secondFilterCtgryNo=&brandShopNo=BDMA07A01&brndShopId=8SBSS&sortColumn=NEW_GOD_SEQ&etcCtgryNo=&leftBrandNM=&currentPage=',
            'http://www.ssfshop.com/8Seconds/SFMA41/list?dspCtgryNo=SFMA41&filterCtgryNo=&secondFilterCtgryNo=&brandShopNo=BDMA07A01&brndShopId=8SBSS&sortColumn=NEW_GOD_SEQ&etcCtgryNo=&leftBrandNM=&currentPage=']

        for url in urls:
            yield scrapy.Request(url=url + "1", callback=self.url_parse, errback=self.errback_httpbin, headers=header,
                                 meta={'original': url})

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

    def url_parse(self, response):
        page_urls = []
        original_url = response.meta['original']

        max_page = response.css('a.last::attr(id)').extract_first()
        for page in range(1, int(max_page) + 1):
            page_urls.append(original_url + str(page))

        for url in page_urls:
            yield scrapy.Request(url=url, callback=self.sub_parse, errback=self.errback_httpbin, headers=header)

    def sub_parse(self, response):
        parsed_uri = urlparse(response.url)
        self.domain = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)

        for url in response.css('ul#dspGood a::attr(href)').extract():
            yield scrapy.Request(url=self.domain + url, callback=self.detail_parse, errback=self.errback_httpbin,
                                 headers=header)

    def detail_parse(self, response):
        cate = response.css('span.bracket a::text').extract()

        img = response.css('div.prd-img img::attr(src)').extract()
        img = img + response.css('div.zoomImg img::attr(src)').extract()

        price = response.css('meta[property="product:sale_price:amount"]::attr(content)').extract_first()
        price_unit = response.css('meta[property="product:sale_price:currency"]::attr(content)').extract_first()

        url = response.url
        product_no = response.css('meta[property="rb:itemId"]::attr(content)').extract_first()
        image_url = response.css('meta[property="og:image"]::attr(content)').extract_first()
        if image_url not in inserted_image:
            yield {
                'host_url': self.domain,
                'tag': cate,
                'product_name': response.css('meta[property="og:title"]::attr(content)').extract(),
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
                    'product_name': response.css('meta[property="og:title"]::attr(content)').extract(),
                    'image_url': make_url(self.domain, im),
                    'product_price': price,
                    'currency_unit': price_unit,
                    'product_url': url,
                    'product_no': product_no,
                    'main': '0',
                    'nation': 'ko'
                }
                inserted_image.append(im)
