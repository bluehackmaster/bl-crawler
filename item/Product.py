import scrapy


class Product(scrapy.Item):
    title = scrapy.Field()
    price = scrapy.Field()
    url = scrapy.Field()
    img = scrapy.Field()
    last_updated = scrapy.Field(serializer=str)
