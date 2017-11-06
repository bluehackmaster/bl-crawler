import scrapy


class Detail(scrapy.Item):
    img = scrapy.Field()
    last_updated = scrapy.Field(serializer=str)
