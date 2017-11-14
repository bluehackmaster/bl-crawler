from __future__ import print_function

import os
import redis
from stylelens_product import Product
from stylelens_product import ProductApi
from stylelens_product.rest import ApiException
from stylelens_crawl import Crawler

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
PRODUCT_VERSION = os.environ['PRODUCT_VERSION']
HOST_CODE = os.environ['HOST_CODE']

rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

if __name__ == '__main__':

  options = {}
  options['host_code'] = HOST_CODE


  rconn.lpush(HOST_CODE, 'started')

  crawler = Crawler(options)

  items = crawler.run()

  api_instance = ProductApi()
  product = Product()
  for item in items:
    product.host_url = item['host_url']
    product.host_code = item['host_code']
    product.version = PRODUCT_VERSION
    # ...

    try:
      # Added a new Product
      api_response = api_instance.add_product(product)
    except ApiException as e:
      print("Exception when calling ProductApi->add_product: %s\n" % e)

  rconn.lpop(HOST_CODE)

