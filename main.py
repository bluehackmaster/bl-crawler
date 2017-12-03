from __future__ import print_function

import redis
import os
from multiprocessing import Process
from threading import Timer
from bluelens_spawning_pool import spawning_pool
from stylelens_product import Product
from stylelens_product import ProductApi
from stylelens_product.models.add_product_response import AddProductResponse
from stylelens_product.rest import ApiException
from stylelens_crawl import Crawler
from bluelens_log import Logging

HEALTH_CHECK_TIME = 60 * 60 * 24

REDIS_HOST_CLASSIFY_QUEUE = 'bl:host:classify:queue'
REDIS_HOST_CRAWL_QUEUE = 'bl:host:crawl:queue'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'


SPAWN_ID = os.environ['SPAWN_ID']
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-crawler')

heart_bit = True

def check_health():
  global  heart_bit
  log.info('check_health: ' + str(heart_bit))
  if heart_bit == True:
    heart_bit = False
    Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  else:
    exit()

def exit():
  log.info('exit: ' + SPAWN_ID)

  data = {}
  data['namespace'] = 'index'
  data['id'] = SPAWN_ID
  spawn = spawning_pool.SpawningPool()
  spawn.setServerUrl(REDIS_SERVER)
  spawn.setServerPassword(REDIS_PASSWORD)
  spawn.delete(data)

def get_latest_crawl_version():
  key, value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  version_id = value.decode("utf-8")
  return version_id

def crawl(host_code, version_id):
  options = {}
  options['host_code'] = host_code

  crawler = Crawler(options)

  try:
    items = crawler.run()
  except Exception as e:
    log.error(str(e))
    exit()

  product_api = ProductApi()
  for item in items:
    product = Product()
    product.name = item['name']
    product.host_url = item['host_url']
    product.host_code = item['host_code']
    product.host_name = item['host_name']
    product.tags = item['tags']
    product.currency_unit = item['currency_unit']
    product.product_url = item['product_url']
    product.product_no = item['product_no']
    product.nation = item['nation']
    product.main_image = item['main_image']
    product.sub_images = item['sub_images']

    try:
      res = product_api.update_product_by_hostcode_and_productno(product,
                                                                 host_code,
                                                                 product.product_no)
      product.version_id = version_id

      if res.data.product_id != None:
        log.debug("Created a product")
        product.id = res.data.product_id
        product.is_indexed = False
        update_product_by_id(product)
      elif res.data.modified_count > 0:
        log.debug("Existing product is updated")
        product.is_indexed = False
        update_product_by_hostcode_and_productno(product)
      else:
        log.debug("The product is same")
        product.is_indexed = True
        update_product_by_hostcode_and_productno(product)
    except ApiException as e:
      log.error("Exception when calling ProductApi->update_product_by_hostcode_and_productno: %s\n" % e)
      exit()

  notify_to_classify(host_code)

def update_product_by_id(product):
  log.debug('update_product_by_id:' + product.product_no)
  product_api = ProductApi()

  try:
    response = product_api.update_product_by_id(product.id, product)
    log.debug(response)
  except ApiException as e:
    log.error("Exception when calling ProductApi->update_product_by_id: %s\n" % e)
    exit()

def update_product_by_hostcode_and_productno(product):
  log.debug('update_product_by_hostcode_and_productno:' + product.product_no)
  product_api = ProductApi()

  try:
    response = product_api.update_product_by_hostcode_and_productno(product.host_code, product.product_no, product)
    log.debug(response)
  except ApiException as e:
    log.error("Exception when calling ProductApi->update_product_by_hostcode_and_productno: %s\n" % e)
    exit()

def notify_to_classify(host_code):
  rconn.lpush(REDIS_HOST_CLASSIFY_QUEUE, host_code)


def dispatch_job(rconn, version_id):
  log.info('Start dispatch_job')
  Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  while True:
    key, value = rconn.blpop([REDIS_HOST_CRAWL_QUEUE])
    crawl(value.decode('utf-8'), version_id)
    global  heart_bit
    heart_bit = True


if __name__ == '__main__':
  log.info('Start bl-crawler')
  version_id = get_latest_crawl_version()
  try:
    Process(target=dispatch_job, args=(rconn,version_id,)).start()
  except Exception as e:
    log.error(str(e))
    exit()
