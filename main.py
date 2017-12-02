from __future__ import print_function

import redis
import os
from multiprocessing import Process
from threading import Timer
from bluelens_spawning_pool import spawning_pool
from stylelens_product import Product
from stylelens_product import ProductApi
from stylelens_product.rest import ApiException
from stylelens_crawl import Crawler
from bluelens_log import Logging

HEALTH_CHECK_TIME = 60

REDIS_HOST_CLASSIFY_QUEUE = 'bl:host:classify:queue'
REDIS_HOST_CRAWL_QUEUE = 'bl:host:crawl:queue'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'


SPAWN_ID = os.environ['SPAWN_ID']
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
PRODUCT_VERSION = os.environ['PRODUCT_VERSION']
HOST_CODE = os.environ['HOST_CODE']

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

def crawl(host_code):
  options = {}
  options['host_code'] = HOST_CODE

  crawler = Crawler(options)

  try:
    items = crawler.run()
  except Exception as e:
    log.error(str(e))

  product_api = ProductApi()
  for item in items:
    product = Product()
    product.host_url = item['host_url']
    product.host_code = item['host_code']
    product.version = PRODUCT_VERSION

    try:
      res = product_api.update_product_by_hostcode_and_productno(product,
                                                                 host_code,
                                                                 product.product_no)
      if res.data.product_id != None:
        log.debug("Created a product")
        # Todo: Need to classify
      elif res.data.modified_count > 0:
        log.debug("Existing product is updated")
        # Todo: Need to classify
      else:
        log.debug("Existing product is same thing")
    except ApiException as e:
      log.error("Exception when calling ProductApi->add_product: %s\n" % e)

def push_to_queue(host_code):
  rconn.lpush(REDIS_HOST_CLASSIFY_QUEUE, host_code)


def dispatch_job(rconn):
  log.info('Start dispatch_job')
  Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  while True:
    key, value = rconn.blpop([REDIS_HOST_CRAWL_QUEUE])
    crawl(value.decode('utf-8'))
    global  heart_bit
    heart_bit = True


if __name__ == '__main__':
  log.info('Start bl-crawler')
  try:
    Process(target=dispatch_job, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
    exit()
