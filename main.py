from __future__ import print_function

import redis
import os
# from multiprocessing import Process
# from threading import Timer
import signal
from bluelens_spawning_pool import spawning_pool
from stylelens_crawl.stylens_crawl import StylensCrawler
from bluelens_log import Logging
from stylelens_product.products import Products
from stylelens_product.hosts import Hosts

# HEALTH_CHECK_TIME = 60 * 60 * 24

REDIS_HOST_CLASSIFY_QUEUE = 'bl:host:classify:queue'
REDIS_HOST_CRAWL_QUEUE = 'bl:host:crawl:queue'
REDIS__QUEUE = 'bl:host:classify:queue'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'

HOST_STATUS_TODO = 'todo'
HOST_STATUS_DOING = 'doing'
HOST_STATUS_DONE = 'done'

SPAWN_ID = os.environ['SPAWN_ID']
HOST_CODE = os.environ['HOST_CODE']
RELEASE_MODE = os.environ['RELEASE_MODE']
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-crawler')

product_api = None
host_api = None

# heart_bit = True

# def check_health():
#   global  heart_bit
#   log.info('check_health: ' + str(heart_bit))
#   if heart_bit == True:
#     heart_bit = False
#     Timer(HEALTH_CHECK_TIME, check_health, ()).start()
#   else:
#     delete_pod()

def delete_pod():
  log.info('delete_pod: ' + SPAWN_ID)

  data = {}
  data['namespace'] = RELEASE_MODE
  data['id'] = SPAWN_ID
  spawn = spawning_pool.SpawningPool()
  spawn.setServerUrl(REDIS_SERVER)
  spawn.setServerPassword(REDIS_PASSWORD)
  spawn.delete(data)

def get_latest_crawl_version():
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  if value is not None:
    version_id = value.decode("utf-8")
    return version_id
  return None

def save_status_on_host(host_code, status, version_id):
  global host_api

  host = {}
  host['host_code'] = host_code
  host['version_id'] = version_id
  host['crawl_status'] = status

  try:
    host_api.update_host(host)
  except Exception as e:
    log.error(str(e))

def crawl(host_code, version_id):
  global product_api
  options = {}
  log.setTag('bl-crawler-' + SPAWN_ID)
  log.debug('start crawl')
  options['host_code'] = host_code

  crawler = StylensCrawler(options)

  try:
    if crawler.start() == True:
      items = crawler.get_items()

      for item in items:
        product = {}
        product['name'] = item['name']
        product['host_url'] = item['host_url']
        product['host_code'] = item['host_code']
        product['host_name'] = item['host_name']
        product['product_no'] = item['product_no']
        product['main_image'] = item['main_image']
        product['sub_images'] = item['sub_images']

        try:
          res = product_api.update_product_by_hostcode_and_productno(product)
          product['version_id'] = version_id
          product['product_url'] = item['product_url']
          product['tags'] = item['tags']
          product['price'] = item['price']
          product['currency_unit'] = item['currency_unit']
          product['nation'] = item['nation']

          if 'cate' in item:
            product['cate'] = item['cate']

          if 'sale_price' in item:
            product['sale_price'] = item['sale_price']

          if 'related_product' in item:
            product['related_product'] = item['related_product']

          if 'thumbnail' in item:
            product['thumbnail'] = item['thumbnail']

          if 'upserted' in res:
            product_id = str(res['upserted'])
            log.debug("Created a product: " + product_id)
            product['is_processed'] = False
            update_product_by_id(product_id, product)
          elif res['nModified'] > 0:
            log.debug("Existing product is updated: product_no:" + product['product_no'])
            product['is_processed']= False
            update_product_by_hostcode_and_productno(product)
          else:
            log.debug("The product is same")
            update_product_by_hostcode_and_productno(product)
        except Exception as e:
          log.error("Exception when calling ProductApi->update_product_by_hostcode_and_productno: %s\n" % e)
          # delete_pod()

  except Exception as e:
    log.error("host_code:" + host_code + ' error: ' + str(e))
    delete_pod()

  notify_to_classify(host_code)
  save_status_on_host(host_code, HOST_STATUS_DONE)
  delete_pod()

def update_product_by_id(id, product):
  log.debug('update_product_by_id:' + product['product_no'])
  global product_api

  try:
    response = product_api.update_product_by_id(id, product)
    # log.debug(response)
  except Exception as e:
    log.error("Exception when calling ProductApi->update_product_by_id: %s\n" % e)
    # delete_pod()

def update_product_by_hostcode_and_productno(product):
  log.debug('update_product_by_hostcode_and_productno:' + product['product_no'])
  global product_api

  try:
    response = product_api.update_product_by_hostcode_and_productno(product)
    # log.debug(response)
  except Exception as e:
    log.error("Exception when calling update_product_by_hostcode_and_productno: %s\n" % e)
    # delete_pod()

def keep_the_job():
  rconn.lpush(REDIS_HOST_CRAWL_QUEUE, HOST_CODE)
  log.info('keep_the_job:' + HOST_CODE)

def notify_to_classify(host_code):
  log.info('notify_to_classify')
  rconn.lpush(REDIS_HOST_CLASSIFY_QUEUE, host_code)

if __name__ == '__main__':
  log.info('Start bl-crawler:new6')

  global product_api
  global host_api
  product_api = Products()
  host_api = Hosts()

  signal.signal(signal.SIGTERM, keep_the_job)

  version_id = get_latest_crawl_version()
  if version_id is not None:
    try:
      save_status_on_host(HOST_CODE, HOST_STATUS_DOING, version_id)
      crawl(HOST_CODE, version_id)
    except Exception as e:
      log.error(str(e))
      delete_pod()
  else:
    delete_pod()
