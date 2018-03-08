from __future__ import print_function

import redis
import os
import traceback
import pickle
import time
from bluelens_spawning_pool import spawning_pool
from stylelens_crawl.stylens_crawl import StylensCrawler
from stylelens_crawl_amazon import stylelens_crawl
from stylelens_crawl_amazon.item_search import ItemSearch
from stylelens_crawl_amazon.model.item_search_data import ItemSearchData
from bluelens_log import Logging
from stylelens_product.products import Products
from stylelens_product.hosts import Hosts
from stylelens_product.crawls import Crawls
from bluelens_k8s.pod import Pod

# HEALTH_CHECK_TIME = 60 * 60 * 24

REDIS_HOST_CLASSIFY_QUEUE = 'bl:host:classify:queue'
REDIS_HOST_CRAWL_QUEUE = 'bl:host:crawl:queue'
REDIS__QUEUE = 'bl:host:classify:queue'
REDIS_CRAWL_AMZ_QUEUE = "bl:crawl:amz:queue"
REDIS_TICKER_KEY = "bl:ticker:crawl:amazon"

STATUS_TODO = 'todo'
STATUS_DOING = 'doing'
STATUS_DONE = 'done'

SPAWN_ID = os.environ['SPAWN_ID']
HOST_CODE = os.environ['HOST_CODE']
HOST_GROUP = os.environ['HOST_GROUP']
VERSION_ID = os.environ['VERSION_ID']
RELEASE_MODE = os.environ['RELEASE_MODE']
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-crawler')

product_api = Products()
host_api = Hosts()
crawl_api = Crawls()

# heart_bit = True

# def check_health():
#   global  heart_bit
#   log.info('check_health: ' + str(heart_bit))
#   if heart_bit == True:
#     heart_bit = False
#     Timer(HEALTH_CHECK_TIME, check_health, ()).start()
#   else:
#     delete_pod()



class Crawler(Pod):
  def __init__(self):
    super().__init__(REDIS_SERVER, REDIS_PASSWORD, rconn, log)

def delete_pod():
  log.info('delete_pod: ' + SPAWN_ID)
  data = {}
  data['namespace'] = RELEASE_MODE
  data['key'] = 'SPAWN_ID'
  data['value'] = SPAWN_ID
  spawn = spawning_pool.SpawningPool()
  spawn.setServerUrl(REDIS_SERVER)
  spawn.setServerPassword(REDIS_PASSWORD)
  spawn.delete(data)

def save_status_on_crawl_job(host_code, status):
  global crawl_api

  crawl = {}
  crawl['status'] = status

  try:
    crawl_api.update_crawl_by_host_code(VERSION_ID, host_code, crawl)
  except Exception as e:
    log.error(str(e))

def wait_tick():
  rconn.blpop([REDIS_TICKER_KEY])

def crawl_amazon(host_code, host_group):
  log.setTag('bl-crawler-' + SPAWN_ID)
  log.debug('start crawl')
  global product_api
  crawler = stylelens_crawl.StylensCrawler()

  while True:
    wait_tick()
    key, value = rconn.blpop([REDIS_CRAWL_AMZ_QUEUE])
    search_data = pickle.loads(value)
    item_search = ItemSearch()
    item_search.search_data = ItemSearchData().from_dict(search_data)
    its = []
    its.append(item_search)
    items = crawler.get_items(its)

    products = get_products(items, host_code, host_group)

    if products != None:
      try:
        product_api.add_products(products)
      except Exception as e:
        log.error("Exception when calling Products.add_products() : " + str(e))
    # similar_items = crawler.get_similar_items()
    # get_products(similar_items, host_code, host_group)

def get_products(items, host_code, host_group):
  products = []

  for item in items:
    try:
      if item is None:
        return False
      product = item.to_dict()
      product['name'] = item.title
      product['host_url'] = 'https://www.amazon.com'
      product['host_code'] = host_code
      product['host_group'] = host_group
      product['host_name'] = 'amazon'
      product['product_no'] = item.asin
      price_dic = product.get('price')
      if price_dic != None:
        product['price']['amount'] = int(price_dic.get('amount'))
        product['price']['currency_code'] = price_dic.get('currency_code')
        product['price']['formatted_price'] = price_dic.get('formatted_price')
      lowest_price_dic = product.get('lowest_price')
      if lowest_price_dic != None:
        product['lowest_price']['amount'] = int(lowest_price_dic.get('amount'))
        product['lowest_price']['currency_code'] = lowest_price_dic.get('currency_code')
        product['lowest_price']['formatted_price'] = lowest_price_dic.get('formatted_price')
      highest_price_dic = product.get('highest_price')
      if highest_price_dic != None:
        product['highest_price']['amount'] = int(highest_price_dic.get('amount'))
        product['highest_price']['currency_code'] = highest_price_dic.get('currency_code')
        product['highest_price']['formatted_price'] = highest_price_dic.get('formatted_price')

      if price_dic == None and lowest_price_dic == None:
        continue
      product['main_image'] = item.l_image.url
      # product['sub_images'] = item['sub_images']
      product['sub_images'] = None
      product['version_id'] = VERSION_ID
      product['product_url'] = item.detail_page_link
      product['tags'] = item.features
      product['nation'] = 'us'
      product['is_processed'] = True
      products.append(product)
    except Exception as e:
      log.error("Exception when calling ProductApi->add_products: %s\n" % e)
      return None

  return products

def crawl(host_code, host_group):
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
        product['host_group'] = host_group
        product['host_name'] = item['host_name']
        product['product_no'] = item['product_no']
        product['main_image'] = item['main_image']
        # product['sub_images'] = item['sub_images']
        product['sub_images'] = None

        try:
          res = product_api.update_product_by_hostcode_and_productno(product)
          product['version_id'] = VERSION_ID
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

  # notify_to_classify(host_code)
  save_status_on_crawl_job(host_code, STATUS_DONE)
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
  log.info('Start bl-crawler:1')

  try:
    save_status_on_crawl_job(HOST_CODE, STATUS_DOING)
    if HOST_GROUP == 'HG8000':
      crawl_amazon(HOST_CODE, HOST_GROUP)
    else:
      crawl(HOST_CODE, HOST_GROUP)
  except Exception as e:
    log.error('global exception')
    log.error(e)
    log.error(str(e))
    traceback.print_exc(limit=None)
    delete_pod()
