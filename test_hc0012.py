from stylelens_crawl.stylens_crawl import StylensCrawler

options = {
    'host_code': 'HC0012'
}

crawler = StylensCrawler(options)

try:
  ret = crawler.start()
  if ret == True:
    items = crawler.get_items()
    print(items)
except Exception as e:
  print(e)
  exit()
