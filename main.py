import subprocess

from util.bigquery import *

if __name__ == '__main__':
    service_name = os.environ.get('service', 'imvely')

    if os.path.exists('out.json'):
        os.remove('out.json')

    print(service_name)
    if service_name == '8seconds':
        subprocess.run("scrapy runspider service/8seconds.py -o out.json -t json", stderr=subprocess.PIPE, shell=True)
    elif service_name == 'naning9':
        subprocess.run("scrapy runspider service/naning9.py -o out.json -t json", stderr=subprocess.PIPE, shell=True)
    elif service_name == 'imvely':
        subprocess.run("scrapy runspider service/imvely.py -o out.json -t json", stderr=subprocess.PIPE, shell=True)
    elif service_name == 'stylenanda':
        subprocess.run("scrapy runspider service/stylenanda.py -o out.json -t json", stderr=subprocess.PIPE, shell=True)

    if os.path.exists('out.json'):
        bigquery = BigQuery(service_name)
        bigquery.upload_data_from_json()
