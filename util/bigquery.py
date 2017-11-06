import json
import os

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from oauth2client.service_account import ServiceAccountCredentials

CLIENT_SECRET = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             'config/application_default_credentials.json')
scope = ['https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/bigquery.insertdata']

credentials = ServiceAccountCredentials.from_json_keyfile_name(CLIENT_SECRET, scopes=scope)

client = bigquery.Client(project='bluelens-11b9b')
dataset = client.dataset('stylelens')

SCHEMA = [
    SchemaField('host_url', 'STRING', mode='required'),
    SchemaField('tag', 'STRING', mode='required'),
    SchemaField('sub_category', 'STRING', mode='NULLABLE'),
    SchemaField('product_name', 'STRING', mode='required'),
    SchemaField('image_url', 'STRING', mode='required'),
    SchemaField('product_price', 'INTEGER', mode='required'),
    SchemaField('currency_unit', 'STRING', mode='required'),
    SchemaField('product_url', 'STRING', mode='required'),
    SchemaField('product_no', 'STRING', mode='required'),
    SchemaField('main', 'INTEGER', mode='required'),
    SchemaField('nation', 'STRING', mode='required'),
]


class BigQuery(object):
    def __init__(self, name):
        self.name = name

        self.table = dataset.table(name=self.name, schema=SCHEMA)
        if self.table.exists() is False:
            self.table.create()
        else:
            self.table.reload()

    def upload_data_from_json(self):
        print('Get json data from out.json file')

        with open('../out.json', mode='rb') as data_file:
            data = json.load(data_file)
            # table.upload_from_file(
            #     data_file, source_format='JSON', skip_leading_rows=1)

        steps = int(round(len(data) / 10000, 0))
        start_idx = end_idx = 0
        inserted_image = []

        print(len(data))
        print(steps)

        for step in range(0, steps + 1):
            if step is 0:
                end_idx = 10000

            request_data = []
            for key in data[start_idx:end_idx]:
                row_data = (key['host_url'],
                            ','.join(key['tag']),
                            '',
                            key['product_name'],
                            key['image_url'],
                            int(key['product_price']),
                            key['currency_unit'],
                            key['product_url'],
                            key['product_no'],
                            int(key['main']),
                            key['nation'])
                request_data.append(row_data)
            print(str(step) + " Step")
            print('request_data::' + str(len(request_data)))
            print('inserted_image::' + str(len(inserted_image)))
            if len(request_data) > 0:
                print(self.table.insert_data(request_data))
            start_idx += len(request_data)
            end_idx += len(request_data)

            request_data.append(row_data)

        print(self.table.insert_data(request_data))
