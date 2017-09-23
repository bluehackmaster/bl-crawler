import json

from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

CLIENT_SECRET = 'config/whatsit-47b0dd63f334.json'
SPREAD_KEY = '1s95VTISsx2m6pr3TphBJS6goI2sntQE59BKS1U-tES4'

scope = ['https://www.googleapis.com/auth/spreadsheets']
credentials = ServiceAccountCredentials.from_json_keyfile_name(CLIENT_SECRET, scope)

service = discovery.build('sheets', 'v4', credentials=credentials)

with open('out.json') as data_file:
    data = json.load(data_file)

values = []

for idx, key in enumerate(data):
    row_data = [str(idx), key['url'], ",".join(key['category']), '', key['title'], key['img'], key['price'],
                ",".join(key['detail_img'])]
    values.append(row_data)

body = {
    'values': values
}

result = service.spreadsheets().values().update(
    spreadsheetId=SPREAD_KEY, range="'stylenanda'!A204", body=body,
    valueInputOption='RAW').execute()


def join_text(text):
    text.join
