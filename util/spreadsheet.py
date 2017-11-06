import json

from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

CLIENT_SECRET = 'config/whatsit-47b0dd63f334.json'
SPREAD_KEY = '1s95VTISsx2m6pr3TphBJS6goI2sntQE59BKS1U-tES4'

scope = ['https://www.googleapis.com/auth/spreadsheets']
credentials = ServiceAccountCredentials.from_json_keyfile_name(CLIENT_SECRET, scope)

service = discovery.build('sheets', 'v4', credentials=credentials)
start = 2

print('Get json data from out.json file')
with open('imvely.json') as data_file:
    data = json.load(data_file)

print('total::' + str(len(data)))
steps = int(round(len(data) / 500, 0))
print('step::' + str(steps))
print('starts to make a insert data for the google speradsheet')

values = []
inserted_image = []
start_idx = end_idx = 0

for step in range(0, steps):
    if step is 0:
        end_idx = 500

    print('step::' + str(step))
    print('start_idx::' + str(start_idx))
    print('end_idx::' + str(end_idx))

    values = []
    for key in data[start_idx: end_idx]:
        row_data = [key['host_url'], ','.join(key['tag']), '', key['product_name'], key['image_url'],
                    key['product_price'], key['currency_unit'],
                    key['product_url'], key['product_no'], key['main'], key['nation']]
        values.append(row_data)

    body = {
        'values': values
    }
    # print(values)
    print('requesting to google api for inserting data')
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREAD_KEY, range="'stylenanda_master'!A" + str(start), body=body,
        valueInputOption='RAW').execute()

    start_idx += len(values)
    end_idx += len(values)
    start += len(values)
    print(str(start))
    print(result)
