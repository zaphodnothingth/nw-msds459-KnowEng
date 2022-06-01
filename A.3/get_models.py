import json
import urllib
import requests

""" already run get
url = 'https://parseapi.back4app.com/classes/Carmodels_Car_Model_List?limit=1000000&order=Make'
headers = {
    'X-Parse-Application-Id': 'u8tMN1sYjkB3Zlmu7vDr3SEbtgseXfsei5eIokeo', # This is your app's application id
    'X-Parse-REST-API-Key': 'MWG4GDeuymPUwZbI8SERxr8I6Ve4G4ui6bXeJjJh' # This is your app's REST API key
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
# print(json.dumps(data, indent=2))
with open('data_out/vehicle_models.json', 'w') as outfile:
    json.dump(data, outfile)
"""

# convert to jsonl
with open('data_out/vehicle_models.json', 'r') as infile:
    data = json.load(infile)
    res = data['results']

with open('data_out/vehicle_models.jsonl', 'w') as outfile:
    for entry in res:
        json.dump(entry, outfile)
        outfile.write('\n')
