import requests
import re
import time
import json

import urllib.request 

#The URL
url = "https://www.autosaur.com/car-brands-complete-list/"
base_dict = {
    "url":url, 
    "crawl_depth":0
    }
urllib.request.urlopen(url)
base_dict['crawl_ts'] = time.time() # mark crawl timestamp
base_dict['node_type'] = 'Website'
request_url = urllib.request.urlopen(url) 
full_page = request_url.read() 

r = re.compile(
    'img [\s\S]*?src=\"(?P<img_src>[^\"]+)\" alt=\"(Car brands: )?(?P<company_name>[^\"]+)\"[\s\S]*?Headquarters: (?P<hq_loc>[^<]+)[\s\S]*?Parent company: (?P<parent_company>[^<]+)[\s\S]*?Website:\s?<a href=\"(?P<url>[^\"]+)\">[\s\S]*?<p>'
)
finds = [m.groupdict() for m in r.finditer(full_page.decode("utf-8"))]

with open('companies.jsonl', 'w') as wf:
    json.dump(base_dict, wf)
    wf.write('\n') # convert to jsonl
    for f in finds:
        # record company relationships
        f['relationships'] = []
        f['relationships'].append({'COMPETES_WITH':'tesla'})
        f['relationships'].append({'HAS_PARENT':f['parent_company']})
        f['relationships'].append({'HEADQUARTERED_IN':f['hq_loc']})
        # fill in company attributes
        f['node_type'] = 'Auto Manufacturer'
        f['body'] = 'tbc'
        f['keywords'] = 'tbc'
        f['crawl_depth'] = 1
        f['crawl_ts'] = None
        f['crawl_parent'] = None # need to generate IDs
        f['children'] = 'gen list of parsed urls'
        json.dump(f, wf)
        wf.write('\n')

print(len(finds))
print('sample:\n', finds[0])

print('done')

