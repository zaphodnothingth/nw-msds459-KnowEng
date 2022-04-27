from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import requests
import re

url = "https://www.autosaur.com/car-brands-complete-list/"
base = re.search("www\.(?P<base>[a-zA-Z]+).com",url)['base']
parser = 'html.parser'  # or 'lxml' (preferred) or 'html5lib', if installed
resp = requests.get(url)
http_encoding = resp.encoding if 'charset' in resp.headers.get('content-type', '').lower() else None
html_encoding = EncodingDetector.find_declared_encoding(resp.content, is_html=True)
encoding = html_encoding or http_encoding
soup = BeautifulSoup(resp.content, parser, from_encoding=encoding)

links = []
for link in soup.find_all('a', href=True):
    if base in link['href'] or 'fastclick' in link['href']:
        continue
    links.append(link['href'])

print(len(links))
print(links)