import scrapy
from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup
from bs4.element import Comment
import json
import re
import pycountry
import langid

# local
from tesla.items import TeslaItem


### Functions for parsing body
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(t.strip() for t in visible_texts)


class TeslaSpider(scrapy.Spider):

    name = "competitors"
    custom_settings = {
        'ITEM_PIPELINES': {
            'tesla.pipelines.TeslaTextPipeline': 300,
            'tesla.pipelines.pgPipeline': 400
        }
        }
    # allowed_domains = ["deloitte.com"]

    with open("data_in/companies.jsonl") as f:
        start_urls = [json.loads(line)['url'] 
                        for line in f.readlines()]
   
    def parse(self, response):
        links = LinkExtractor().extract_links(response)
        urls = [link.url for link in links]
        prohibited = ['yandex', 'terms-and-conditions', 'sitemap', 'site-map', 'copyright'] # prohibited url pieces
        urls = [url for url in urls if all([proh not in url for proh in prohibited])] # drop links with prohibited pieces
        proh_countries = [country.alpha_2 for country in pycountry.countries if country.alpha_2 != 'US'] # filter other countries
        urls = [url for url in urls if not any([bool(re.search(rf"[/\.]{code}/?$", url, re.IGNORECASE)) for code in proh_countries])] # drop links ending with proh countries
        for url in urls:
            child = scrapy.Request(url, callback = self.parse_dir_contents, meta={'parent':response.url})
            yield child

    def parse_dir_contents(self, response):
        
        item = TeslaItem()

        item['url'] = response.url
        item['name'] = ''.join(response.selector.xpath('./td/a/text()').extract())
        item['title'] = response.selector.xpath('//title/text()').extract()  # [0].split('|')[1].strip()
        item['parent'] = response.meta['parent']
        html = response.text
        body = text_from_html(html)
        if langid.classify(body)[0] in ('en', 'uk'): # accept only english language
            item['body'] = body
            yield item
