import scrapy
from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup
from bs4.element import Comment
import json
import re

# local
from tesla.items import WikiItem


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

    name = "wikip"
    custom_settings = {
        'ITEM_PIPELINES': {
            'tesla.pipelines.WikiPipeline': 400
        },
        'DEPTH_LIMIT':1
        }

    allowed_domains = ["en.wikipedia.org"]

    start_urls = [
        "https://en.wikipedia.org/wiki/Tesla,_Inc."
    ]
   
    def parse(self, response):
        links = LinkExtractor().extract_links(response)
        urls = [link.url for link in links]
        for url in urls:
            child = scrapy.Request(url, callback = self.parse_dir_contents, meta={'parent':response.url})
            yield child

    def parse_dir_contents(self, response):
        
        item = WikiItem()

        item['url'] = response.url
        item['name'] = ''.join(response.selector.xpath('./td/a/text()').extract())
        item['title'] = response.css('h1::text').extract_first() 
        item['parent'] = response.meta['parent']
        item['text'] = response.xpath('//div[@id="mw-content-text"]//text()')\
            .extract()                      
        
        yield item
