import scrapy
from scrapy.linkextractors import LinkExtractor
import json

# local
from tesla.items import TeslaItem

class TeslaSpider(scrapy.Spider):

    name = "competitors"
    # allowed_domains = ["deloitte.com"]

    with open("data/companies.jsonl") as f:
        start_urls = [json.loads(line)['url'] 
                        for line in f.readlines()]
   
    def parse(self, response):
        for link in LinkExtractor().extract_links(response):
            url = link.url
            yield scrapy.Request(url, callback = self.parse_dir_contents)

    def parse_dir_contents(self, response):
        
        item = TeslaItem()

        item['url'] = response.url
        item['name'] = ''.join(response.selector.xpath('./td/a/text()').extract())
        item['title'] = response.selector.xpath('//title/text()').extract()  # [0].split('|')[1].strip()
        item['body'] = response.body

        yield item
