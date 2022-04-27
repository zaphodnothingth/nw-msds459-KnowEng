from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector

# local
from tesla.items import TeslaItem

class TeslaSpider(BaseSpider):

    name = "competitors"
    # allowed_domains = ["deloitte.com"]

    with open("../companies.jsonl") as f:
        start_urls = [json.loads(line)['url'] 
                        for line in f.readlines()]

    def parse(self, response):

        hxs = HtmlXPathSelector(response)
        sites = hxs.select('//table[@class="custom_table"]/tr')

        items = []

        for site in sites:

            #print site
            item = TeslaItem()

            item['name'] = ''.join(site.select('./td/a/text()').extract())
            item['url'] = ''.join(site.select('./td/a/@href').extract())
            # item['title'] = ''.join(site.select('./td[4]/text()').extract())
            item['title'] = site.select('//title/text()').extract()  # [0].split('|')[1].strip()
            
            # use xpath differently
            item['content'] = response.xpath('//div[@id=$value]/a/text()', value='content').get()


            items.append(item)

        return items
