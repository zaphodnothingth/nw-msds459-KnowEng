# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class TeslaItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    url = Field()
    name = Field()
    tags = Field()
    entities = Field()
    parent = Field()
    title = Field()
    body = Field()

class WikiItem(Item):
    url = Field()
    name = Field()
    tags = Field()
    entities = Field()
    parent = Field()
    title = Field()
    body = Field()
    keywords = Field()
    text = Field()
