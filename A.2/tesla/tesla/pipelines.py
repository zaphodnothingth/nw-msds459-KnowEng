# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter



import re
import nltk
import spacy
# python -m spacy download en_core_web_sm
nlp = spacy.load('en_core_web_sm')


# local
# from tesla.items import WikiItem  # item class 
from string import whitespace


def remove_stopwords(tokens):
    stopword_list = nltk.corpus.stopwords.words('english')
    good_tokens = [token for token in tokens if token not in stopword_list]
    return good_tokens     


def url_tags(url):
    tags_list = []
    if len(url.split("/")) > 2:
        tags_list.append(url.split("/")[2])
    if len(url.split("/")) > 3:
        tags_list.append(url.split("/")[3])
    if len(url.split("/")) > 4:
        more_tags = [x.lower() for x in remove_stopwords(url\
                        .split("/")[4].split("_"))]
        for tag in more_tags:
            tag = re.sub('[^a-zA-Z]', '', tag)  # alphanumeric values only  
            tags_list.append(tag)
    
    return tags_list


def ent_extr(doc):
    doc = nlp(doc)
    ent_list = [(str(item.text), str(item.label_)) for item in doc.ents] # list of tuples of entity & label
    return ent_list

class TeslaTextPipeline:
    def process_item(self, item, spider):
        # clean whitespace from title
        item['title'] = [line for line in item['title'] if line not in whitespace]
        item['title'] = ' '.join(item['title'])
        # gen tags from url
        item['tags'] = url_tags(item['url'])
        # clean body whitespace
        item['body'] = ' '.join(item['body'].split())
        # extract entities from body
        entities = []
        entities.extend(ent_extr(item['title']))
        entities.extend(ent_extr(item['body']))
        item['entities'] = entities
        return item


class WikiPipeline(object):
    def process_item(self, item, spider):
        item['text'] = [line for line in item['text'] if line not in whitespace]
        item['text'] = ''.join(item['text'])
        # extract entities from body
        entities = []
        entities.extend(ent_extr(item['title']))
        entities.extend(ent_extr(item['text']))
        item['entities'] = entities
        return item
