# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
import json 
from scrapy.exceptions import DropItem
import csv
import os


class JsonDBCrawlsteamPipeline:
    def process_item(self, item, spider):
        with open('jsondatasteam.json', 'a', encoding='utf-8') as file:
            line = json.dumps(dict(item), ensure_ascii=False) + '\n'
            file.write(line)
        return item

class CSVDBCrawlsteamPipeline:
    def process_item(self, item, spider):
        with open('csvdatasteam.csv', 'a', encoding='utf-8', newline='') as file:
            write = csv.writer(file, delimiter='$')
            write.writerow([
                item.get('name', ''),
                item.get('release_date', ''),
                item.get('original_price', ''),
                item.get('discount_price', ''),
                item.get('link', ''),
                item.get('review_summary', ''),
                item.get('developer', ''),
                item.get('publisher', ''),
                item.get('description', ''),
                item.get('additional_developers', ''),
                item.get('minimum_requirements', ''),
                item.get('recommended_requirements', '')
            ])
        return item

class MongoDBCrawlsteamPipeline:
    def __init__(self):
        econnect = os.environ.get('Mongo_HOST', 'localhost')  # Giá trị mặc định là 'localhost'
        self.client = pymongo.MongoClient(f'mongodb://{econnect}:27017')
        self.db = self.client['DATASTEAM']

    def process_item(self, item, spider):
        collection = self.db['games']
        try:
            collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f'Error inserting item: {e}')
