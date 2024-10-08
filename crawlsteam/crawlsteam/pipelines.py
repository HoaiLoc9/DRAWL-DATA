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
    pass  


class CSVDBCrawlsteamPipeline:
    def process_item(self, item, spider):
        with open('csvdatasteam.csv', 'a', encoding='utf-8', newline='') as file:
            write = csv.writer(file, delimiter=",")
            write.writerow([
                item.get('name', ''),
                item.get('release_date', ''),
                item.get('final_price_number_discount', ''),
                item.get('final_price_number_original', ''),
                # item.get('link', ''),
                item.get('review_summary', ''),
                item.get('developer', ''),
                item.get('publisher', ''),
                item.get('description', ''),
                item.get('game_tag', ''),
#                item.get('additional_developers', ''),
                # item.get('minimum_requirements', ''),
                # item.get('recommended_requirements', '')
                item.get('minimum_ram', ''),
                item.get('minimum_rom', ''),
                item.get('minimum_cpu', '')
            ])
        return item
    pass


# class MongoDBCrawlsteamPipeline:
#     def __init__(self):
#         econnect = str(os.environ['Mongo_HOST'])
#         self.client = pymongo.MongoClient('mongodb://'+econnect+':27017')
#         self.db = self.client['DATASTEAM']
#         pass
    
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
        
#class CrawlsteamPipeline:
#    def process_item(self, item, spider):
#        return item

# class MongoDBPipeline:
#     def __init__(self, mongo_uri, mongo_db):
 #        self.mongo_uri = mongo_uri
 #        self.mongo_db = mongo_db

#     @classmethod
#     def from_crawler(cls, crawler):
#         return cls(
#             mongo_uri=crawler.settings.get('MONGO_URI'),
#             mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
#         )

#     def open_spider(self, spider):
#         """Kết nối với MongoDB khi spider bắt đầu chạy."""
#         self.client = pymongo.MongoClient(self.mongo_uri)
#         self.db = self.client[self.mongo_db]

#     def close_spider(self, spider):
#         """Đóng kết nối khi spider dừng chạy."""
#         self.client.close()

#     def process_item(self, item, spider):
#         """Lưu item vào MongoDB."""
#         self.db['games'].insert_one(dict(item))  # 'games' là tên của collection
#         return item
