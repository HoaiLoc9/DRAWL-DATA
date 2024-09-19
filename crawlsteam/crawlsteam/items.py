# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class CrawlsteamItem(scrapy.Item):
    name = scrapy.Field()
    release_date = scrapy.Field()
    original_price = scrapy.Field()
    discount_price = scrapy.Field()
    link = scrapy.Field()
    description = scrapy.Field()
    review_summary = scrapy.Field()
    developer = scrapy.Field()
    publisher = scrapy.Field()
    game_tag = scrapy.Field()
    dev_names = scrapy.Field()
    minimum_requirements = scrapy.Field()
    recommended_requirements = scrapy.Field()
