# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlsteamItem(scrapy.Item):
    # define the fields for your item here like:
    name = scrapy.Field()
    release_date = scrapy.Field()
    final_price_number_discount = scrapy.Field()
    final_price_number_original = scrapy.Field()
    review_summary = scrapy.Field()
    # link = scrapy.Field()
    developer = scrapy.Field()
    publisher = scrapy.Field()
    game_tag = scrapy.Field()
#    dev_names = scrapy.Field()
    description = scrapy.Field()
    # minimum_requirements = scrapy.Field()
    # recommended_requirements = scrapy.Field()
    minimum_ram = scrapy.Field()
    minimum_rom = scrapy.Field()
    minimum_cpu = scrapy.Field()
    
