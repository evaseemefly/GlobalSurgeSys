# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class GlobalsurgespiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class StationSurgeListItem(scrapy.Item):
    """
        站点潮位集合 Item
    """
    station_code = scrapy.Field()
    tid = scrapy.Field()
    surge_list = scrapy.Field()


class StationsSurgeItem(scrapy.Item):
    """
        站点 surge item
    """
    dt = scrapy.Field()
    surge = scrapy.Field()
    ts = scrapy.Field()
