# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class GlobalsurgespiderPipeline:
    def process_item(self, item, spider):
        # TODO:[*] 23-02-22 STEP1: 将 items.py -> StationSurgeListItem 批量写入 db
        # TODO:[*] 23-02-22 STEP2: 批量录入后 更新 station_status,可使用装饰器实现
        print(item)
        return item

    def to_db(self, item: dict):
        pass
