# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from typing import List
from globalsurgespider.items import StationsSurgeItem, StationSurgeListItem
from core.data import StationSurgeRealData
from models.models import StationRealDataSpecific


class GlobalsurgespiderPipeline:
    def process_item(self, item: StationSurgeListItem, spider):
        # TODO:[*] 23-02-22 STEP1: 将 items.py -> StationSurgeListItem 批量写入 db
        # TODO:[*] 23-02-22 STEP2: 批量录入后 更新 station_status,可使用装饰器实现
        # print(item)
        self.to_db(item)
        return item

    def to_db(self, item: dict):
        """

        @param item: ['station_code','surge_list']
        @return:
        """
        station_code: str = item['station_code']
        list_realdata: List[StationSurgeListItem] = item['surge_list']
        tid: int = item['tid']
        stationSurge = StationSurgeRealData(station_code, list_realdata, tid)
        # step1: 创建分表
        stationSurge.create_split_tab()
        # step2: 像 station_realdata_specific 表中写入当前实况数据
        stationSurge.insert_realdata_list(to_coverage=True, realdata_list=list_realdata)
        pass
