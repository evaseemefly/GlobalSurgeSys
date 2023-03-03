import scrapy
from scrapy import Request
from scrapy.selector import Selector
from scrapy.http import XmlResponse, HtmlResponse
from bs4 import BeautifulSoup
from lxml import etree
import arrow
import pandas as pd
import numpy as np
import re
from typing import List
# 本項目的
from core.data import SpiderTask
from globalsurgespider.items import StationsSurgeItem, StationSurgeListItem
from globalsurgespider.settings import SPIDER_TITLE_STAMPS
from models.models import SpiderTaskInfo
from decorators import provide_store_task
from common.enums import TaskTypeEnum
from core.db import DbFactory
from conf.settings import TASK_OPTIONS


class GlobalstationstatusSpider(scrapy.Spider):
    name = 'globalStationStatus'
    # 爬取的域： http://www.ioc-sealevelmonitoring.org/
    allowed_domains = ['www.ioc-sealevelmonitoring.org']
    start_urls = ['http://www.ioc-sealevelmonitoring.org/service.php?server=gml&show=active&showgauges=t']
    session = DbFactory().Session

    def parse(self, response: XmlResponse):

        # print(response)
        # utc时间
        now_utc: arrow.Arrow = arrow.Arrow.utcnow()
        date_utc_ymdhm: str = now_utc.format('YYYYMMDDHHmm')
        task_name_prefix: str = TASK_OPTIONS.get('name_prefix')
        task_name: str = f'{task_name_prefix}{date_utc_ymdhm}'
        # response.text
        # xpath_parse = response.xpath('/wfs/gml:Point')
        temp = response.xpath('body/featurecollection/featuremember/point')
        soup = BeautifulSoup(response.body)
        soup_temp = soup.find('body/featurecollection/featuremember/point')
        # response.
        # TODO:[-] 23-02-14 注意此处 fromstring 不能直接转换 response 需要转换二进制的内容 response.body。注意不能通过 response.text 获取对应的响应内容
        sel = Selector(text=response.body)
        # 取出所有的 point点
        selector_list = sel.xpath('body/featurecollection/featuremember/point')
        # 只取出前5个
        # TODO:[*] 23-03-02 测试全部
        # selector_list = selector_list[:2]

        list_station_status: List[dict] = []
        for point in selector_list:
            code = point.xpath('code/text()').get()
            # code_str=re.match(code_xml,'')
            status = point.xpath('status/text()').get()
            list_station_status.append({'code': code, 'status': status})
            print(f'当前状态:code:{code},status:{status}')
        task_info = SpiderTask(now_utc, len(list_station_status), task_name)
        tid = task_info.to_db()
        params_item: dict = {'tid': tid}
        # 遍历 list_station_status 继续爬取单站数据
        # 注意此处若直接调用 self.func 的话不会执行？
        # self.spider_all_station(list_station_status)
        # self.spider_test(list_station_status)
        # 方法1: 目前只能通过此种方法实现
        # TODO:[-] 23-02-28 将以下代码放置 spider_test 方法中，无法触发 yield -> station_test_parse
        # list_station_status = [{'code': 'palm1', 'status': 'online'}]
        for item_station in list_station_status:
            try:
                # 判断当前站点的状态,为 online 则执行下一步爬取操作
                if item_station['status'] == 'online':
                    # 当前的站点 code
                    temp_code: str = item_station['code']
                    params_item['station_code'] = temp_code
                    url = f'http://www.ioc-sealevelmonitoring.org/bgraph.php?code={temp_code}&output=tab&period=0.5'
                    yield Request(url, callback=self.station_surge_parse, meta=params_item)
            except Exception as ex:
                print(ex)

        # 方法2:调用生成器 此种方式无法触发 yield -> station_test_parse
        # self.spider_all_station(list_station_status, now_utc, task_name)
        # 方法3: 注明为生成器
        # generator = self.spider_test(list_station_status)
        # yield generator
        # 方法4: 将 以上代码封装至 func 中，并通过生成器调用，但人不会执行 parse 方法
        # station_spider_generator = self.spider_generator_fun(list_station_status)
        # while True:
        #     try:
        #         next(station_spider_generator)
        #     except:
        #         break
        # 方法5: 在 parse 方法中定义一个内部方法,此种办法仍不行
        # def _generator_target_station(list_station):
        #     for item_station in list_station:
        #         try:
        #             # 判断当前站点的状态,为 online 则执行下一步爬取操作
        #             if item_station['status'] == 'online':
        #                 # 当前的站点 code
        #                 temp_code: str = item_station['code']
        #                 url = f'http://www.ioc-sealevelmonitoring.org/bgraph.php?code={temp_code}&output=tab&period=0.5'
        #                 yield scrapy.Request(url, callback=self.station_surge_parse)
        #         except Exception as ex:
        #             print(ex)
        #
        # temp_generator = _generator_target_station(list_station_status)
        # while True:
        #     try:
        #         next(temp_generator)
        #     except Exception as ex:
        #         print('结束生成器循环')
        #         break
        pass

    def spider_generator_fun(self, list_station_status: List[dict]):
        for item_station in list_station_status:
            try:
                # 判断当前站点的状态,为 online 则执行下一步爬取操作
                if item_station['status'] == 'online':
                    # 当前的站点 code
                    temp_code: str = item_station['code']
                    url = f'http://www.ioc-sealevelmonitoring.org/bgraph.php?code={temp_code}&output=tab&period=0.5'
                    yield Request(url, callback=self.station_surge_parse)
            except Exception as ex:
                print(ex)

    def spider_test(self, list_station_status: List[dict]):
        for item_station in list_station_status:
            try:
                # 判断当前站点的状态,为 online 则执行下一步爬取操作
                if item_station['status'] == 'online':
                    # 当前的站点 code
                    temp_code: str = item_station['code']
                    url = f'http://www.ioc-sealevelmonitoring.org/bgraph.php?code={temp_code}&output=tab&period=0.5'
                    yield Request(url, callback=self.station_test_parse)
            except Exception as ex:
                print(ex)

    def generator_spider_all_station(self, list_station_status: List[dict] = []):
        """
            爬取全部站点的生成器
            注意此处不能不传入 url
            TypeError: Request url must be str, got NoneType
        :param list_station_status:
        :return:
        """
        for item_station in list_station_status:
            # 判断当前站点的状态,为 online 则执行下一步爬取操作
            if item_station['status'] == 'online':
                # 当前的站点 code
                temp_code: str = item_station['code']
                url = f'http://www.ioc-sealevelmonitoring.org/bgraph.php?code={temp_code}&output=tab&period=0.5'
                # TODO:[*] 23-02-27 注意此处不会执行 station_surge_parise 方法
                # yield print(temp_code)
                # 参考文章: https://blog.csdn.net/sinat_28984567/article/details/109001820
                yield Request(url, dont_filter=True, callback=self.station_surge_parse)
        # TODO:[*] 23-02-22 STEP3: 将 将 spider_count interval 等信息写入 tb:spider_task_info

    # @provide_store_task
    def spider_all_station(self, list_station_status: List[dict], now_utc: arrow.Arrow, task_name: str):
        """
            注意由于本方法中包含 yield 字段，所以本方法为一个生成器(generator)，不能直接调用笨方法
        :param list_station_status:
        :param now_utc: 传入的当前时间
        :param task_name: 任务名
        :return:
        """
        index = 0
        # 手动调用生成器
        # generator = self.generator_spider_all_station(list_station_status)
        self.spider_test(list_station_status)
        # generator = self.spider_test(list_station_status)
        #
        # while index < len(list_station_status):
        #     try:
        #         # 当执行了第一遍之后提示 'NoneType' object is not callable
        #         next(generator)
        #         # generator.next()
        #         print(index)
        #         index += 1
        #     except Exception as ex:
        #         # 'list_iterator' object is not callable
        #         print(ex)
        #         break
        # # TODO:[*] 23-02-22 STEP3: 将 将 spider_count interval 等信息写入 tb:spider_task_info
        # interval: int = TASK_OPTIONS.get('interval')
        # task_info: SpiderTaskInfo = SpiderTaskInfo(timestamp=now_utc.timestamp, task_name=task_name,
        #                                            task_type=TaskTypeEnum.SUCCESS.value,
        #                                            spider_count=len(list_station_status), interval=interval)
        # self.session.add(task_info)
        # self.session.commit()

    def station_surge_parse(self, response: HtmlResponse):
        """
            爬取单个站点并提交 pipeline 持久化保存
            爬取指定的共享站点的潮位数据
        :param response: html页面，可以通过html的方式进行解析
        :param count: list 每次截取的长度，默认长度30.时间间隔1min。
        :return: {'dt': datetime.datetime(2023, 2, 14, 19, 42, tzinfo=tzutc()),
                 'surge': 2.2037,
                 'ts': 1676403720}[]
        """
        count: int = 30
        tid = response.meta['tid']
        station_code = response.meta['station_code']
        list_station_surge: List[dict] = self.station_surge_html2list(response, SPIDER_TITLE_STAMPS, station_code)
        list_items: List[StationsSurgeItem] = []
        list_station_surge = list_station_surge[::-1][:count]

        for surge in list_station_surge:
            item = StationsSurgeItem()
            item['dt'] = surge['dt']
            item['ts'] = surge['ts']
            item['surge'] = surge['surge']
            list_items.append(item)
            # yield item
        itemStationList: StationSurgeListItem = StationSurgeListItem()
        itemStationList['station_code'] = station_code
        itemStationList['tid'] = tid
        itemStationList['surge_list'] = list_items
        yield itemStationList
        # yield list_items
        #     list_items.append(item)
        # return list_items

    def station_test_parse(self, response: HtmlResponse):
        yield print('监听到触发 test parse')

    def station_surge_html2list(self, response: HtmlResponse, list_stamp: List[str] = ['rad'],
                                station_code: str = 'default') -> List[dict]:
        """
            将指定站点的 html -> list 返回
            'http://www.ioc-sealevelmonitoring.org/bgraph.php?code={temp_code}&output=tab&period=0.5'
        :param response:
        :param list_stamp: 匹配 surge 的头文件名称,默认为 'rad'
        :param station_code: 当前处理的站点 code
        :return:
        """
        # print(response)
        html_content = response.text
        '''
            eg:
                <div align=center>
                    <table border="1">
                        <th colspan="2">Tide gauge at Qinglan</th>
                        <tr><td>Time (UTC)</td><td class=field>rad(m)</td></tr>
                        <tr><td>2022-04-19 07:04:00</td><td>1
        '''
        # print(html_content)
        etree_htm = etree.HTML(html_content)
        # df: pd.DataFrame = None
        list_station_rad: List[dict] = []

        '''
                eg:
                    <div align=center>
                        <table border="1">
                            <th colspan="2">Tide gauge at Qinglan</th>
                            <tr><td>Time (UTC)</td><td class=field>rad(m)</td></tr>
                            <tr><td>2022-04-19 07:04:00</td><td>1
                TODO:[-] 22-09-27 
                出现了 stamp 为 wls(m) 
            '''
        # 定位到 table -> tbody
        # 22-04-26 注意此处 由于本身页面有 html > body > div 而打印出来的只保留了 div 中的部分，缺失了前面的 html > body
        try:
            content = etree_htm.xpath('/html/body/div/table/tr')
            # print(content)
            table_name_index: int = -1

            rad_stamp_list: List[str] = list_stamp
            rad_col_index: int = -1

            for index, tr in enumerate(content):
                # print(tr)
                td_list = tr.xpath('td')
                '''
                    <td>Time (UTC)</td><td class=field>rad(m)</td>
                '''
                # 22-05-17 注意此处可能出现多个 td 需要先找到 rad 所在的列的 index
                try:
                    if len(td_list) >= 2:
                        table_name = td_list[0].text
                        for td_index, td_name in enumerate(td_list):
                            # 22-11-18 由于标志符目前看可能不止一种，修改为数组
                            if td_name.text in rad_stamp_list:
                                rad_col_index = td_index
                        if table_name == 'Time (UTC)':
                            table_name_index = index
                            # print(f'找到Time表头所在行index为:{table_name_index}')
                        else:
                            if rad_col_index >= 0:
                                temp_dict = {}
                                # <tr><td>2022-04-19 07:04:00</td><td>1
                                # '2022-04-19 13:31:00'
                                temp_dt_str: str = td_list[0].text
                                # 2022-04-19T13:31:00+00:00
                                # 注意时间为 UTC 时间
                                temp_dt = arrow.get(temp_dt_str).datetime
                                temp_rad_str: str = td_list[rad_col_index].text
                                # 22-04-27 注意此处的 时间戳转为换 int ，不要使用 float
                                # ts:1548260567 10位
                                temp_dict = {'dt': temp_dt,
                                             'surge': float(np.nan if temp_rad_str.strip() == '' else temp_rad_str),
                                             'ts': arrow.get(temp_dt_str).timestamp}
                                list_station_rad.append(temp_dict)
                                # for td in td_list[index:]:
                                #     print(td)
                except Exception as ex:
                    print(ex.args)
            # 将 list_station_red -> dataframe

            if len(list_station_rad) > 0:
                print(f'[-]def station_surge_html2list : 处理:{station_code}成功~')
            else:
                print(f'[-]def station_surge_html2list : 处理:{station_code}失败!')
        except Exception as e:
            print(f'[*]def station_surge_html2list : 处理:{station_code}异常!')
            print(e.args)
        return list_station_rad
