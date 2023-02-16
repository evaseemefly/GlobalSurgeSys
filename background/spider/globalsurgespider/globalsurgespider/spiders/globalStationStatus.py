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
from globalsurgespider.items import StationsSurgeItem
from globalsurgespider.settings import SPIDER_TITLE_STAMPS


class GlobalstationstatusSpider(scrapy.Spider):
    name = 'globalStationStatus'
    # 爬取的域： http://www.ioc-sealevelmonitoring.org/
    allowed_domains = ['www.ioc-sealevelmonitoring.org']
    start_urls = ['http://www.ioc-sealevelmonitoring.org/service.php?server=gml&show=active&showgauges=t']

    def parse(self, response: XmlResponse):
        print(response)
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
        selector_list = selector_list[:5]
        list_station_status: List[dict] = []
        for point in selector_list:
            code = point.xpath('code/text()').get()
            # code_str=re.match(code_xml,'')
            status = point.xpath('status/text()').get()
            list_station_status.append({'code': code, 'status': status})
            print(f'当前状态:code:{code},status:{status}')

        # 遍历 list_station_status 继续爬取单站数据
        for item_station in list_station_status:
            # 判断当前站点的状态,为 online 则执行下一步爬取操作
            if item_station['status'] == 'online':
                # 当前的站点 code
                temp_code: str = item_station['code']
                url = f'http://www.ioc-sealevelmonitoring.org/bgraph.php?code={temp_code}&output=tab&period=0.5'
                yield Request(url, callback=self.station_surge_parise)
        pass

    def station_surge_parise(self, response: HtmlResponse):
        """
            爬取指定的共享站点的潮位数据
        :param response: html页面，可以通过html的方式进行解析
        :return: {'dt': datetime.datetime(2023, 2, 14, 19, 42, tzinfo=tzutc()),
                 'surge': 2.2037,
                 'ts': 1676403720}[]
        """

        list_station_surge: List[dict] = self.station_surge_html2list(response, SPIDER_TITLE_STAMPS)
        list_items: List[StationsSurgeItem] = []
        for surge in list_station_surge:
            item = StationsSurgeItem()
            item['dt'] = surge['dt']
            item['ts'] = surge['ts']
            item['surge'] = surge['surge']
            yield item
        #     list_items.append(item)
        # return list_items

    def station_surge_html2list(self, response: HtmlResponse, list_stamp: List[str] = ['rad']) -> List[dict]:
        """
            将指定站点的 html -> list 返回
            'http://www.ioc-sealevelmonitoring.org/bgraph.php?code={temp_code}&output=tab&period=0.5'
        :param response:
        :param list_stamp: 匹配 surge 的头文件名称,默认为 'rad'
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
        # TODO:[-] 22-04-26 注意此处 由于本身页面有 html > body > div 而打印出来的只保留了 div 中的部分，缺失了前面的 html > body
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
                # TODO:[-] 22-05-17 注意此处可能出现多个 td 需要先找到 rad 所在的列的 index
                try:
                    if len(td_list) >= 2:
                        table_name = td_list[0].text
                        for td_index, td_name in enumerate(td_list):
                            # TODO:[-] 22-11-18 由于标志符目前看可能不止一种，修改为数组
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
                                # TODO:[-] 22-04-27 注意此处的 时间戳转为换 int ，不要使用 float
                                # ts:1548260567 10位
                                temp_dict = {'dt': temp_dt,
                                             'surge': float(np.nan if temp_rad_str.strip() == '' else temp_rad_str),
                                             'ts': arrow.get(temp_dt_str).int_timestamp}
                                list_station_rad.append(temp_dict)
                                # for td in td_list[index:]:
                                #     print(td)
                except Exception as ex:
                    print(ex.args)
            # 将 list_station_red -> dataframe

            if len(list_station_rad) > 0:
                print('处理成功~')
            else:
                print('处理失败!')
        except Exception as e:
            print(e.args)
        return list_station_rad
