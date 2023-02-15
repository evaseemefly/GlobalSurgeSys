import scrapy
from scrapy.selector import Selector
from scrapy.http import XmlResponse
from bs4 import BeautifulSoup
from lxml import etree
import re


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
        sel = Selector(text=response.body)
        # 取出所有的 point点
        selector_list = sel.xpath('body/featurecollection/featuremember/point')
        # 只取出前5个
        selector_list = selector_list[:5]
        for point in selector_list:
            code = point.xpath('code/text()').get()
            # code_str=re.match(code_xml,'')
            status = point.xpath('status/text()').get()
            print(f'当前状态:code:{code},status:{status}')
        print(sel.xpath('./wfs/gml'))
        # TODO:[-] 23-02-14 注意此处 fromstring 不能直接转换 response 需要转换二进制的内容 response.body
        root = etree.fromstring(response.body)
        for gml in root.getchildren():
            print(gml)

        pass
