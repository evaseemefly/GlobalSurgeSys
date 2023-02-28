from typing import List
from arrow import Arrow
import arrow
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base

from common.enums import TaskTypeEnum
from models.models import StationRealDataSpecific, SpiderTaskInfo
from sqlalchemy import ForeignKey, Sequence, MetaData, Table
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from core.db import DbFactory
from conf.settings import TASK_OPTIONS, DB_TABLE_SPLIT_OPTIONS


class StationSurgeRealData:
    """
        海洋站潮位实况
        实现分表功能
    """

    def __init__(self, station_code: str, list_station_realdata: List[StationRealDataSpecific]):
        self.station_code = station_code
        self.list_station_realdata = list_station_realdata
        pass

    @property
    def gmt_start(self) -> Arrow:
        """
            起始时间
        """

        def _sort(x, y):
            if x < y:
                return 1
            elif x > y:
                return -1
            else:
                return 0

        # sorted_list = self.list_station_realdata.sort(_sort)
        sorted_list = sorted(self.list_station_realdata, key=lambda station: station['ts'])
        start_ts: Arrow = sorted_list[0]['ts']
        return arrow.get(start_ts)

    @property
    def gmt_end(self) -> Arrow:
        def _sort(x, y):
            if x < y:
                return 1
            elif x > y:
                return -1
            else:
                return 0

        # 倒叙排列
        sorted_list = sorted(self.list_station_realdata, key=lambda station: station['ts'], reverse=True)
        end_ts: Arrow = sorted_list[0]['ts']
        return arrow.get(end_ts)

    def create_split_tab(self) -> bool:
        """
            创建分表
        """
        is_created = self._check_need_split_tab(to_create=True)
        return is_created
        pass

    def check_realdata_list(self, station_code: str, to_coverage: bool = False,
                            realdata_list: List[StationRealDataSpecific] = []) -> bool:
        """
            判断 当前需要插入的数据是否已经存在于 表中，若存在且 to_coverage = True 则进行覆盖
        :param to_coverage:
        :param realdata_list:
        :return:
        """
        tab_name: str = self._get_split_tab_name()
        if self._check_exist_tab(tab_name):
            self._insert_realdata(station_code, realdata_list, to_coverage=to_coverage)

        pass

    def _insert_realdata(self, station_code: str, realdata_list: List[StationRealDataSpecific],
                         to_coverage: bool = False):
        """
            若 to_coverage = True 则向表中覆盖已存在的数据
        :param realdata_list:
        :param to_coverage:
        :return:
        """
        dist_stationcodes: set[str] = set([temp.station_code for temp in realdata_list])
        for dist_station_code in dist_stationcodes:
            temp_stationcode_realdata = list(filter(lambda o: o['station_code'] == dist_station_code, realdata_list))

    def _check_need_split_tab(self, to_create: bool = True) -> bool:
        """
            判断是否需要分表，若已存在表是否覆盖
        @param to_create: 是否覆盖
        @return:
        """
        tab_name: str = self._get_split_tab_name()
        is_need = False
        if self._check_exist_tab(tab_name) is False or (self._check_exist_tab(tab_name) and to_create):
            is_need = self._create_station_realdata_tab(tab_name)
        return is_need

    def _get_split_tab_name(self) -> str:
        """
            获取分表名称
            eg: gmt_start 2023-02-26 xxx_202302
        @return: 分表名称 eg: station_realdata_specific_202302
        """
        tab_base_name: str = DB_TABLE_SPLIT_OPTIONS.get('station').get('tab_split_name')
        tab_dt_name: str = self.gmt_start.format('YYYYMM')
        tab_name: str = f'{tab_base_name}_{tab_dt_name}'
        return tab_name

    def _check_exist_tab(self, tab_name: str) -> bool:
        """
            判断指定表是否存在
        @param tab_name:
        @return:
        """
        is_exist = False
        auto_base = automap_base()
        db_factory = DbFactory()
        session = db_factory.Session
        engine = db_factory.engine
        auto_base.prepare(engine, reflect=True)
        list_tabs = auto_base.classes
        if tab_name in list_tabs:
            is_exist = True
        return is_exist
        pass

    def _create_station_realdata_tab(self, tab_name: str) -> bool:
        """
            根据 tab_name 创建对应的潮位实况表
        @param tab_name:
        @return:
        """
        is_ok = False
        meta_data = MetaData()
        Table(tab_name, meta_data, Column('id', Integer, primary_key=True),
              Column('is_del', TINYINT(1), nullable=False, server_default=text("'0'"), default=0),
              Column('station_code', VARCHAR(200), nullable=False), Column('tid', Integer, nullable=False),
              Column('surge', Float, nullable=False),
              Column('timestamp', VARCHAR(100), nullable=False),
              Column('gmt_create_time', DATETIME(fsp=6), default=datetime.utcnow),
              Column('gmt_modify_time', DATETIME(fsp=6), default=datetime.utcnow))
        db_factory = DbFactory()
        session = db_factory.Session
        engine = db_factory.engine
        with engine.connect() as conn:
            # result_proxy = conn.execute(sql_str)
            # result = result_proxy.fetchall()
            try:
                meta_data.create_all(engine)
                is_ok = True
            except Exception as ex:
                print(ex.args)
        return is_ok


class SpiderTask:
    def __init__(self, now_utc: Arrow, count_stations: int, task_name: str):
        self.now_utc = now_utc
        self.count_stations = count_stations
        self.task_name = task_name
        self.session = DbFactory().Session
        pass

    def to_db(self) -> int:
        interval: int = TASK_OPTIONS.get('interval')
        task_info: SpiderTaskInfo = SpiderTaskInfo(timestamp=self.now_utc.timestamp, task_name=self.task_name,
                                                   task_type=TaskTypeEnum.SUCCESS.value,
                                                   spider_count=self.count_stations, interval=interval)
        self.session.add(task_info)
        self.session.commit()
        return task_info.id
