from typing import List
from arrow import Arrow
import arrow
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import select, update
from sqlalchemy.ext.automap import automap_base
import sqlalchemy
import numpy as np

from common.enums import TaskTypeEnum
from globalsurgespider.items import StationSurgeListItem
from models.models import StationRealDataSpecific, SpiderTaskInfo, StationStatus
from sqlalchemy import ForeignKey, Sequence, MetaData, Table
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from core.db import DbFactory
from decorators import timer_count
from conf.settings import TASK_OPTIONS, DB_TABLE_SPLIT_OPTIONS


class StationSurgeRealData:
    """
        海洋站潮位实况
        实现分表功能
    """

    def __init__(self, station_code: str, list_station_realdata: List[StationRealDataSpecific], tid: int):
        self.station_code = station_code
        self.list_station_realdata = list_station_realdata
        self.tid = tid
        self.session = DbFactory().Session
        pass

    @property
    def gmt_start(self) -> Arrow:
        """
            起始时间
        """

        # sorted_list = self.list_station_realdata.sort(_sort)
        sorted_list = sorted(self.list_station_realdata, key=lambda station: station['ts'])
        start_ts: Arrow = sorted_list[0]['ts']
        return arrow.get(start_ts)

    @property
    def gmt_end(self) -> Arrow:
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

    def insert_realdata_list(self, to_coverage: bool = False,
                             realdata_list: List[StationSurgeListItem] = []) -> bool:
        """
            判断 当前需要插入的数据是否已经存在于 表中，若存在且 to_coverage = True 则进行覆盖
        :param to_coverage:
        :param realdata_list:
        :return:
        """
        tab_name: str = self._get_split_tab_name()
        station_code: str = self.station_code
        if self._check_exist_tab(tab_name):
            # 动态更新表名
            self._insert_realdata(tab_name, station_code, realdata_list, to_coverage=to_coverage)

        pass

    @timer_count(10)
    def _insert_realdata(self, tab_name: str, station_code: str, realdata_list: List[StationSurgeListItem],
                         to_coverage: bool = False):
        """
            若 to_coverage = True 则向表中覆盖已存在的数据
        :param realdata_list:
        :param to_coverage:
        :return:
        """
        # dist_stationcodes: set[str] = set([temp.station_code for temp in realdata_list])
        # for dist_station_code in dist_stationcodes:
        #     temp_stationcode_realdata = list(filter(lambda o: o['station_code'] == dist_station_code, realdata_list))
        # 按照 station_code | timestamp | gmt_dt 查询，若存在则批量更新
        # 动态修改当前的表名
        StationRealDataSpecific.__table__.name = tab_name
        # 根据 station_code | gmt_start < gmt_realitime < gmt_end 过滤结果
        # 对于要插入的集合进行遍历
        # -> S1: station_code | gmt_realtime | ts 存在 则 update
        # -> s2: station_code | gmt_realtime | ts 不存在 则 create
        # query = select(StationRealDataSpecific).where(StationRealDataSpecific.station_code == station_code).where(
        #     StationRealDataSpecific.gmt_realtime > self.gmt_start).where(
        #     StationRealDataSpecific.gmt_realtime < self.gmt_end)
        query = select(StationRealDataSpecific).where(StationRealDataSpecific.station_code == station_code)
        # 插入
        print(f'[-]写入{station_code}站点的realdata,count:{len(realdata_list)}中~')
        for temp_realdata in realdata_list:
            # ERROR:
            #     raise AttributeError(f"Use item[{name!r}] to get field value")
            # AttributeError: Use item['surge'] to get field value
            # 判断是否存在
            temp_query = query.where(StationRealDataSpecific.gmt_realtime == temp_realdata['dt'])
            temp_now: datetime = datetime.utcnow()
            surge: float = temp_realdata['surge']
            if np.isnan(surge):
                continue
            try:
                temp_query = self.session.scalars(temp_query).fetchall()
                # TODO:[*] 23-03-03 注意此处的 surge 有可能为 nan 需要加入判断
                # surge:int=
                if len(temp_query) > 0:
                    # update
                    self.session.execute(
                        update(StationRealDataSpecific).where(
                            StationRealDataSpecific.station_code == station_code).where(
                            StationRealDataSpecific.gmt_realtime == temp_realdata['dt']).values(
                            surge=surge, gmt_modify_time=temp_now)).execute_options(
                        synchronize_session="evaluate")
                else:
                    obj_realdata = StationRealDataSpecific(station_code=station_code, surge=surge,
                                                           tid=self.tid,
                                                           gmt_realtime=temp_realdata['dt'],
                                                           ts=temp_realdata['ts'])
                    self.session.add(obj_realdata)
            except Exception as ex:
                print(ex.args)

        # 23-03-01 以下暂时去掉
        # if len(self.session.scalars(query).fetchall()) > 0:
        #     for station_realdata in self.session.scalars(query):
        #         print(station_realdata)
        # else:
        #
        #     # 插入
        #     for temp_realdata in realdata_list:
        #         # ERROR:
        #         #     raise AttributeError(f"Use item[{name!r}] to get field value")
        #         # AttributeError: Use item['surge'] to get field value
        #         obj_realdata = StationRealDataSpecific(station_code=station_code, surge=temp_realdata['surge'],
        #                                                tid=self.tid,
        #                                                gmt_realtime=temp_realdata['dt'],
        #                                                ts=temp_realdata['ts'])
        #         self.session.add(obj_realdata)
        #         pass
        #
        #     pass

        self.session.commit()
        self.session.close()
        print(f'[-]写入{station_code}站点realdata完毕!')
        # TODO:[-] 23-03-02 写入完当前爬取的 station 潮位后更新 tb:station_status
        # TODO:[*] 23-03-02 此处修改为根据 realdata_list 找到最近的实况时间

        station_status = StationStatusData(station_code)
        station_status.insert(TaskTypeEnum.SUCCESS, self.tid, self.gmt_end.datetime)
        print(f'[-]更新{station_code}站点的status状态~')
        pass

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
            按照结束时间生成表名
            eg: gmt_start 2023-02-26 xxx_202302
        @return: 分表名称 eg: station_realdata_specific_202302
        """
        tab_base_name: str = DB_TABLE_SPLIT_OPTIONS.get('station').get('tab_split_name')
        tab_dt_name: str = self.gmt_end.format('YYYYMM')
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
              Column('ts', Integer, nullable=False),
              Column('gmt_realtime', DATETIME(fsp=6), default=datetime.utcnow),
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
        # with DbFactory().Session as session:
        task_id: int = -1
        try:
            self.session.add(task_info)
            self.session.commit()
        finally:
            task_id = task_info.id
            self.session.close()
        return task_id


class StationStatusData:
    def __init__(self, station_code: str):
        self.station_code = station_code
        self.session = DbFactory().Session

    def insert(self, status: TaskTypeEnum, tid: int, gmt_realtime: datetime):
        """
            插入 station_code 更新 gmt_update_dt | status
        :param status: 状态
        :param tid: tb: task_info id
        :param gmt_realtime: 最后的潮位实况时间
        :return:
        """
        # session = self.session
        now_utc: datetime = datetime.utcnow()
        # step1: 从 tb: station_status 中查找，若存在则update | 不存在则 create
        query = select(StationStatus).where(StationStatus.station_code == self.station_code)
        temp_first = self.session.scalars(query).fetchall()
        if len(temp_first) > 0:
            # update
            # AttributeError: 'CursorResult' object has no attribute 'execute_options'
            self.session.execute(
                update(StationStatus).where(StationStatus.station_code == self.station_code).values(
                    status=status.value,
                    gmt_realtime=gmt_realtime, gmt_modify_time=now_utc))
        else:
            obj_status = StationStatus(station_code=self.station_code, tid=tid, status=status.value,
                                       gmt_realtime=gmt_realtime)
            self.session.add(obj_status)
        self.session.commit()
        # self.session.close()
