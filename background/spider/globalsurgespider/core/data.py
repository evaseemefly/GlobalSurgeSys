from typing import List
from arrow import Arrow
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from models.models import StationRealDataSpecific
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

        sorted_list = self.list_station_realdata.sort(_sort)
        start_ts: Arrow = sorted_list[0]['timestamp']
        return Arrow(start_ts)

    @property
    def gmt_end(self) -> Arrow:
        def _sort(x, y):
            if x < y:
                return 1
            elif x > y:
                return -1
            else:
                return 0

        sorted_list = self.list_station_realdata.sort(_sort, reverse=True)
        end_ts: Arrow = sorted_list[0]['timestamp']
        return Arrow(end_ts)

    def create_split_tab(self):
        """
            创建分表
        """
        self._check_need_split_tab(to_create=True)
        pass

    def _check_need_split_tab(self, to_create: bool = True) -> bool:
        tab_name: str = self._get_split_tab_name()
        if self._check_exist_tab(tab_name) is False and to_create is True:
            self._create_station_realdata_tab(tab_name)

            pass

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
        is_ob = False
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
            except Exception as ex:
                print(ex.args)
        return tab_name
