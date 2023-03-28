from typing import List, Optional
from datetime import datetime
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from common.default import DEFAULT_LATLNG
from models.models import StationRealDataSpecific, StationRealDataIndex, RegionInfo, StationInfo
from schema.region import RegionSchema
from schema.station_surge import SurgeRealDataSchema

from dao.base import BaseDao
from dao.region import RegionDao
from dao.station import StationDao
from common.utils import get_split_tablename


class StationSurgeDao(BaseDao):
    def get_station_surge_list(self, station_code: str, gmt_start: datetime, gmt_end: datetime) -> List[
        SurgeRealDataSchema]:
        """
            根据传入的海洋站 code 以及 起止时间获取时间范围内的 surge data 集合
            起止时间不需要超过1个月
        @param station_code:
        @param gmt_start:
        @param gmt_end:
        @return:
        """
        # TODO:[*] 23-03-12 此处存在一个问题:当 起止时间不在一月(已在spider模块修改)
        # 先判断是否需要联合表进行查询
        list_surge: List[StationRealDataSpecific] = self.get_target_dt_surge(station_code, gmt_start, gmt_end)
        if self._check_need_split_tab(gmt_start, gmt_end):
            list_surge.extend(
                self.get_station_surge_list(station_code, gmt_start, gmt_end, is_use_starttime_split=False))
        return list_surge

    def _check_need_split_tab(self, start: datetime, end: datetime):
        """
            判断传入的起止时间是否跨月
        @param start:
        @param end:
        @return:
        """
        if start.month == end.month:
            return False
        return True

    def get_target_dt_surge(self, station_code: str, start: datetime, end: datetime,
                            is_use_starttime_split: bool = True) -> List[StationRealDataSpecific]:
        # 对应分表的名称
        split_dt: datetime = start if is_use_starttime_split else end
        tb_name: str = get_split_tablename(split_dt)
        # 判断指定表是否存在
        session: Session = self.db.session
        filter_query = session.query(StationRealDataIndex).filter(StationRealDataIndex.table_name == tb_name,
                                                                  StationRealDataIndex.is_del == False).all()
        if len(filter_query) > 0:
            StationRealDataSpecific.__table__.name = tb_name
            surge_filter_query = session.query(StationRealDataSpecific).filter(
                StationRealDataSpecific.gmt_realtime >= start, StationRealDataSpecific.gmt_realtime <= end,
                StationRealDataSpecific.station_code == station_code)
            return surge_filter_query.all()
        return []

    # def get_dist_month_tabs(self,start:datetime,end:datetime)->List[str]:

    def get_table_name_by_recently(self) -> str:
        """
            获取距离当前时间最近的表名
        :return:
        """
        session: Session = self.db.session
        table: Optional[StationRealDataIndex] = session.query(StationRealDataIndex).order_by(
            StationRealDataIndex.gmt_create_time.desc()).first()
        return table.table_name

    def get_table_name_by_dt(self, dt: datetime) -> Optional[str]:
        """
            获取当前时间对应的表名,若不存在则返回 None
        :param dt:
        :return:
        """
        session: Session = self.db.session
        table: Optional[StationRealDataIndex] = session.query(StationRealDataIndex).filter(
            StationRealDataIndex.gmt_start <= dt, StationRealDataIndex.gmt_end >= dt).first()
        if table is not None:
            return table.table_name
        return None

    def get_dist_station_surge_list_by_recently(self, is_join_station: bool = False) -> List[dict]:
        """
            获取所有不同站点的最近一次的实况
        :param is_join_station:  是否拼接 tb:station
        :return:
        """
        session: Session = self.db.session
        # step1: 获取距离当前最近的一张表名
        table_name: str = self.get_table_name_by_recently()
        # step2: 动态修改 realdata tb
        StationRealDataSpecific.__table__.name = table_name
        # step3: 从最近的一张表中查询
        filter_condition = select(StationRealDataSpecific.station_code, StationRealDataSpecific.gmt_realtime,
                                  StationRealDataSpecific.surge, StationRealDataSpecific.tid).group_by(
            StationRealDataSpecific.station_code).order_by(StationRealDataSpecific.gmt_realtime.desc())

        query = session.execute(filter_condition).fetchall()
        list_schema: List[dict] = []
        # 若要与region拼接
        if is_join_station:
            list_station: List[StationInfo] = StationDao().get_all_station()
            for temp_query in query:
                temp_station: Optional[StationInfo] = StationDao.get_station_by_code(list_station, temp_query[0])
                temp_schema: dict[str, Any] = {
                    'station_code': temp_query[0],
                    'gmt_realtime': temp_query[1],
                    'surge': temp_query[2],
                    'tid': temp_query[3],
                    'lat': temp_station.lat if temp_station is not None else DEFAULT_LATLNG,
                    'lon': temp_station.lon if temp_station is not None else DEFAULT_LATLNG,
                }
                list_schema.append(temp_schema)
        else:
            for temp_query in query:
                temp_schema: dict[str, Any] = {
                    'station_code': temp_query[0],
                    'gmt_realtime': temp_query[1],
                    'surge': temp_query[2],
                    'tid': temp_query[3],
                }
                list_schema.append(temp_schema)
        return list_schema

    def get_dist_station_surge_list_by_dt(self, dt: datetime) -> List[SurgeRealDataSchema]:
        """
            根据传入的 dt 获取所有站点该时刻的潮值
        :param dt:
        :return:
        """
        session: Session = self.db.session
        # step1: 获取最近的表名
        table_name: str = self.get_table_name_by_dt(dt)
        # step2: 动态调整表名
        StationRealDataSpecific.__table__.name = table_name
        # step3: 获取该时刻的全部站的潮位
        filter_condition = select(StationRealDataSpecific.station_code, StationRealDataSpecific.gmt_realtime,
                                  StationRealDataSpecific.surge, StationRealDataSpecific.tid).where(
            StationRealDataSpecific.gmt_realtime == dt).group_by(StationRealDataSpecific.station_code)
        # TODO:[-] 23-03-15 以下代码与 get_dist_station_surge_list_by_recently 方法中相同
        query = session.execute(filter_condition).fetchall()
        list_schema = []
        for temp_query in query:
            temp_schema: dict[str, Any] = {
                'station_code': temp_query[0],
                'gmt_realtime': temp_query[1],
                'surge': temp_query[2],
                'tid': temp_query[3]
            }
            list_schema.append(temp_schema)
        return list_schema
