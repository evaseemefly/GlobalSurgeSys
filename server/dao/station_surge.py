from typing import List, Optional
from datetime import datetime
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from models.models import StationRealDataSpecific, StationRealDataIndex, RegionInfo
from schema.station_surge import SurgeRealDataSchema

from dao.base import BaseDao
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

    def get_dist_station_recently_surge_list(self) -> List[SurgeRealDataSchema]:
        """
            获取所有不同站点的最近一次的实况
        :return:
        """
        session: Session = self.db.session
        # step1: 获取距离当前最近的一张表名
        table: Optional[StationRealDataIndex] = session.query(StationRealDataIndex).order_by(
            StationRealDataIndex.gmt_create_time.desc()).first()
        # step2: 动态修改 realdata tb
        StationRealDataSpecific.__table__.name = table.table_name
        # step3: 从最近的一张表中查询
        filter_condition = select(StationRealDataSpecific.station_code, StationRealDataSpecific.gmt_realtime,
                                  StationRealDataSpecific.surge, StationRealDataSpecific.tid).group_by(
            StationRealDataSpecific.station_code).order_by(StationRealDataSpecific.gmt_realtime.desc())

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
