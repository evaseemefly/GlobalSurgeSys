from typing import List, Optional
from datetime import datetime
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from models.models import StationRealDataSpecific, StationRealDataIndex
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
