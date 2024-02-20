from typing import List, Optional, Any
from datetime import datetime
import arrow
from sqlalchemy import select, update, func, and_, text
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from common.default import DEFAULT_LATLNG, DEFAULT_SURGE
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
        list_surge: List[StationRealDataSpecific] = self.get_target_dt_surge(station_code, gmt_start, gmt_end, True,
                                                                             False, True)
        if self._check_need_split_tab(gmt_start, gmt_end):
            # TODO:[-] 24-02-19 此处注意由于传入的还是起止时间未变，只是修改了 is_use_starttime_split=False 参数
            list_surge_remain = self.get_target_dt_surge(station_code, gmt_start, gmt_end, is_use_starttime_split=False,
                                                         is_desc=False, is_hourly=True)
            list_surge.extend(list_surge_remain)
        return list_surge

    def get_station_surge_list_bysqlbindparams(self, station_code: str, gmt_start: datetime, gmt_end: datetime) -> List[
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
        list_surge: List[StationRealDataSpecific] = self.get_target_dt_surge_bysqlbindparams(station_code, gmt_start,
                                                                                             gmt_end, True,
                                                                                             False, True)
        if self._check_need_split_tab(gmt_start, gmt_end):
            # TODO:[-] 24-02-19 此处注意由于传入的还是起止时间未变，只是修改了 is_use_starttime_split=False 参数
            list_surge_remain = self.get_target_dt_surge_bysqlbindparams(station_code, gmt_start, gmt_end,
                                                                         is_use_starttime_split=False,
                                                                         is_desc=False, is_hourly=True)
            list_surge.extend(list_surge_remain)
        return list_surge

    def get_station_surge_list_byorm(self, station_code: str, gmt_start: datetime, gmt_end: datetime) -> List[
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
        list_surge: List[StationRealDataSpecific] = self.get_target_dt_surge_backup(station_code, gmt_start, gmt_end,
                                                                                    True,
                                                                                    False, True)
        if self._check_need_split_tab(gmt_start, gmt_end):
            # TODO:[-] 24-02-19 此处注意由于传入的还是起止时间未变，只是修改了 is_use_starttime_split=False 参数
            list_surge_remain = self.get_target_dt_surge_backup(station_code, gmt_start, gmt_end,
                                                                is_use_starttime_split=False,
                                                                is_desc=False, is_hourly=True)
            list_surge.extend(list_surge_remain)
        # TODO:[*] 23-04-03 此处加入拼接
        # step1: 根据起止时间生成时间集合
        # 时间间隔单位(单位: s)——以1h为时间间隔步长
        dt_step_unit: int = 60 * 60
        # TODO:[-] 23-04-25 TypeError: unsupported operand type(s) for -: 'method' and 'method'
        # TODO:[*] 23-04-25 注意此处存在问题 linux 与 win 获取时间戳时，linux为func
        dt_diff = int((arrow.get(gmt_end).timestamp() - arrow.get(gmt_start).timestamp()) / dt_step_unit)
        dt_index_list = [i for i in range(dt_diff)]
        # 根据传入的起止时间按照指定的时间间隔(dt_step_unit) 生成时间集合
        # 起始时间(arrow)
        arrow_start: arrow.Arrow = arrow.get(gmt_start)
        # 起始时间(整点时刻:arrow)
        arrow_start_hourly: arrow.Arrow = arrow.Arrow(arrow_start.year, arrow_start.month, arrow_start.day,
                                                      arrow_start.hour, 0)
        # 时间列表(整点)
        dt_utc_list: List[arrow.Arrow] = [arrow.get(arrow_start_hourly).shift(hours=i) for i in dt_index_list]
        result: List[StationRealDataSpecific] = []
        # ERROR: 注意 dt_list 是 utc 时间,而 list_surge 中的时间为 local
        for temp_dt_ar_utc in dt_utc_list:
            temp_dt_utc: datetime.datetime = temp_dt_ar_utc.datetime
            # TODO:[*] 23-04-25 注意此处存在问题 linux 与 win 获取时间戳时，linux为func
            filter_obj = list(filter(lambda x: x.ts == temp_dt_ar_utc.timestamp(), list_surge))
            if len(filter_obj) > 0:
                filter_obj[0].gmt_realtime = temp_dt_utc
                result.append(filter_obj[0])
            else:
                # TODO:[*] 23-04-25 注意此处存在问题 linux 与 win 获取时间戳时，linux为func
                temp_obj = StationRealDataSpecific(station_code=station_code, tid=-1, surge=DEFAULT_SURGE,
                                                   gmt_realtime=temp_dt_utc,
                                                   ts=temp_dt_ar_utc.timestamp(),
                                                   )
                result.append(temp_obj)

        return result

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

    def get_target_dt_surge_backup(self, station_code: str, start: datetime, end: datetime,
                                   is_use_starttime_split: bool = True, is_desc: bool = True, is_hourly: bool = True) -> \
            List[
                StationRealDataSpecific]:
        """
            获取指定 起止时间范围内的 对应 code 的潮位集合(整点)
        @param station_code:
        @param start:
        @param end:
        @param is_use_starttime_split:
        @param is_desc:
        @param is_hourly:
        @return:
        """
        # 对应分表的名称
        split_dt: datetime = start if is_use_starttime_split else end
        tb_name: str = get_split_tablename(split_dt)
        # 判断指定表是否存在
        session: Session = self.db.session
        filter_query = session.query(StationRealDataIndex).filter(StationRealDataIndex.table_name == tb_name,
                                                                  StationRealDataIndex.is_del == False).all()

        if len(filter_query) > 0:
            # TODO:[*] 24-02-19 此处尝试改为 style2.0 的写法
            # 此处发现的bug是若使用1.4stle通过手动切换表名的方式实际并为切换表名？，改为style2.0的方式解决
            # TODO:[*] 24-02-20 此处修改为在model中通过类方法为table name 赋值
            StationRealDataSpecific.set_tablename(split_dt)
            # StationRealDataSpecific.__table__.name = tb_name
            stmt = select(StationRealDataSpecific).where(StationRealDataSpecific.gmt_realtime >= start,
                                                         StationRealDataSpecific.gmt_realtime <= end,
                                                         StationRealDataSpecific.station_code == station_code)
            # surge_filter_query = session.query(StationRealDataSpecific).filter(
            #     StationRealDataSpecific.gmt_realtime >= start, StationRealDataSpecific.gmt_realtime <= end,
            #     StationRealDataSpecific.station_code == station_code)
            # TODO:[-] 23-03-31 取整点的数据
            if is_hourly:
                stmt = stmt.filter(func.MINUTE(StationRealDataSpecific.gmt_realtime) == 0)
                # surge_filter_query = surge_filter_query.filter(
                #     func.MINUTE(StationRealDataSpecific.gmt_realtime) == 0)
            if is_desc:
                # surge_filter_query = surge_filter_query.order_by(StationRealDataSpecific.gmt_realtime.desc())
                stmt = stmt.order_by(StationRealDataSpecific.gmt_realtime.desc())
            elif is_desc is False:
                # surge_filter_query = surge_filter_query.order_by(StationRealDataSpecific.gmt_realtime.asc())
                stmt = stmt.order_by(StationRealDataSpecific.gmt_realtime.asc())
            # res = surge_filter_query.all()
            # print(stmt)
            # TODO:[*] 24-02-20
            # 使用 execute.fetchall() 返回的结果均为 Row,但不会出现两次动态变更表名而结果不变的问题，使用 fetchall 返回的结果为元祖
            # 使用 execute.scalars() 返回的结果集是对象
            res = session.execute(stmt).scalars().all()
            # TODO:[=] 24-02-19 此处手动commit会导致 StationRealDataSpecific deleted
            # session.commit()
            return res
        return []

    # def get_dist_month_tabs(self,start:datetime,end:datetime)->List[str]:

    def get_target_dt_surge(self, station_code: str, start: datetime, end: datetime,
                            is_use_starttime_split: bool = True, is_desc: bool = True, is_hourly: bool = True) -> \
            List[
                StationRealDataSpecific]:
        """
            获取指定 起止时间范围内的 对应 code 的潮位集合(整点)
            + 24-02-20: 此处修改为不使用 sqlalchemy style2.0 实现动态切换表名查询 ——
                        改为原生sql实现
        @param station_code:
        @param start:
        @param end:
        @param is_use_starttime_split:
        @param is_desc:
        @param is_hourly:
        @return:
        """
        # 对应分表的名称
        split_dt: datetime = start if is_use_starttime_split else end
        tb_name: str = get_split_tablename(split_dt)
        # 判断指定表是否存在
        session: Session = self.db.session
        filter_query = session.query(StationRealDataIndex).filter(StationRealDataIndex.table_name == tb_name,
                                                                  StationRealDataIndex.is_del == False).all()

        if len(filter_query) > 0:
            # TODO:[-] 24-02-19 此处尝试改为 2.0版本的写法
            # 此处发现的bug是若使用1.4stle通过手动切换表名的方式实际并为切换表名？，改为style2.0的方式解决
            order_desc = 'desc'
            if is_desc:
                pass
            elif is_desc is False:
                order_desc = 'asc'
            sql_str = text(f"""
                SELECT *
                FROM {tb_name}
                WHERE {tb_name}.gmt_realtime >= '{start}'
                  AND {tb_name}.gmt_realtime <= '{end}'
                  AND {tb_name}.station_code = '{station_code}'
                  AND MINUTE({tb_name}.gmt_realtime) = 0
                ORDER BY {tb_name}.gmt_realtime {order_desc}
            """)
            res = session.execute(sql_str)
            res = res.fetchall()
            return res
        return []

    def get_target_dt_surge_bysqlbindparams(self, station_code: str, start: datetime, end: datetime,
                                            is_use_starttime_split: bool = True, is_desc: bool = True,
                                            is_hourly: bool = True) -> \
            List[
                StationRealDataSpecific]:
        """
            获取指定 起止时间范围内的 对应 code 的潮位集合(整点)
            + 24-02-20: 此处修改为不使用 sqlalchemy style2.0 实现动态切换表名查询 ——
                        改为原生sql实现
        @param station_code:
        @param start:
        @param end:
        @param is_use_starttime_split:
        @param is_desc:
        @param is_hourly:
        @return:
        """
        # 对应分表的名称
        split_dt: datetime = start if is_use_starttime_split else end
        tb_name: str = get_split_tablename(split_dt)
        # 判断指定表是否存在
        session: Session = self.db.session
        filter_query = session.query(StationRealDataIndex).filter(StationRealDataIndex.table_name == tb_name,
                                                                  StationRealDataIndex.is_del == False).all()

        if len(filter_query) > 0:
            # TODO:[-] 24-02-19 此处尝试改为 2.0版本的写法
            # 此处发现的bug是若使用1.4stle通过手动切换表名的方式实际并为切换表名？，改为style2.0的方式解决
            order_desc = 'desc'
            if is_desc:
                pass
            elif is_desc is False:
                order_desc = 'asc'

            sql_str = text(f"""
                SELECT *
                FROM {tb_name}
                WHERE {tb_name}.gmt_realtime >= :start
                  AND {tb_name}.gmt_realtime <= :end
                  AND {tb_name}.station_code = :station_code
                  AND MINUTE({tb_name}.gmt_realtime) = 0
                ORDER BY {tb_name}.gmt_realtime {order_desc}
            """)
            # start:'2024-01-31 03:32:56+00:00'
            # end:'2024-02-03 03:32:56+00:00'
            sql = sql_str.bindparams(start=arrow.get(start).format('YYYY-MM-DDTHH:mm:ss.SSS') + 'Z',
                                     end=arrow.get(end).format('YYYY-MM-DDTHH:mm:ss.SSS') + 'Z',
                                     station_code=station_code)
            res = session.query(StationRealDataSpecific).from_statement(sql)
            res = res.all()
            return res
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
