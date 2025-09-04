from typing import List
import arrow
from arrow import Arrow
from datetime import datetime
from sqlalchemy import select, update, MetaData, Column, Integer, text, Float, Table, bindparam
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.automap import automap_base

import numpy as np

from common.enums import TaskTypeEnum
from conf.settings import TASK_OPTIONS, DB_TABLE_SPLIT_OPTIONS
from core.db import DbFactory
from models.models import StationRealDataSpecific, SpiderTaskInfo, StationStatus, StationRealDataIndex
from schemas.surge import GTSPointSchema, GTSEntiretySchema


class GTSSurgeRealData:
    """
        GTS Surge Data
        海洋站潮位实况
        实现分表功能
    """

    def __init__(self, gts_schema: GTSEntiretySchema, tid: int):
        self.station_code = gts_schema.station_code
        self.list_station_realdata: List[GTSPointSchema] = gts_schema.data_points
        self.tid = tid
        self.session = DbFactory().Session

    @property
    def gmt_start(self) -> Arrow:
        """
            起始时间
        """

        # sorted_list = self.list_station_realdata.sort(_sort)
        lambda_sort_key: str = 'timestamp_utc'
        sorted_list = sorted(self.list_station_realdata, key=lambda station: getattr(station, lambda_sort_key))
        start_ts: int = getattr(sorted_list[0], lambda_sort_key)
        return arrow.get(start_ts)

    @property
    def gmt_end(self) -> Arrow:
        lambda_sort_key: str = 'timestamp_utc'
        # 倒叙排列
        # 注意此处的 list_station_realdata 是 List[GTSPointSchema] <= shema
        # 获取 timestamp_utc 字段需要通过 getattr(cls,xx) 的方式获取该字段
        sorted_list = sorted(self.list_station_realdata, key=lambda station: getattr(station, lambda_sort_key),
                             reverse=True)
        end_ts: int = getattr(sorted_list[0], lambda_sort_key)
        return arrow.get(end_ts)

    def create_split_tab(self) -> bool:
        """
            创建分表
        """
        is_created = self._check_need_split_tab(to_create=True)
        return is_created
        pass

    def insert_realdata_list(self, to_coverage: bool = True,
                             realdata_list: List[GTSPointSchema] = []) -> bool:
        """
            判断 当前需要插入的数据是否已经存在于 表中，若存在且 to_coverage = True 则进行覆盖
        :param to_coverage:
        :param realdata_list:
        :return:
        """
        # TODO:[*] 23-03-12 注意此处需要根据插入的数据进行判断，是否需要跨表插入
        # 注意此处为 scrapy.Item 不能直接通过 obj.xx 需要通过 obj.get('xx') 获取
        months_list: List[int] = [temp.dt_utc.month for temp in realdata_list]

        def _filter_month(val_realdata, month: int) -> bool:
            return val_realdata.dt.month == month

        months_set = set(months_list)
        # eg {month:int,realdate:List}
        list_distmonth_realdata = []
        # {month:realdata_list}
        dict_distmonth_realdata: dict = {}
        # 若当前爬取的实况跨月，则需要按照不同的月份进行分表
        for temp_month in months_set:
            dict_distmonth_realdata[temp_month] = []
            for temp_realdata in realdata_list:
                if temp_realdata.dt_utc.month == temp_month:
                    dict_distmonth_realdata[temp_month].append(temp_realdata)

        for temo_month, temp_realdata_list in dict_distmonth_realdata.items():
            # temp_realdata_dict
            # 取出第一个
            # - 23-04-26 注意 temp_realdata_list[0].get('dt') 是 datetime.datetime ，需要转为 arrow
            now_arrow: Arrow = arrow.get(temp_realdata_list[0].timestamp_utc)
            tab_name: str = self._get_split_tab_name(now_arrow)
            station_code: str = self.station_code
            if self._check_exist_tab(tab_name):
                pass
            else:
                # 若不存在指定的表，需要创建该表
                self._create_station_realdata_tab(tab_name, now_arrow)
            # 动态更新表名
            self._insert_realdata(tab_name, station_code, realdata_list, to_coverage=to_coverage)

        pass

    def _insert_realdata(self, tab_name: str, station_code: str, realdata_list: List[GTSPointSchema],
                         to_coverage: bool = False):
        """
            若 to_coverage = True 则向表中覆盖已存在的数据
        :param realdata_list:
        :param to_coverage:
        :return:
        """

        if not realdata_list:
            print(f'[-] realdata_list for {station_code} is empty, skipping insertion.')
            return

        # 【修改】动态修改表名。注意：此操作在多线程环境下不安全，可能导致线程间互相影响。
        # 如果是单线程爬虫或任务，则此方法是可接受的。
        StationRealDataSpecific.__table__.name = tab_name
        # 根据 station_code | gmt_start < gmt_realitime < gmt_end 过滤结果
        # 对于要插入的集合进行遍历
        # -> S1: station_code | gmt_realtime | ts 存在 则 update
        # -> s2: station_code | gmt_realtime | ts 不存在 则 create

        print(f'[-] Processing {station_code} realdata, count: {len(realdata_list)}...')

        # 【新增】步骤1: 准备批量操作所需的数据
        # 过滤掉 surge 为 nan 的数据，并准备好待处理的数据字典列表
        valid_realdata = []
        """
            等待批量操作的实况数据集
            过滤掉 surge 为 nan 的数据，并准备好待处理的数据字典列表
        """

        for item in realdata_list:
            surge = item.sea_level_meters
            if not np.isnan(surge):
                valid_realdata.append({
                    "station_code": station_code,
                    "surge": surge,
                    "tid": self.tid,
                    "gmt_realtime": item.dt_utc,
                    "ts": item.timestamp_utc
                })

        if not valid_realdata:
            print(f'[-] All realdata for {station_code} had NaN surge, skipping.')
            return

        # 【新增】提取所有需要检查的时间戳
        all_dts = {item['gmt_realtime'] for item in valid_realdata}

        # 【新增】步骤2: 一次性查询出数据库中已存在的所有相关记录
        query = select(StationRealDataSpecific.gmt_realtime).where(
            StationRealDataSpecific.station_code == station_code,
            StationRealDataSpecific.gmt_realtime.in_(all_dts)
        )
        existing_dts = set(self.session.scalars(query).all())

        # 【新增】步骤3: 在内存中区分需要插入和需要更新的数据
        records_to_insert = []
        records_to_update = []

        for data_dict in valid_realdata:
            if data_dict['gmt_realtime'] in existing_dts:
                # 如果数据已存在，并且允许覆盖，则加入更新列表
                if to_coverage:
                    records_to_update.append(data_dict)
            else:
                # 如果数据不存在，则加入插入列表
                records_to_insert.append(data_dict)

        try:
            # 【新增】步骤4: 执行批量插入
            if records_to_insert:
                # 使用 add_all 效率不如 core insert，但如果需要返回 ORM 对象特性，则可使用
                # 这里我们直接使用字典，所以用 Core insert 更高效
                self.session.add_all([StationRealDataSpecific(**data) for data in records_to_insert])
                # SQLAlchemy 2.0 风格的批量插入
                # self.session.execute(StationRealDataSpecific.__table__.insert(), records_to_insert)
                print(f'[-] Bulk inserted {len(records_to_insert)} records for {station_code}.')

            # 【新增】步骤5: 执行批量更新
            if records_to_update:
                # SQLAlchemy 2.0 风格的批量更新
                # 注意：这种批量更新语法要求字典中包含主键或唯一约束的键值，以便定位要更新的行。
                # 在这个场景下，复合唯一键是 (station_code, gmt_realtime)。
                # 利用 SQLAlchemy 的 bindparam 功能来实现动态参数绑定，其核心目的是为了执行高效的批量更新（Batch Update）。

                # bindparam 在 SQLAlchemy 中代表一个“绑定的参数”，可以理解为一个占位符。
                # eg: UPDATE station_realdata_specific SET surge = ? WHERE station_code = ? AND gmt_realtime = ?
                stmt = update(StationRealDataSpecific).where(
                    StationRealDataSpecific.station_code == bindparam('_station_code'),
                    StationRealDataSpecific.gmt_realtime == bindparam('_gmt_realtime')
                ).values(surge=bindparam('surge'))

                # 准备符合 bindparam 的数据
                # update_params 是一个参数字典的列表。
                update_params = [
                    {'_station_code': d['station_code'], '_gmt_realtime': d['gmt_realtime'], 'surge': d['surge']}
                    for d in records_to_update
                ]

                self.session.execute(stmt, update_params)
                print(f'[-] Bulk updated {len(records_to_update)} records for {station_code}.')

            self.session.commit()
            print(f'[-] insert/update for {station_code} realdata is over!')

        except Exception as ex:
            self.session.rollback()  # 【新增】发生异常时回滚
            print(f'[!] Exception in _insert_realdata for {station_code}: {ex}')
            # 可以在这里重新抛出异常或进行更详细的日志记录
        finally:
            self.session.close()  # 【修改】确保 session 总能被关闭

            # 【修改】更新状态表的逻辑移到事务之外，因为它是一个独立的操作
            # 并且只在成功插入/更新数据后执行
        if valid_realdata:
            # 【优化】直接从处理过的数据中找到最新的时间，而不是再次遍历原始列表
            latest_realtime = max(item['gmt_realtime'] for item in valid_realdata)
            station_status = StationStatusData(station_code)
            # 假设 StationStatusData 内部会管理自己的 session
            station_status.insert(TaskTypeEnum.SUCCESS, self.tid, latest_realtime)
            print(f'[-] updated {station_code} status~')

    def _check_need_split_tab(self, dt: Arrow, to_create: bool = True) -> bool:
        """

            判断是否需要分表，若已存在表是否覆盖
        @param to_create: 是否覆盖
        @return:
        """
        tab_name: str = self._get_split_tab_name(dt)
        is_need = False
        if self._check_exist_tab(tab_name) is False or (self._check_exist_tab(tab_name) and to_create):
            is_need = self._create_station_realdata_tab(tab_name, dt)
        return is_need

    def _get_split_tab_name(self, dt: Arrow) -> str:
        """
            获取分表名称
            按照结束时间生成表名
            eg: gmt_start 2023-02-26 xxx_202302
        @param dt:当前时间
        @return: 分表名称 eg: station_realdata_specific_202302
        """
        tab_base_name: str = DB_TABLE_SPLIT_OPTIONS.get('station').get('tab_split_name')
        tab_dt_name: str = dt.format('YYYYMM')
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

    def _create_station_realdata_tab(self, tab_name: str, dt: arrow.Arrow) -> bool:
        """
            根据 tab_name 创建对应的潮位实况表
        @param tab_name:
        @return:
        """
        is_ok = False
        meta_data = MetaData()
        Table(tab_name, meta_data, Column('id', Integer, primary_key=True),
              Column('is_del', TINYINT(1), nullable=False, server_default=text("'0'"), default=0),
              Column('station_code', VARCHAR(200), nullable=False, index=True), Column('tid', Integer, nullable=False),
              Column('surge', Float, nullable=False),
              Column('ts', Integer, nullable=False),
              Column('gmt_realtime', DATETIME(fsp=6), default=datetime.utcnow, index=True),
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
        # TODO:[-] 23-04-28 还需要插入表 station_realdata_index
        year: int = dt.format('yyyy')
        local_start: arrow.Arrow = arrow.Arrow(year=year, month=dt.format('MM'), day=1, hour=0, minute=0)
        local_end: arrow.Arrow = local_start.shift(month=1)
        gmt_start: arrow.Arrow = local_start.shift(hour=-8)
        gmt_end: arrow.Arrow = local_end.shift(hour=-8)
        tab_index: StationRealDataIndex = StationRealDataIndex(tab_name=tab_name, year=year,
                                                               gmt_start=gmt_start.datetime,
                                                               gmt_end=gmt_end.datetime)
        session.add(tab_index)
        session.commit()
        return is_ok


class StationStatusData:
    def __init__(self, station_code: str):
        self.station_code = station_code
        # self.session = DbFactory().Session
        # 【修改】将 Session 的获取和管理放在方法内部，确保每个操作都是一个独立的事务单元。
        #  这使得该类更加健壮和可复用。
        self.db_factory = DbFactory()

    def insert(self, status: TaskTypeEnum, tid: int, gmt_realtime: datetime):
        """
            【推荐方案】使用 MySQL 的 ON DUPLICATE KEY UPDATE 实现原子性的 UPSERT。
             这是 SQLAlchemy 2.0 风格下处理此问题的最佳实践，高效且并发安全。
        """
        session = self.db_factory.Session
        now_utc: datetime = datetime.utcnow()

        try:
            # 步骤 1: 构建一个标准的 INSERT 语句，这是 SQLAlchemy 2.0 的方式
            # 我们使用 mysql_insert 来告诉 SQLAlchemy 我们要构建一个 MySQL 特定的语句
            insert_stmt = mysql_insert(StationStatus).values(
                station_code=self.station_code,
                tid=tid,
                status=status.value,
                gmt_realtime=gmt_realtime,
                gmt_create_time=now_utc,  # 仅在插入时生效
                gmt_modify_time=now_utc  # 插入和更新时都应更新
            )

            # 步骤 2: 在 INSERT 语句上添加 ON DUPLICATE KEY UPDATE 子句
            # 这是此方案的核心，它将两个操作合并成了一个原子操作
            upsert_stmt = insert_stmt.on_duplicate_key_update(
                # 如果发生键冲突（即 station_code 已存在），则更新以下字段
                status=insert_stmt.inserted.status,
                gmt_realtime=insert_stmt.inserted.gmt_realtime,
                gmt_modify_time=now_utc  # 直接设置为当前时间
            )
            # insert_stmt.inserted 是一个特殊对象，代表了你正尝试插入的值

            # 步骤 3: 执行这一个语句
            # SQLAlchemy 会将其编译为一条完整的、原子的 SQL 发送给 MySQL
            session.execute(upsert_stmt)
            session.commit()

        except Exception as ex:
            print(f'[!] StationStatusData insert/update error for {self.station_code}! msg: {ex}')
            session.rollback()
        finally:
            session.close()


class SpiderTask:
    def __init__(self, now_utc: Arrow, count_stations: int, task_name: str):
        self.now_utc = now_utc
        self.count_stations = count_stations
        self.task_name = task_name
        # self.session = DbFactory().Session
        self.db_factory = DbFactory()
        pass

    def to_db(self) -> int:
        session = self.db_factory.Session
        interval: int = TASK_OPTIONS.get('interval')
        task_info: SpiderTaskInfo = SpiderTaskInfo(timestamp=self.now_utc.timestamp(), task_name=self.task_name,
                                                   task_type=TaskTypeEnum.SUCCESS.value,
                                                   spider_count=self.count_stations, interval=interval)
        # with DbFactory().Session as session:
        task_id: int = -1
        try:
            session.add(task_info)
            session.commit()
        except Exception as ex:
            print(f'[!] SpiderTaskInfo insert/update error for msg: {ex}')
            session.rollback()
        finally:
            task_id = task_info.id
            session.close()
        return task_id
