from typing import List, Dict, Any
from arrow import Arrow
import arrow
from datetime import datetime
from sqlalchemy import create_engine, inspect, insert, UniqueConstraint
from sqlalchemy import select, update
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.automap import automap_base
import sqlalchemy
import numpy as np
import logging

from common.enums import TaskTypeEnum
from models.models import StationRealDataSpecific, SpiderTaskInfo, StationStatus, StationRealDataIndex, BaseMeta
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy import ForeignKey, Sequence, MetaData, Table
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from core.db import DbFactory
from conf.settings import TASK_OPTIONS, DB_TABLE_SPLIT_OPTIONS
# TODO: [-] 25-12-04 NEW 引入 loguru
from loguru import logger


class StationSurgeRealData:
    """
    海洋站潮位实况 - 实现分表功能 (重构版)
    """

    # TODO:[-] 25-12-02 NEW 新增类变量，用于缓存本次进程生命周期内已确认存在的表名
    # set 查找时间复杂度为 O(1)，极大减少每分钟任务对数据库元数据的重复查询
    _verified_tables = set()

    def __init__(self, tid: int = 0):
        """
            # TODO: [修改] 移除 station_code 参数，使其支持多站点批量处理
        :param tid:
        """
        self.tid = tid
        # TODO: [优化] 建议在外部管理 Session 生命周期，或者使用 Context Manager，这里为了兼容旧代码保持原样
        self.db_factory = DbFactory()
        self.session = self.db_factory.Session

    def insert_realdata_list(self, realdata_list: List[Dict[str, Any]], to_coverage: bool = False) -> bool:
        """
            批量写入所有站点的临近实况数据
            TODO:[-] 25-12-02 由于目前采用每分钟高频写入的操作，将判断分表是否存在的操作加入了缓存
            数据分组 -> 遍历月份。
            计算表名 -> 检查缓存。
            如果缓存中有 -> 直接插入。
            如果缓存中无 -> 检查数据库/建表 -> 加入缓存 -> 插入。

            优化说明：
                准确性更高：假设你的数据里有 11月30日 23:59 和 12月01日 00:01 的数据。
                Min/Max 法：Range 是 11月-12月。你可能需要循环遍历这个区间来 check 表。
                分组法（当前方案）：代码会自动识别出数据里只有 11月和 12月两个组，精确地只检查这两个表，逻辑更严谨且不需要处理跨年/跨月的时间计算逻辑。
                性能等价：遍历一次列表来计算 Min/Max 的开销，和遍历一次列表来 Group By 的开销是一样的（都是 O(N)）。
                解决核心痛点：通过 _verified_tables 类变量，我们将每分钟 1 次的数据库元数据查询（Inspector），减少到了每个月 1 次（只有当月份变更生成新表名时，才会穿透缓存去查库）。对于 1500-2000 条数据，这已经是极致的优化。
        """
        if not realdata_list:
            return False

        # 1. 按月份对数据进行分组
        # 结构: { 3: [data1, data2], 4: [data3] } (key为月份)
        dict_distmonth_realdata: Dict[int, List[Dict]] = {}

        for item in realdata_list:
            # TODO: [修正] 统一使用 arrow 对象获取月份，确保传入的数据项中有 'dt' (arrow对象)
            month = item.get('dt').month
            if month not in dict_distmonth_realdata:
                dict_distmonth_realdata[month] = []
            dict_distmonth_realdata[month].append(item)

        # 2. 遍历每个月份的数据，插入到对应的分表中
        for month, batch_data in dict_distmonth_realdata.items():
            if not batch_data:
                continue

            # 取第一条数据的时间来确定表名
            first_dt: arrow.Arrow = batch_data[0].get('dt')
            tab_name = self._get_split_tab_name(first_dt)

            # TODO:[-] 25-12-02 NEW 核心优化逻辑：先检查内存缓存，如果已确认存在，直接跳过 DB 检查
            if tab_name not in self.__class__._verified_tables:
                # 缓存未命中，说明是新月或者程序刚启动，需要查询数据库
                if not self._check_exist_tab(tab_name):
                    logger.info(f"[-] Table {tab_name} does not exist, creating...")
                    self._create_station_realdata_tab(tab_name, first_dt)

                # TODO:[-] 25-12-02 NEW 确认表存在后，加入缓存。下一次分钟级任务将直接命中缓存。
                self.__class__._verified_tables.add(tab_name)

            # 执行插入
            self._insert_realdata(tab_name, batch_data, to_coverage=to_coverage)

        # ==========================================
        # TODO:[-] 25-12-02 NEW 批量更新站点状态表
        # 在所有实况数据入库尝试后，统一更新这些站点的最新状态
        self._batch_update_station_status(realdata_list)
        # ==========================================
        return True

    def _insert_realdata(self, tab_name: str, realdata_list: List[Dict], to_coverage: bool = False):
        """
            向指定表插入数据 (使用 Core Table 对象，避免修改 ORM 模型全局状态)
            针对 插入以及是否覆盖已做重大修改!
            TODO: [修改] 移除 station_code 参数，改为传入 realdata_list (List[Dict])
        :param tab_name:
        :param realdata_list:
        :param to_coverage: 是否启用覆盖模式：覆盖模式 (Upsert) 默认 false
        :return:
        """
        # TODO: [修改] 动态获取 Table 对象，而不是修改 StationRealDataSpecific.__table__.name
        # 使用 metadata 反射或者构造一个新的 Table 对象
        # 这里为了安全，我们基于 BaseMeta.metadata 和表名动态构造 Table 对象
        try:
            # 尝试从 metadata 中获取，如果之前创建过
            if tab_name in BaseMeta.metadata.tables:
                table = BaseMeta.metadata.tables[tab_name]
            else:
                # 如果 metadata 里没有，从数据库反射加载
                table = Table(tab_name, BaseMeta.metadata, autoload_with=self.db_factory.engine)
        except NoSuchTableError:
            # 理论上前面已经 check_exist 并 create 了，这里是双重保险
            logger.error(f"[!] Table {tab_name} not found during insert.")
            return

        logger.info(f'[-] Inserting  realdata into {tab_name}, count: {len(realdata_list)}')

        try:
            for temp_realdata in realdata_list:
                dt_val = temp_realdata['dt'].datetime  # 转回 datetime 用于存库
                ts_val = temp_realdata['ts']
                surge_val = temp_realdata['surge']
                # TODO: [修改] 从数据字典中获取 station_code
                station_code = temp_realdata.get('station_code')
                sensor_type: str = temp_realdata.get('sensor_type')

                if surge_val is None or np.isnan(surge_val):
                    continue

                # TODO: [优化] 下面的逻辑是逐条查询后插入/更新，效率较低。
                # 建议生产环境使用 MySQL 的 INSERT ... ON DUPLICATE KEY UPDATE (需使用 sqlalchemy.dialects.mysql.insert)

                # 1. 查询是否存在
                # 使用 Core Select
                # stmt_select = select(table).where(
                #     table.c.station_code == station_code,
                #     table.c.gmt_realtime == dt_val
                # )
                # result = self.session.execute(stmt_select).fetchone()
                #
                # if result:
                #     # Update
                #     # TODO: [逻辑] 如果 to_coverage 为 False，是否应该跳过更新？你之前的代码似乎只要存在就更新(to_coverage参数未完全使用)
                #     stmt_update = update(table).where(
                #         table.c.station_code == station_code,
                #         table.c.gmt_realtime == dt_val
                #     ).values(surge=surge_val, gmt_modify_time=datetime.utcnow())
                #     self.session.execute(stmt_update)
                # else:
                #     # Insert
                #     stmt_insert = insert(table).values(
                #         station_code=station_code,
                #         surge=surge_val,
                #         tid=self.tid,
                #         gmt_realtime=dt_val,
                #         ts=ts_val,
                #         is_del=0,
                #         gmt_create_time=datetime.utcnow(),
                #         gmt_modify_time=datetime.utcnow()
                #     )
                #     self.session.execute(stmt_insert)

                if surge_val is None or np.isnan(surge_val):
                    continue

                # TODO: [优化] 使用 MySQL 的 INSERT ... ON DUPLICATE KEY UPDATE 替换原有的 Select + Insert/Update 逻辑
                # 这种方式将 "查询+写入" 合并为一次原子操作，大幅提升效率并解决并发冲突。

                # 1. 构建基础的 Insert 语句
                insert_stmt = mysql_insert(table).values(
                    station_code=station_code,
                    surge=surge_val,
                    tid=self.tid,
                    gmt_realtime=dt_val,
                    ts=ts_val,
                    is_del=0,
                    sensor_type=sensor_type,
                    gmt_create_time=datetime.utcnow(),
                    gmt_modify_time=datetime.utcnow()
                )

                if to_coverage:
                    # TODO: [逻辑修正] 覆盖模式
                    # 当 (station_code + sensor_type + ts) 冲突时，更新数值和修改时间
                    upsert_stmt = insert_stmt.on_duplicate_key_update(
                        surge=insert_stmt.inserted.surge,  # 更新潮位值
                        gmt_modify_time=datetime.utcnow()  # 更新修改时间
                    )
                    self.session.execute(upsert_stmt)
                else:
                    # TODO: [逻辑] 非覆盖模式 (Insert Ignore)
                    # 如果数据已存在则忽略，不报错也不更新。相当于 MySQL 的 INSERT IGNORE ...
                    insert_ignore_stmt = insert_stmt.prefix_with('IGNORE')
                    self.session.execute(insert_ignore_stmt)

            self.session.commit()
            logger.info(f'[-] Insert/Update {station_code} finished.')

        except Exception as ex:
            self.session.rollback()
            logger.error(f"[!] Error inserting data: {ex}")
            # print(ex.args)
        finally:
            # 建议在外部关闭 session，或者在这里关闭
            self.session.close()

    def _batch_update_station_status(self, realdata_list: List[Dict]):
        """
            TODO:[-] 25-12-03
            批量更新站点状态表 (StationStatus)
            逻辑：
            1. 从 realdata_list 中提取每个站点的最新时间。
            2. 使用 Upsert 批量写入/更新 StationStatus。
        """
        if not realdata_list:
            return

        # 1. 数据预处理：按 station_code 分组，取 dt (lasttime) 最大的那条数据
        # 这一步是为了防止一个批次里同一个站点有多条数据，导致无谓的多次更新，且确保写入的是最新时间
        station_latest_map: Dict[str, Dict] = {}

        for item in realdata_list:
            code = item.get('station_code')
            if not code:
                continue

            # 比较并保留该站点最新的数据
            if code not in station_latest_map:
                station_latest_map[code] = item
            else:
                current_dt = item.get('dt')
                existing_dt = station_latest_map[code].get('dt')
                # 确保都是 arrow 对象或 datetime 进行比较
                if current_dt > existing_dt:
                    station_latest_map[code] = item

        if not station_latest_map:
            return

        logger.info(f'[-] Updating StationStatus for {len(station_latest_map)} stations...')

        try:
            # 2. 构建批量 Upsert 语句
            # 注意：ON DUPLICATE KEY UPDATE 生效的前提是 station_code 在数据库中是 唯一索引 (UNIQUE)

            # 准备 values 列表
            values_to_insert = []
            now_time = datetime.utcnow()

            for code, item in station_latest_map.items():
                values_to_insert.append({
                    'station_code': code,
                    'status': TaskTypeEnum.SUCCESS.value,  # 1001
                    'gmt_realtime': item.get('dt').datetime,  # 更新为获取到的 lasttime
                    'tid': self.tid,
                    'is_del': 0,
                    'gmt_create_time': now_time,
                    'gmt_modify_time': now_time
                })

            # 使用 Core Table 构造 SQL (假设 StationStatus 表结构已通过 models 反射或导入)
            # 如果 StationStatus 没有被 import 到这里，可以使用 Table 反射
            status_table = StationStatus.__table__

            insert_stmt = mysql_insert(status_table).values(values_to_insert)

            # 3. 配置 Upsert 更新逻辑
            # 当 station_code 冲突时，更新 status, gmt_realtime, gmt_modify_time
            upsert_stmt = insert_stmt.on_duplicate_key_update(
                status=insert_stmt.inserted.status,
                gmt_realtime=insert_stmt.inserted.gmt_realtime,
                gmt_modify_time=datetime.utcnow(),
                tid=insert_stmt.inserted.tid
            )

            self.session.execute(upsert_stmt)
            self.session.commit()
            logger.info(f'[-] StationStatus batch update finished.')

        except Exception as ex:
            self.session.rollback()
            logger.error(f"[!] Error updating station status: {ex}")

    def _get_split_tab_name(self, dt: arrow.Arrow) -> str:
        """
        获取分表名称: station_realdata_specific_YYYYMM
        """
        tab_base_name = DB_TABLE_SPLIT_OPTIONS.get('station', {}).get('tab_split_name', 'station_realdata_specific')
        tab_dt_name = dt.format('YYYYMM')
        return f'{tab_base_name}_{tab_dt_name}'

    def _check_exist_tab(self, tab_name: str) -> bool:
        """
        TODO: [修改] 使用 inspect 替代 automap_base，极大提高性能
        """
        # 1. 创建检查器 (Inspector)
        insp = inspect(self.db_factory.engine)
        # 2. 询问数据库
        # has_table 方法会直接向数据库查询表是否存在 (类似于 SHOW TABLES LIKE 'tab_name')
        return insp.has_table(tab_name)

    def _create_station_realdata_tab(self, tab_name: str, dt: arrow.Arrow) -> bool:
        """
        创建分表
        """
        # TODO: [修改] 动态复制 StationRealDataSpecific 的列定义，避免硬编码
        columns = []
        for col in StationRealDataSpecific.__table__.columns:
            # 复制列属性，注意要重置 primary_key 以外的绑定信息
            columns.append(col.copy())

        # TODO: [修正] 在创建新表时，强制加入联合唯一索引
        # 只有有了这个索引，ON DUPLICATE KEY UPDATE 才会生效
        uix_constraint = UniqueConstraint('station_code', 'sensor_type', 'ts', name=f'uix_{tab_name}_unique')

        # 定义新表
        new_table = Table(tab_name, BaseMeta.metadata, *columns, uix_constraint, extend_existing=True)

        try:
            # 在数据库创建表
            new_table.create(self.db_factory.engine)

            # 插入索引表记录 (StationRealDataIndex)
            year = int(dt.format('YYYY'))
            # 注意：arrow shift 逻辑
            local_start = arrow.get(year=year, month=int(dt.format('M')), day=1)
            local_end = local_start.shift(months=1)
            # 假设你的 gmt 逻辑是 -8 小时
            gmt_start = local_start.shift(hours=-8)
            gmt_end = local_end.shift(hours=-8)

            tab_index = StationRealDataIndex(
                table_name=tab_name,
                year=year,
                gmt_start=gmt_start.datetime,
                gmt_end=gmt_end.datetime
            )
            # 这里的 session 需要一个新的，因为 insert_realdata 可能会关闭 session
            # 简单起见，重新获取
            session = self.db_factory.Session
            session.add(tab_index)
            session.commit()
            session.close()
            return True
        except Exception as ex:
            logger.error(f"[!] Create table error: {ex}")
            return False
