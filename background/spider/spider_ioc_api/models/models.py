from typing import List
from typing import Optional
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy import ForeignKey, Sequence, MetaData
from sqlalchemy.orm import mapped_column, DeclarativeBase
from datetime import datetime
from core.db import DbFactory

from common.default import DEFAULT_FK, UNLESS_INDEX, NONE_ID, DEFAULT_CODE, DEFAULT_PATH_TYPE, DEFAULT_PRO, \
    UNLESS_RANGE, DEFAULT_TABLE_NAME, DEFAULT_YEAR, DEFAULT_SURGE, DEFAULT_NAME, DEFAULT_COUNTRY_INDEX

from common.enums import TaskTypeEnum


# 1.3 -> 2.0 版本
# https://docs.sqlalchemy.org/en/20/orm/quickstart.html

# engine = DbFactory().engine
# # 生成基类
# BaseMeta = DeclarativeBase()
# md = MetaData(bind=engine)  # 引用MetaData
# metadata = BaseMeta.metadata


class BaseMeta(DeclarativeBase):
    pass


class IIdModel(BaseMeta):
    __abstract__ = True
    id: Mapped[int] = mapped_column(primary_key=True)


class IDel(BaseMeta):
    """
        软删除 抽象父类
    """
    __abstract__ = True
    is_del: Mapped[int] = mapped_column(nullable=False, default=0)


class IModel(BaseMeta):
    """
        model 抽象父类，主要包含 创建及修改时间
    """
    __abstract__ = True
    gmt_create_time: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    gmt_modify_time: Mapped[datetime] = mapped_column(default=datetime.utcnow)


class IRealDataDt(BaseMeta):
    __abstract__ = True
    gmt_realtime: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    ts: Mapped[int] = mapped_column(default=0)


class StationRealDataIndex(IIdModel, IDel, IModel):
    """
        实况索引表
        分表索引使用 索引对象为 tb: StationRealDataSpecific
    """
    table_name: Mapped[str] = mapped_column(default=DEFAULT_TABLE_NAME)
    year: Mapped[int] = mapped_column(default=DEFAULT_YEAR)
    gmt_start: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    gmt_end: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    __tablename__ = 'station_realdata_index'


class StationRealDataSpecific(IIdModel, IDel, IModel, IRealDataDt):
    """
        海洋站实况表 , 按照 yymm 进行分表
    """
    # 海洋站代码
    station_code: Mapped[str] = mapped_column(String(50), default=DEFAULT_CODE)
    # 当前时间
    # gmt_dt: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    # gmt_realtime: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    # gmt_realtime: Mapped[int] = mapped_column(default=0)
    surge: Mapped[float] = mapped_column(default=DEFAULT_SURGE)
    # 所属的 SpiderTaskInfo id
    tid: Mapped[int] = mapped_column(default=0)
    sensor_type: Mapped[str] = mapped_column(String(32), default=DEFAULT_NAME)
    __tablename__ = 'station_realdata_specific'

    # (可选) 在模型层声明约束，虽然动态分表主要看 core/data.py 的实现，但这里保留是个好习惯
    __table_args__ = (
        UniqueConstraint('station_code', 'sensor_type', 'ts', name='uix_station_sensor_ts'),
    )


class StationInfo(IIdModel, IDel, IModel):
    station_name: Mapped[str] = mapped_column(default=DEFAULT_NAME)
    station_code: Mapped[str] = mapped_column(default=DEFAULT_CODE)
    lat: Mapped[float] = mapped_column(nullable=True)
    lon: Mapped[float] = mapped_column(nullable=True)
    desc: Mapped[str] = mapped_column()
    is_abs: Mapped[int] = mapped_column(nullable=False, default=0)
    # 所属父级 id
    pid: Mapped[int] = mapped_column(default=0)
    is_in_common_use: Mapped[int] = mapped_column(nullable=False, default=0)
    sort: Mapped[int] = mapped_column(nullable=False, default=0)
    # 归属的行政区划id tb: RegionInfo
    rid: Mapped[int] = mapped_column(default=0)
    __tablename__ = 'station_info'


class RegionInfo(IIdModel, IDel, IModel):
    """
        行政区划表
    """
    location: Mapped[str] = mapped_column()
    city: Mapped[str] = mapped_column()
    city_name_ch: Mapped[str] = mapped_column()
    country: Mapped[int] = mapped_column(nullable=False, default=DEFAULT_COUNTRY_INDEX)
    __tablename__ = 'region_info'


class StationStatus(IIdModel, IDel, IModel):
    station_code: Mapped[str] = mapped_column(default=DEFAULT_CODE)
    status: Mapped[int] = mapped_column(nullable=False, default=TaskTypeEnum.FAIL.value)
    gmt_realtime: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    # 所属的 SpiderTaskInfo id
    tid: Mapped[int] = mapped_column(nullable=False, default=0)
    __tablename__ = 'station_status'

    # station_code 的唯一性约束
    __table_args__ = (
        UniqueConstraint('station_code', name='uix_station_code'),
    )


class CommonDict(IIdModel, IDel, IModel):
    pid: Mapped[int] = mapped_column(nullable=False, default=-1)
    val: Mapped[str] = mapped_column()
    desc: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column()
    __tablename__ = 'common_dict'


class SpiderTaskInfo(IIdModel, IDel, IModel):
    """
        (1146, "Table 'wave_forecast_db.spider_task_info' doesn't exist")
    """

    __tablename__ = 'spider_task_info'
    timestamp: Mapped[int] = mapped_column(default=0)
    task_name: Mapped[str] = mapped_column()
    task_type: Mapped[int] = mapped_column(nullable=False, default=TaskTypeEnum.HANGUP.value)
    # 爬取站点总数
    spider_count: Mapped[int] = mapped_column(nullable=False, default=0)
    # 时间间隔
    interval: Mapped[int] = mapped_column(nullable=False, default=0)
