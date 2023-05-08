from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped
# from sqlalchemy import ForeignKey, Sequence, MetaData
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
# TODO:[-] 23-04-25 ImportError: cannot import name 'mapped_column' from 'sqlalchemy.orm'
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy_utc import UtcDateTime
from datetime import datetime
from arrow import Arrow
from db.db_factory import DBFactory

from common.default import DEFAULT_FK, UNLESS_INDEX, NONE_ID, DEFAULT_CODE, DEFAULT_PATH_TYPE, DEFAULT_PRO, \
    UNLESS_RANGE, DEFAULT_TABLE_NAME, DEFAULT_YEAR, DEFAULT_SURGE, DEFAULT_NAME, DEFAULT_COUNTRY_INDEX

from common.enums import TaskTypeEnum
from common.utils import get_split_tablename
# 配置文件
from config.tb_config import DB_TABLE_SPLIT_OPTIONS


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
    # gmt_create_time: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    # gmt_modify_time: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    gmt_create_time = Column(UtcDateTime)
    gmt_modify_time = Column(UtcDateTime)


class IRealDataDt(BaseMeta):
    __abstract__ = True
    # gmt_realtime: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    gmt_realtime = Column(UtcDateTime)
    ts: Mapped[int] = mapped_column(default=0)


class StationRealDataIndex(IIdModel, IDel, IModel):
    """
        实况索引表
        分表索引使用 索引对象为 tb: StationRealDataSpecific
    """
    table_name: Mapped[str] = mapped_column(default=DEFAULT_TABLE_NAME)
    year: Mapped[int] = mapped_column(default=DEFAULT_YEAR)
    # gmt_start: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    # gmt_end: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    gmt_start = Column(UtcDateTime)
    gmt_end = Column(UtcDateTime)
    __tablename__ = 'station_realdata_index'


class StationRealDataSpecific(IIdModel, IDel, IModel, IRealDataDt):
    """
        海洋站实况表 , 按照 yymm 进行分表
    """
    # 海洋站代码
    station_code: Mapped[str] = mapped_column(default=DEFAULT_CODE)
    # 当前时间
    # gmt_dt: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    # gmt_realtime: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    # gmt_realtime: Mapped[int] = mapped_column(default=0)
    # 此处设施 nullable 无效
    # surge: Mapped[Optional[float]] = mapped_column(nullable=True)
    surge: Mapped[Optional[float]] = mapped_column(default=DEFAULT_SURGE, nullable=True)
    # surge: Mapped[Optional[float]]
    # 所属的 SpiderTaskInfo id
    tid: Mapped[int] = mapped_column(default=0)
    __tablename__ = 'station_realdata_specific'

    @classmethod
    def get_split_tablename(dt: datetime) -> str:
        return get_split_tablename(dt)

    @classmethod
    def set_tablename(self, dt: datetime):
        """
            根据传入的 dt 动态设置当前表名
        @param dt:
        @return:
        """
        tab_name: str = self.get_split_tablename(dt)
        self.__table__.name = tab_name


class StationAstronomicTideRealDataModel(IIdModel, IDel, IModel, IRealDataDt):
    """
        天文潮
    """
    __tablename__ = 'station_astronomic_tide'
    station_code = Column(VARCHAR(200), nullable=False)
    # forecast_dt = Column(DATETIME(fsp=2))
    surge = Column(Float, nullable=False)
    # ts = Column(Integer, nullable=False, default=0)


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
    # location: Mapped[str] = mapped_column()
    val_en: Mapped[str] = mapped_column()
    val_ch: Mapped[str] = mapped_column()
    pid: Mapped[int] = mapped_column(nullable=False, default=DEFAULT_COUNTRY_INDEX)
    __tablename__ = 'region_info'


class StationStatus(IIdModel, IDel, IModel):
    # class StationStatus(BaseMeta):
    """
        TODO:[*] 23-03-10 此处会与 fastapi pydantic 中的 schema 发生冲突，尝试更换为1.4的写法
    """
    station_code: Mapped[str] = mapped_column(default=DEFAULT_CODE)
    status: Mapped[int] = mapped_column(nullable=False, default=TaskTypeEnum.FAIL.value)
    # gmt_realtime: Mapped[datetime] = Column(sa.DATETIME(timezone=False), nullable=False)
    gmt_realtime = Column(UtcDateTime)
    # 所属的 SpiderTaskInfo id
    tid: Mapped[int] = mapped_column(nullable=False, default=0)
    # -----
    # id = Column(Integer, primary_key=True)
    # station_code = Column(VARCHAR, nullable=False, default=0)
    # status = Column(Integer, nullable=False, default=0)
    # tid = Column(Integer, nullable=False, default=0)
    # is_del = Column(TINYINT(1), nullable=False, default=0)
    __tablename__ = 'station_status'

    # class Config:
    #     orm_mode = True

    def __repr__(self):
        return f'code:{self.station_code},tid:{self.tid}|{self.status}'


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
