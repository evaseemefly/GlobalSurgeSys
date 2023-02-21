from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, Sequence, MetaData, Table
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime

from core.db import DbFactory

from common.default import DEFAULT_FK, UNLESS_INDEX, NONE_ID, DEFAULT_CODE, DEFAULT_PATH_TYPE, DEFAULT_PRO, \
    UNLESS_RANGE, DEFAULT_TABLE_NAME, DEFAULT_YEAR, DEFAULT_SURGE, DEFAULT_NAME, DEFAULT_COUNTRY_INDEX

from common.enums import TaskTypeEnum

engine = DbFactory().engine

# 生成基类
BaseMeta = declarative_base()
md = MetaData(bind=engine)  # 引用MetaData
metadata = BaseMeta.metadata


class IIdModel(BaseMeta):
    __abstract__ = True
    id = Column(Integer, primary_key=True)


class IDel(BaseMeta):
    """
        软删除 抽象父类
    """
    __abstract__ = True
    is_del = Column(TINYINT(1), nullable=False, server_default=text("'0'"), default=0)


class IModel(BaseMeta):
    """
        model 抽象父类，主要包含 创建及修改时间
    """
    __abstract__ = True
    gmt_create_time = Column(DATETIME(fsp=6), default=datetime.utcnow)
    gmt_modify_time = Column(DATETIME(fsp=6), default=datetime.utcnow)


class StationRealDataIndex(IIdModel, IDel, IModel):
    """
        实况索引表
        分表索引使用 索引对象为 tb: StationRealDataSpecific
    """
    table_name = Column(VARCHAR(50), nullable=False, default=DEFAULT_TABLE_NAME)
    year = Column(Integer, nullable=False, default=DEFAULT_YEAR)
    gmt_start = Column(DATETIME(fsp=6), default=datetime.utcnow)
    gmt_end = Column(DATETIME(fsp=6), default=datetime.utcnow)
    __tablename__ = 'station_realdata_index'


class StationRealDataSpecific(IIdModel, IDel, IModel):
    """
        海洋站实况表 , 按照 yymm 进行分表
    """
    # 海洋站代码
    station_code = Column(VARCHAR(10), default=DEFAULT_CODE)
    # 当前时间
    gmt_dt = Column(DATETIME(fsp=6), default=datetime.utcnow)
    timestamp = Column(Integer, nullable=False, default=0)
    surge = Column(Float, nullable=False, default=DEFAULT_SURGE)
    # 所属的 SpiderTaskInfo id
    tid = Column(Integer, nullable=False, default=0)
    __tablename__ = 'station_realdata_specific'


class StationInfo(IIdModel, IDel, IModel):
    station_name = Column(VARCHAR(10), default=DEFAULT_NAME)
    station_code = Column(VARCHAR(10), default=DEFAULT_CODE)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    desc = Column(VARCHAR(500), nullable=True)
    is_abs = Column(TINYINT(1), nullable=False, server_default=text("'0'"), default=0)
    # 所属父级 id
    pid = Column(Integer, default=-1)
    is_in_common_use = Column(TINYINT(1), nullable=False, server_default=text("'0'"), default=0)
    sort = Column(INTEGER(4), default=-1)
    # 归属的行政区划id tb: RegionInfo
    rid = Column(Integer, default=-1)
    __tablename__ = 'station_info'


class RegionInfo(IIdModel, IDel, IModel):
    """
        行政区划表
    """
    location = Column(VARCHAR(50), nullable=True)
    city = Column(VARCHAR(20), nullable=True)
    city_name_ch = Column(VARCHAR(10), nullable=False)
    country = Column(INTEGER(4), default=DEFAULT_COUNTRY_INDEX)
    __tablename__ = 'region_info'


class StationStatus(IIdModel, IDel, IModel):
    station_code = Column(VARCHAR(10), nullable=False)
    status = Column(INTEGER(4), default=DEFAULT_COUNTRY_INDEX)
    # 所属的 SpiderTaskInfo id
    tid = Column(Integer, nullable=False, default=0)
    __tablename__ = 'station_status'


class CommonDict(IIdModel, IDel, IModel):
    pid = status = Column(Integer, default=-1)
    val = Column(VARCHAR(50), nullable=True)
    desc = Column(VARCHAR(100), nullable=True)
    name = Column(VARCHAR(10), nullable=True)
    __tablename__ = 'common_dict'


class SpiderTaskInfo(IIdModel, IDel, IModel):
    timestamp = Column(Integer, nullable=False, default=0)
    task_name = Column(VARCHAR(50), nullable=True)
    task_type = Column(INTEGER(4), default=TaskTypeEnum.HANGUP.value)
    # 爬取站点总数
    spider_count = Column(Integer, default=0)
    # 时间间隔
    interval = Column(Integer, default=0)
    __tablename__ = 'spider_task_info'


def to_migrate():
    """
        根据ORM生成数据库结构
    :return:
    """
    BaseMeta.metadata.create_all(engine)
