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
    table_name = Column(VARCHAR(), nullable=False, default=DEFAULT_TABLE_NAME)
    year = Column(Integer(), nullable=False, default=DEFAULT_YEAR)
    gmt_start = Column(DATETIME(fsp=6), default=datetime.utcnow)
    gmt_end = Column(DATETIME(fsp=6), default=datetime.utcnow)


class StationRealDataSpecific(IIdModel, IDel, IModel):
    """
        海洋站实况表 , 按照 yymm 进行分表
    """
    # 海洋站代码
    station_code = Column(VARCHAR(), default=DEFAULT_CODE)
    # 当前时间
    gmt_dt = Column(DATETIME(fsp=6), default=datetime.utcnow)
    timestamp = Column(Integer(), nullable=False, default=0)
    surge = Column(Float(), nullable=False, default=DEFAULT_SURGE)


class StationInfo(IIdModel, IDel, IModel):
    station_name = Column(VARCHAR(), default=DEFAULT_NAME)
    station_code = Column(VARCHAR(), default=DEFAULT_CODE)
    lat = Column(Float(), nullable=True)
    lon = Column(Float(), nullable=True)
    desc = Column(VARCHAR(), nullable=True)
    is_abs = Column(TINYINT(1), nullable=False, server_default=text("'0'"), default=0)
    # 所属父级 id
    pid = Column(Integer, default=-1)
    is_in_common_use = Column(TINYINT(1), nullable=False, server_default=text("'0'"), default=0)
    sort = Column(Integer, default=-1)
    # 归属的行政区划id tb: RegionInfo
    rid = Column(Integer, default=-1)


class RegionInfo(IIdModel, IDel, IModel):
    """
        行政区划表
    """
    location = Column(VARCHAR(), nullable=True)
    city = Column(VARCHAR(), nullable=True)
    city_name_ch = Column(VARCHAR(), nullable=False)
    country = Column(Integer(), default=DEFAULT_COUNTRY_INDEX)


class StationStatus(IIdModel, IDel, IModel):
    station_code = Column(VARCHAR(), nullable=False)
    status = Column(Integer(), default=DEFAULT_COUNTRY_INDEX)

class CommonDict(IIdModel, IDel, IModel):
