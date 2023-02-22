from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy import ForeignKey, Sequence, MetaData
from sqlalchemy.orm import mapped_column
from datetime import datetime
from core.db import DbFactory

engine = DbFactory().engine

# 生成基类
BaseMeta = DeclarativeBase()
md = MetaData(bind=engine)  # 引用MetaData
metadata = BaseMeta.metadata


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


class StationRealDataIndex(IIdModel, IDel, IModel):
    """
        实况索引表
        分表索引使用 索引对象为 tb: StationRealDataSpecific
    """
    table_name: Mapped[str] = Column(VARCHAR(50), nullable=False, default=DEFAULT_TABLE_NAME)
    year = Column(Integer, nullable=False, default=DEFAULT_YEAR)
    gmt_start = Column(DATETIME(fsp=6), default=datetime.utcnow)
    gmt_end = Column(DATETIME(fsp=6), default=datetime.utcnow)
    __tablename__ = 'station_realdata_index'
