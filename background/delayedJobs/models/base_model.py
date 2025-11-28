from datetime import datetime

from arrow import Arrow
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy.ext.declarative import declarative_base

BaseMeta = declarative_base()


class IIdIntModel(BaseMeta):
    __abstract__ = True
    # id: Mapped[int] = mapped_column(primary_key=True, default=NONE_ID)
    # TODO:[-] 23-05-22 对于int类型主键，不需要设置默认值
    # autoincrement 自增字段
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


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


class IIssueTime(BaseMeta):
    __abstract__ = True
    issue_dt: Mapped[datetime] = mapped_column(default=datetime.utcnow().date())
    issue_ts: Mapped[int] = mapped_column(default=Arrow.utcnow().int_timestamp)


class IReleaseTime(BaseMeta):
    __abstract__ = True
    release_time: Mapped[datetime] = mapped_column(default=datetime.utcnow().date())
    release_ts: Mapped[int] = mapped_column(default=Arrow.utcnow().int_timestamp)


class IForecastTime(BaseMeta):
    __abstract__ = True
    forecast_time: Mapped[datetime] = mapped_column(default=datetime.utcnow().date())
    forecast_ts: Mapped[int] = mapped_column(default=Arrow.utcnow().int_timestamp)
