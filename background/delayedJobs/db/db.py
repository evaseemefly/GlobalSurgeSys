from contextlib import contextmanager

from sqlalchemy import create_engine, Engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session, Session
#
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from sqlalchemy import ForeignKey, Sequence, MetaData, Table
#
from datetime import datetime

from conf.db_config import DBConfig
from conf.settings import DATABASES


class DbFactory:
    """
        数据库工厂
        24-08-28 目前使用的 数据库工厂类
    """

    default_config: DBConfig = DBConfig()
    """默认配置项"""

    def __init__(self, db_mapping: str = 'default', engine_str: str = None, host: str = None, port: str = None,
                 db_name: str = None,
                 user: str = None,
                 pwd: str = None):
        """
            mysql 数据库 构造函数
        :param db_mapping:
        :param engine_str:
        :param host:
        :param port:
        :param db_name:
        :param user:
        :param pwd:
        """
        db_options = DATABASES.get(db_mapping)
        config = self.default_config
        '''当前加载的默认配置'''
        self.engine_str = engine_str if engine_str else db_options.get('ENGINE')
        self.host = host if host else db_options.get('HOST')
        self.port = port if port else db_options.get('POST')
        self.db_name = db_name if db_name else db_options.get('NAME')
        self.user = user if user else db_options.get('USER')
        self.password = pwd if pwd else db_options.get('PASSWORD')
        # TypeError: Invalid argument(s) 'encoding' sent to create_engine(), using configuration MySQLDialect_mysqldb/QueuePool/Engine.  Please check that the keyword arguments are appropriate for this combination of components.
        self.engine = create_engine(
            f"mysql+{self.engine_str}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}",
            pool_pre_ping=True, future=True, echo=False, pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_recycle=config.pool_recycle, )
        # TODO:[-] 23-03-03 通过 scoped_session 来提供现成安全的全局session
        # 参考: https://juejin.cn/post/6844904164141580302
        self._session_def = scoped_session(sessionmaker(bind=self.engine))
        """cls中的默认 session """

    @property
    def Session(self) -> scoped_session:
        """
            获取 cls._session_def -> session
        @return:
        """
        if self._session_def is None:
            self._session_def = scoped_session(sessionmaker(bind=self.engine))
        return self._session_def()


@contextmanager
def session_yield_scope():
    """
        [-] 24-08-26 基于事物的Session会话管理
    """

    # session = DBFactory().session
    session = DbFactory().Session
    """提供一个事务范围的会话"""
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_exist_tab(tab_name: str) -> bool:
    """
        判断指定表是否存在
    @param tab_name:
    @return:
    """
    is_exist = False
    auto_base = automap_base()
    db_factory = DbFactory()
    # session = db_factory.Session
    engine = db_factory.engine
    # engine = db_factory.get_engine()
    try:
        auto_base.prepare(engine, reflect=True)
        list_tabs = auto_base.classes
        if tab_name in list_tabs:
            is_exist = True
    except Exception as e:
        print(e.args)
    return is_exist
