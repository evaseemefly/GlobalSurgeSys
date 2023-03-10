from models.models import StationStatus
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from dao.base import BaseDao


class StationStatusDao(BaseDao):
    def get_all_station_status(self, is_del=False):
        """
            + 23-03-09 获取全部站点的状态
        :param db:
        :param is_del:
        :return:
        """
        session: Session = self.db.session
        # filter_condition = select(StationStatus).where(StationStatus.is_del == is_del)

        # query = session.execute(filter_condition)
        # sqlalchemy.exc.ArgumentError: Executable SQL or text() construct expected, got <sqlalchemy.engine.result.ChunkedIteratorResult object at 0x0000021B539E2C08>.
        # return session.scalars(filter_condition)

        filter_query = session.query(StationStatus).filter(StationStatus.is_del == False).all()
        return filter_query
