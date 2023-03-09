from sqlalchemy.orm import Session
from models.models import StationStatus
from sqlalchemy import select, update
from dao.base import BaseDao


class StationStatusDao(BaseDao):
    def get_all_station_status(self, is_del=False):
        """
            + 23-03-09 获取全部站点的状态
        :param db:
        :param is_del:
        :return:
        """
        session = self.session
        filter_condition = select(StationStatus).where(StationStatus.is_del == is_del)
        query = session.execute(filter_condition)
        return session.scalar(query).fetchall()
