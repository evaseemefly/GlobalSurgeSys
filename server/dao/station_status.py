from typing import List, Optional, Any

from models.models import StationStatus, RegionInfo, StationInfo
from schema.station_status import StationStatusAndGeoInfoSchema
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

    def get_one_station_status(self, station_code: str, is_del=False) -> Optional[StationStatus]:
        """
            + 23-03-11 根据 station_code 获取对应的站点状态,若不存在则返回None
        :param station_code:
        :param is_del:
        :return:
        """
        session: Session = self.db.session
        filter_query = session.query(StationStatus).filter(StationStatus.is_del == is_del,
                                                           StationStatus.station_code == station_code).first()
        if filter_query is not None:
            return filter_query
        return None

    def get_all_station_status_join_latlng(self):
        session: Session = self.db.session
        # sqlalchemy.exc.ArgumentError: Join target, typically a FROM expression,
        # or ORM relationship attribute expected
        # filter_condition = select(StationStatus, StationInfo).join_from(
        #     StationStatus, StationInfo).where(StationStatus.station_code == StationInfo.station_code,
        #                                       StationStatus.is_del == False).order_by(StationInfo.id)
        # SELECT xxx
        # FROM station_status INNER JOIN station_info ON station_status.station_code = station_info.station_code
        # WHERE station_status.is_del = false ORDER BY station_info.id
        #

        # filter_condition = select(StationStatus, StationInfo).join(
        # TODO:[-] 23-03-13 此处使用 join 关联两张表注意 join ( 第二张表,关联条件)
        filter_condition = select(StationStatus.status, StationStatus.gmt_realtime, StationStatus.gmt_modify_time,
                                  StationInfo.station_code, StationInfo.rid, StationInfo.lat,
                                  StationInfo.lon, StationStatus.is_del).join(
            StationInfo, StationStatus.station_code == StationInfo.station_code).where(
            StationStatus.is_del == False).order_by(StationInfo.id)
        query = session.execute(filter_condition).fetchall()
        list_schema = []
        for temp_query in query:
            temp_schema: dict[str, Any] = {'status': temp_query[0],
                                           'gmt_realtime': temp_query[1],
                                           'gmt_modify_time': temp_query[2],
                                           'station_code': temp_query[3],
                                           'rid': temp_query[4],
                                           'lat': temp_query[5],
                                           'lon': temp_query[6],
                                           'is_del': temp_query[7]}
            list_schema.append(temp_schema)
        return list_schema
